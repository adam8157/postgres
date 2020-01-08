/*-------------------------------------------------------------------------
 *
 * nodeAgg.c
 *	  Routines to handle aggregate nodes.
 *
 *	  ExecAgg normally evaluates each aggregate in the following steps:
 *
 *		 transvalue = initcond
 *		 foreach input_tuple do
 *			transvalue = transfunc(transvalue, input_value(s))
 *		 result = finalfunc(transvalue, direct_argument(s))
 *
 *	  If a finalfunc is not supplied then the result is just the ending
 *	  value of transvalue.
 *
 *	  Other behaviors can be selected by the "aggsplit" mode, which exists
 *	  to support partial aggregation.  It is possible to:
 *	  * Skip running the finalfunc, so that the output is always the
 *	  final transvalue state.
 *	  * Substitute the combinefunc for the transfunc, so that transvalue
 *	  states (propagated up from a child partial-aggregation step) are merged
 *	  rather than processing raw input rows.  (The statements below about
 *	  the transfunc apply equally to the combinefunc, when it's selected.)
 *	  * Apply the serializefunc to the output values (this only makes sense
 *	  when skipping the finalfunc, since the serializefunc works on the
 *	  transvalue data type).
 *	  * Apply the deserializefunc to the input values (this only makes sense
 *	  when using the combinefunc, for similar reasons).
 *	  It is the planner's responsibility to connect up Agg nodes using these
 *	  alternate behaviors in a way that makes sense, with partial aggregation
 *	  results being fed to nodes that expect them.
 *
 *	  If a normal aggregate call specifies DISTINCT or ORDER BY, we sort the
 *	  input tuples and eliminate duplicates (if required) before performing
 *	  the above-depicted process.  (However, we don't do that for ordered-set
 *	  aggregates; their "ORDER BY" inputs are ordinary aggregate arguments
 *	  so far as this module is concerned.)	Note that partial aggregation
 *	  is not supported in these cases, since we couldn't ensure global
 *	  ordering or distinctness of the inputs.
 *
 *	  If transfunc is marked "strict" in pg_proc and initcond is NULL,
 *	  then the first non-NULL input_value is assigned directly to transvalue,
 *	  and transfunc isn't applied until the second non-NULL input_value.
 *	  The agg's first input type and transtype must be the same in this case!
 *
 *	  If transfunc is marked "strict" then NULL input_values are skipped,
 *	  keeping the previous transvalue.  If transfunc is not strict then it
 *	  is called for every input tuple and must deal with NULL initcond
 *	  or NULL input_values for itself.
 *
 *	  If finalfunc is marked "strict" then it is not called when the
 *	  ending transvalue is NULL, instead a NULL result is created
 *	  automatically (this is just the usual handling of strict functions,
 *	  of course).  A non-strict finalfunc can make its own choice of
 *	  what to return for a NULL ending transvalue.
 *
 *	  Ordered-set aggregates are treated specially in one other way: we
 *	  evaluate any "direct" arguments and pass them to the finalfunc along
 *	  with the transition value.
 *
 *	  A finalfunc can have additional arguments beyond the transvalue and
 *	  any "direct" arguments, corresponding to the input arguments of the
 *	  aggregate.  These are always just passed as NULL.  Such arguments may be
 *	  needed to allow resolution of a polymorphic aggregate's result type.
 *
 *	  We compute aggregate input expressions and run the transition functions
 *	  in a temporary econtext (aggstate->tmpcontext).  This is reset at least
 *	  once per input tuple, so when the transvalue datatype is
 *	  pass-by-reference, we have to be careful to copy it into a longer-lived
 *	  memory context, and free the prior value to avoid memory leakage.  We
 *	  store transvalues in another set of econtexts, aggstate->aggcontexts
 *	  (one per grouping set, see below), which are also used for the hashtable
 *	  structures in AGG_HASHED mode.  These econtexts are rescanned, not just
 *	  reset, at group boundaries so that aggregate transition functions can
 *	  register shutdown callbacks via AggRegisterCallback.
 *
 *	  The node's regular econtext (aggstate->ss.ps.ps_ExprContext) is used to
 *	  run finalize functions and compute the output tuple; this context can be
 *	  reset once per output tuple.
 *
 *	  The executor's AggState node is passed as the fmgr "context" value in
 *	  all transfunc and finalfunc calls.  It is not recommended that the
 *	  transition functions look at the AggState node directly, but they can
 *	  use AggCheckCallContext() to verify that they are being called by
 *	  nodeAgg.c (and not as ordinary SQL functions).  The main reason a
 *	  transition function might want to know this is so that it can avoid
 *	  palloc'ing a fixed-size pass-by-ref transition value on every call:
 *	  it can instead just scribble on and return its left input.  Ordinarily
 *	  it is completely forbidden for functions to modify pass-by-ref inputs,
 *	  but in the aggregate case we know the left input is either the initial
 *	  transition value or a previous function result, and in either case its
 *	  value need not be preserved.  See int8inc() for an example.  Notice that
 *	  the EEOP_AGG_PLAIN_TRANS step is coded to avoid a data copy step when
 *	  the previous transition value pointer is returned.  It is also possible
 *	  to avoid repeated data copying when the transition value is an expanded
 *	  object: to do that, the transition function must take care to return
 *	  an expanded object that is in a child context of the memory context
 *	  returned by AggCheckCallContext().  Also, some transition functions want
 *	  to store working state in addition to the nominal transition value; they
 *	  can use the memory context returned by AggCheckCallContext() to do that.
 *
 *	  Note: AggCheckCallContext() is available as of PostgreSQL 9.0.  The
 *	  AggState is available as context in earlier releases (back to 8.1),
 *	  but direct examination of the node is needed to use it before 9.0.
 *
 *	  As of 9.4, aggregate transition functions can also use AggGetAggref()
 *	  to get hold of the Aggref expression node for their aggregate call.
 *	  This is mainly intended for ordered-set aggregates, which are not
 *	  supported as window functions.  (A regular aggregate function would
 *	  need some fallback logic to use this, since there's no Aggref node
 *	  for a window function.)
 *
 *	  Grouping sets:
 *
 *	  A list of grouping sets which is structurally equivalent to a ROLLUP
 *	  clause (e.g. (a,b,c), (a,b), (a)) can be processed in a single pass over
 *	  ordered data.  We do this by keeping a separate set of transition values
 *	  for each grouping set being concurrently processed; for each input tuple
 *	  we update them all, and on group boundaries we reset those states
 *	  (starting at the front of the list) whose grouping values have changed
 *	  (the list of grouping sets is ordered from most specific to least
 *	  specific).
 *
 *	  Where more complex grouping sets are used, we break them down into
 *	  "phases", where each phase has a different sort order (except phase 0
 *	  which is reserved for hashing).  During each phase but the last, the
 *	  input tuples are additionally stored in a tuplesort which is keyed to the
 *	  next phase's sort order; during each phase but the first, the input
 *	  tuples are drawn from the previously sorted data.  (The sorting of the
 *	  data for the first phase is handled by the planner, as it might be
 *	  satisfied by underlying nodes.)
 *
 *	  Hashing can be mixed with sorted grouping.  To do this, we have an
 *	  AGG_MIXED strategy that populates the hashtables during the first sorted
 *	  phase, and switches to reading them out after completing all sort phases.
 *	  We can also support AGG_HASHED with multiple hash tables and no sorting
 *	  at all.
 *
 *	  From the perspective of aggregate transition and final functions, the
 *	  only issue regarding grouping sets is this: a single call site (flinfo)
 *	  of an aggregate function may be used for updating several different
 *	  transition values in turn. So the function must not cache in the flinfo
 *	  anything which logically belongs as part of the transition value (most
 *	  importantly, the memory context in which the transition value exists).
 *	  The support API functions (AggCheckCallContext, AggRegisterCallback) are
 *	  sensitive to the grouping set for which the aggregate function is
 *	  currently being called.
 *
 *	  Plan structure:
 *
 *	  What we get from the planner is actually one "real" Agg node which is
 *	  part of the plan tree proper, but which optionally has an additional list
 *	  of Agg nodes hung off the side via the "chain" field.  This is because an
 *	  Agg node happens to be a convenient representation of all the data we
 *	  need for grouping sets.
 *
 *	  For many purposes, we treat the "real" node as if it were just the first
 *	  node in the chain.  The chain must be ordered such that hashed entries
 *	  come before sorted/plain entries; the real node is marked AGG_MIXED if
 *	  there are both types present (in which case the real node describes one
 *	  of the hashed groupings, other AGG_HASHED nodes may optionally follow in
 *	  the chain, followed in turn by AGG_SORTED or (one) AGG_PLAIN node).  If
 *	  the real node is marked AGG_HASHED or AGG_SORTED, then all the chained
 *	  nodes must be of the same type; if it is AGG_PLAIN, there can be no
 *	  chained nodes.
 *
 *	  We collect all hashed nodes into a single "phase", numbered 0, and create
 *	  a sorted phase (numbered 1..n) for each AGG_SORTED or AGG_PLAIN node.
 *	  Phase 0 is allocated even if there are no hashes, but remains unused in
 *	  that case.
 *
 *	  AGG_HASHED nodes actually refer to only a single grouping set each,
 *	  because for each hashed grouping we need a separate grpColIdx and
 *	  numGroups estimate.  AGG_SORTED nodes represent a "rollup", a list of
 *	  grouping sets that share a sort order.  Each AGG_SORTED node other than
 *	  the first one has an associated Sort node which describes the sort order
 *	  to be used; the first sorted node takes its input from the outer subtree,
 *	  which the planner has already arranged to provide ordered data.
 *
 *	  Memory and ExprContext usage:
 *
 *	  Because we're accumulating aggregate values across input rows, we need to
 *	  use more memory contexts than just simple input/output tuple contexts.
 *	  In fact, for a rollup, we need a separate context for each grouping set
 *	  so that we can reset the inner (finer-grained) aggregates on their group
 *	  boundaries while continuing to accumulate values for outer
 *	  (coarser-grained) groupings.  On top of this, we might be simultaneously
 *	  populating hashtables; however, we only need one context for all the
 *	  hashtables.
 *
 *	  So we create an array, aggcontexts, with an ExprContext for each grouping
 *	  set in the largest rollup that we're going to process, and use the
 *	  per-tuple memory context of those ExprContexts to store the aggregate
 *	  transition values.  hashcontext is the single context created to support
 *	  all hash tables.
 *
 *	  When the hash table memory exceeds work_mem, we advance the transition
 *	  states only for groups already in the hash table. For tuples that would
 *	  need to create a new hash table entries (and initialize new transition
 *	  states), we spill them to disk to be processed later. The tuples are
 *	  spilled in a partitioned manner, so that subsequent batches are smaller
 *	  and less likely to exceed work_mem (if a batch does exceed work_mem, it
 *	  must be spilled recursively).
 *
 *	  Note that it's possible for transition states to start small but then
 *	  grow very large; for instance in the case of ARRAY_AGG. In such cases,
 *	  it's still possible to significantly exceed work_mem.
 *
 *    Transition / Combine function invocation:
 *
 *    For performance reasons transition functions, including combine
 *    functions, aren't invoked one-by-one from nodeAgg.c after computing
 *    arguments using the expression evaluation engine. Instead
 *    ExecBuildAggTrans() builds one large expression that does both argument
 *    evaluation and transition function invocation. That avoids performance
 *    issues due to repeated uses of expression evaluation, complications due
 *    to filter expressions having to be evaluated early, and allows to JIT
 *    the entire expression into one native function.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeAgg.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "storage/buffile.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/dynahash.h"
#include "utils/expandeddatum.h"
#include "utils/logtape.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"

/*
 * Control how many partitions are created when spilling HashAgg to
 * disk.
 *
 * HASH_PARTITION_FACTOR is multiplied by the estimated number of partitions
 * needed such that each partition will fit in memory. The factor is set
 * higher than one because there's not a high cost to having a few too many
 * partitions, and it makes it less likely that a partition will need to be
 * spilled recursively. Another benefit of having more, smaller partitions is
 * that small hash tables may perform better than large ones due to memory
 * caching effects.
 *
 * HASH_PARTITION_MEM is the approximate amount of work_mem we should reserve
 * for the partitions themselves (i.e. buffering of the files backing the
 * partitions). This is sloppy, because we must reserve the memory before
 * filling the hash table; but we choose the number of partitions at the time
 * we need to spill.
 *
 * We also specify a min and max number of partitions per spill. Too few might
 * mean a lot of wasted I/O from repeated spilling of the same tuples. Too
 * many will result in lots of memory wasted buffering the spill files (and
 * possibly pushing hidden costs to the OS for managing more files).
 */
#define HASH_PARTITION_FACTOR 1.50
#define HASH_MIN_PARTITIONS 4
#define HASH_MAX_PARTITIONS 256
#define HASH_PARTITION_MEM (HASH_MIN_PARTITIONS * BLCKSZ)

/*
 * Represents partitioned spill data for a single hashtable.
 */
typedef struct HashAggSpill
{
	int       n_partitions;		/* number of output partitions */
	int       partition_bits;	/* number of bits for partition mask
								   log2(n_partitions) parent partition bits */
	int      *partitions;		/* output logtape numbers */
	int64    *ntuples;			/* number of tuples in each partition */
	LogicalTapeSet *lts;            /* the logical tape set it spills in */
} HashAggSpill;

/*
 * Represents work to be done for one pass of hash aggregation. Initially,
 * only the input fields are set. If spilled to disk, also set the spill data.
 */
typedef struct HashAggBatch
{
	int      input_tape;		/* input partition */
	int      input_bits;		/* number of bits for input partition mask */
	int64    input_tuples;		/* number of tuples in this batch */
	int		 setno;				/* grouping set */
	HashAggSpill spill;			/* spill output */
	LogicalTapeSet *lts;            /* the logical tape set it spills in */
} HashAggBatch;

static void select_current_set(AggState *aggstate, int setno, bool is_hash);
static void initialize_phase(AggState *aggstate, int newphase);
static TupleTableSlot *fetch_input_tuple(AggState *aggstate);
static void initialize_aggregates(AggState *aggstate,
								  AggStatePerGroup *pergroups,
								  int numReset);
static void advance_transition_function(AggState *aggstate,
										AggStatePerTrans pertrans,
										AggStatePerGroup pergroupstate);
static void advance_aggregates(AggState *aggstate);
static void process_ordered_aggregate_single(AggState *aggstate,
											 AggStatePerTrans pertrans,
											 AggStatePerGroup pergroupstate);
static void process_ordered_aggregate_multi(AggState *aggstate,
											AggStatePerTrans pertrans,
											AggStatePerGroup pergroupstate);
static void finalize_aggregate(AggState *aggstate,
							   AggStatePerAgg peragg,
							   AggStatePerGroup pergroupstate,
							   Datum *resultVal, bool *resultIsNull);
static void finalize_partialaggregate(AggState *aggstate,
									  AggStatePerAgg peragg,
									  AggStatePerGroup pergroupstate,
									  Datum *resultVal, bool *resultIsNull);
static void prepare_projection_slot(AggState *aggstate,
									TupleTableSlot *slot,
									int currentSet);
static void finalize_aggregates(AggState *aggstate,
								AggStatePerAgg peragg,
								AggStatePerGroup pergroup);
static TupleTableSlot *project_aggregates(AggState *aggstate);
static Bitmapset *find_unaggregated_cols(AggState *aggstate);
static bool find_unaggregated_cols_walker(Node *node, Bitmapset **colnos);
static void build_hash_table(AggState *aggstate, int setno,
							 int64 ngroups_estimate);
static void prepare_hash_slot(AggState *aggstate);
static void hash_recompile_expressions(AggState *aggstate);
static uint32 calculate_hash(AggState *aggstate);
static long hash_choose_num_buckets(AggState *aggstate,
									long estimated_nbuckets,
									Size memory);
static int hash_choose_num_spill_partitions(uint64 input_groups,
											double hashentrysize);
static AggStatePerGroup lookup_hash_entry(AggState *aggstate, uint32 hash);
static void lookup_hash_entries(AggState *aggstate);
static TupleTableSlot *agg_retrieve_direct(AggState *aggstate);
static void agg_fill_hash_table(AggState *aggstate);
static bool agg_refill_hash_table(AggState *aggstate);
static TupleTableSlot *agg_retrieve_hash_table(AggState *aggstate);
static TupleTableSlot *agg_retrieve_hash_table_in_memory(AggState *aggstate);
static void hash_spill_init(HashAggSpill *spill, int input_bits,
							uint64 input_tuples, double hashentrysize);
static Size hash_spill_tuple(HashAggSpill *spill, int input_bits,
							 TupleTableSlot *slot, uint32 hash);
static MinimalTuple hash_read_spilled(LogicalTapeSet *lts, int tapenum, uint32 *hashp);
static HashAggBatch *hash_batch_new(LogicalTapeSet *lts, int tapenum, int setno, int64 input_tuples, int input_bits);
static void hash_finish_initial_spills(AggState *aggstate);
static void hash_spill_finish(AggState *aggstate, HashAggSpill *spill,
							  int setno, int input_bits);
static void hash_reset_spill(HashAggSpill *spill);
static void hash_reset_spills(AggState *aggstate);
static Datum GetAggInitVal(Datum textInitVal, Oid transtype);
static void build_pertrans_for_aggref(AggStatePerTrans pertrans,
									  AggState *aggstate, EState *estate,
									  Aggref *aggref, Oid aggtransfn, Oid aggtranstype,
									  Oid aggserialfn, Oid aggdeserialfn,
									  Datum initValue, bool initValueIsNull,
									  Oid *inputTypes, int numArguments);
static int	find_compatible_peragg(Aggref *newagg, AggState *aggstate,
								   int lastaggno, List **same_input_transnos);
static int	find_compatible_pertrans(AggState *aggstate, Aggref *newagg,
									 bool shareable,
									 Oid aggtransfn, Oid aggtranstype,
									 Oid aggserialfn, Oid aggdeserialfn,
									 Datum initValue, bool initValueIsNull,
									 List *transnos);


/*
 * Select the current grouping set; affects current_set and
 * curaggcontext.
 */
static void
select_current_set(AggState *aggstate, int setno, bool is_hash)
{
	/* when changing this, also adapt ExecInterpExpr() and friends */
	if (is_hash)
		aggstate->curaggcontext = aggstate->hashcontext;
	else
		aggstate->curaggcontext = aggstate->aggcontexts[setno];

	aggstate->current_set = setno;
}

/*
 * Switch to phase "newphase", which must either be 0 or 1 (to reset) or
 * current_phase + 1. Juggle the tuplesorts accordingly.
 *
 * Phase 0 is for hashing, which we currently handle last in the AGG_MIXED
 * case, so when entering phase 0, all we need to do is drop open sorts.
 */
static void
initialize_phase(AggState *aggstate, int newphase)
{
	Assert(newphase <= 1 || newphase == aggstate->current_phase + 1);

	/*
	 * Whatever the previous state, we're now done with whatever input
	 * tuplesort was in use.
	 */
	if (aggstate->sort_in)
	{
		tuplesort_end(aggstate->sort_in);
		aggstate->sort_in = NULL;
	}

	if (newphase <= 1)
	{
		/*
		 * Discard any existing output tuplesort.
		 */
		if (aggstate->sort_out)
		{
			tuplesort_end(aggstate->sort_out);
			aggstate->sort_out = NULL;
		}
	}
	else
	{
		/*
		 * The old output tuplesort becomes the new input one, and this is the
		 * right time to actually sort it.
		 */
		aggstate->sort_in = aggstate->sort_out;
		aggstate->sort_out = NULL;
		Assert(aggstate->sort_in);
		tuplesort_performsort(aggstate->sort_in);
	}

	/*
	 * If this isn't the last phase, we need to sort appropriately for the
	 * next phase in sequence.
	 */
	if (newphase > 0 && newphase < aggstate->numphases - 1)
	{
		Sort	   *sortnode = aggstate->phases[newphase + 1].sortnode;
		PlanState  *outerNode = outerPlanState(aggstate);
		TupleDesc	tupDesc = ExecGetResultType(outerNode);

		aggstate->sort_out = tuplesort_begin_heap(tupDesc,
												  sortnode->numCols,
												  sortnode->sortColIdx,
												  sortnode->sortOperators,
												  sortnode->collations,
												  sortnode->nullsFirst,
												  work_mem,
												  NULL, false);
	}

	aggstate->current_phase = newphase;
	aggstate->phase = &aggstate->phases[newphase];
}

/*
 * Fetch a tuple from either the outer plan (for phase 1) or from the sorter
 * populated by the previous phase.  Copy it to the sorter for the next phase
 * if any.
 *
 * Callers cannot rely on memory for tuple in returned slot remaining valid
 * past any subsequently fetched tuple.
 */
static TupleTableSlot *
fetch_input_tuple(AggState *aggstate)
{
	TupleTableSlot *slot;

	if (aggstate->sort_in)
	{
		/* make sure we check for interrupts in either path through here */
		CHECK_FOR_INTERRUPTS();
		if (!tuplesort_gettupleslot(aggstate->sort_in, true, false,
									aggstate->sort_slot, NULL))
			return NULL;
		slot = aggstate->sort_slot;
	}
	else
		slot = ExecProcNode(outerPlanState(aggstate));

	if (!TupIsNull(slot) && aggstate->sort_out)
		tuplesort_puttupleslot(aggstate->sort_out, slot);

	return slot;
}

/*
 * (Re)Initialize an individual aggregate.
 *
 * This function handles only one grouping set, already set in
 * aggstate->current_set.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
initialize_aggregate(AggState *aggstate, AggStatePerTrans pertrans,
					 AggStatePerGroup pergroupstate)
{
	/*
	 * Start a fresh sort operation for each DISTINCT/ORDER BY aggregate.
	 */
	if (pertrans->numSortCols > 0)
	{
		/*
		 * In case of rescan, maybe there could be an uncompleted sort
		 * operation?  Clean it up if so.
		 */
		if (pertrans->sortstates[aggstate->current_set])
			tuplesort_end(pertrans->sortstates[aggstate->current_set]);


		/*
		 * We use a plain Datum sorter when there's a single input column;
		 * otherwise sort the full tuple.  (See comments for
		 * process_ordered_aggregate_single.)
		 */
		if (pertrans->numInputs == 1)
		{
			Form_pg_attribute attr = TupleDescAttr(pertrans->sortdesc, 0);

			pertrans->sortstates[aggstate->current_set] =
				tuplesort_begin_datum(attr->atttypid,
									  pertrans->sortOperators[0],
									  pertrans->sortCollations[0],
									  pertrans->sortNullsFirst[0],
									  work_mem, NULL, false);
		}
		else
			pertrans->sortstates[aggstate->current_set] =
				tuplesort_begin_heap(pertrans->sortdesc,
									 pertrans->numSortCols,
									 pertrans->sortColIdx,
									 pertrans->sortOperators,
									 pertrans->sortCollations,
									 pertrans->sortNullsFirst,
									 work_mem, NULL, false);
	}

	/*
	 * (Re)set transValue to the initial value.
	 *
	 * Note that when the initial value is pass-by-ref, we must copy it (into
	 * the aggcontext) since we will pfree the transValue later.
	 */
	if (pertrans->initValueIsNull)
		pergroupstate->transValue = pertrans->initValue;
	else
	{
		MemoryContext oldContext;

		oldContext = MemoryContextSwitchTo(
										   aggstate->curaggcontext->ecxt_per_tuple_memory);
		pergroupstate->transValue = datumCopy(pertrans->initValue,
											  pertrans->transtypeByVal,
											  pertrans->transtypeLen);
		MemoryContextSwitchTo(oldContext);
	}
	pergroupstate->transValueIsNull = pertrans->initValueIsNull;

	/*
	 * If the initial value for the transition state doesn't exist in the
	 * pg_aggregate table then we will let the first non-NULL value returned
	 * from the outer procNode become the initial value. (This is useful for
	 * aggregates like max() and min().) The noTransValue flag signals that we
	 * still need to do this.
	 */
	pergroupstate->noTransValue = pertrans->initValueIsNull;
}

/*
 * Initialize all aggregate transition states for a new group of input values.
 *
 * If there are multiple grouping sets, we initialize only the first numReset
 * of them (the grouping sets are ordered so that the most specific one, which
 * is reset most often, is first). As a convenience, if numReset is 0, we
 * reinitialize all sets.
 *
 * NB: This cannot be used for hash aggregates, as for those the grouping set
 * number has to be specified from further up.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
initialize_aggregates(AggState *aggstate,
					  AggStatePerGroup *pergroups,
					  int numReset)
{
	int			transno;
	int			numGroupingSets = Max(aggstate->phase->numsets, 1);
	int			setno = 0;
	int			numTrans = aggstate->numtrans;
	AggStatePerTrans transstates = aggstate->pertrans;

	if (numReset == 0)
		numReset = numGroupingSets;

	for (setno = 0; setno < numReset; setno++)
	{
		AggStatePerGroup pergroup = pergroups[setno];

		select_current_set(aggstate, setno, false);

		for (transno = 0; transno < numTrans; transno++)
		{
			AggStatePerTrans pertrans = &transstates[transno];
			AggStatePerGroup pergroupstate = &pergroup[transno];

			initialize_aggregate(aggstate, pertrans, pergroupstate);
		}
	}
}

/*
 * Given new input value(s), advance the transition function of one aggregate
 * state within one grouping set only (already set in aggstate->current_set)
 *
 * The new values (and null flags) have been preloaded into argument positions
 * 1 and up in pertrans->transfn_fcinfo, so that we needn't copy them again to
 * pass to the transition function.  We also expect that the static fields of
 * the fcinfo are already initialized; that was done by ExecInitAgg().
 *
 * It doesn't matter which memory context this is called in.
 */
static void
advance_transition_function(AggState *aggstate,
							AggStatePerTrans pertrans,
							AggStatePerGroup pergroupstate)
{
	FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
	MemoryContext oldContext;
	Datum		newVal;

	if (pertrans->transfn.fn_strict)
	{
		/*
		 * For a strict transfn, nothing happens when there's a NULL input; we
		 * just keep the prior transValue.
		 */
		int			numTransInputs = pertrans->numTransInputs;
		int			i;

		for (i = 1; i <= numTransInputs; i++)
		{
			if (fcinfo->args[i].isnull)
				return;
		}
		if (pergroupstate->noTransValue)
		{
			/*
			 * transValue has not been initialized. This is the first non-NULL
			 * input value. We use it as the initial value for transValue. (We
			 * already checked that the agg's input type is binary-compatible
			 * with its transtype, so straight copy here is OK.)
			 *
			 * We must copy the datum into aggcontext if it is pass-by-ref. We
			 * do not need to pfree the old transValue, since it's NULL.
			 */
			oldContext = MemoryContextSwitchTo(
											   aggstate->curaggcontext->ecxt_per_tuple_memory);
			pergroupstate->transValue = datumCopy(fcinfo->args[1].value,
												  pertrans->transtypeByVal,
												  pertrans->transtypeLen);
			pergroupstate->transValueIsNull = false;
			pergroupstate->noTransValue = false;
			MemoryContextSwitchTo(oldContext);
			return;
		}
		if (pergroupstate->transValueIsNull)
		{
			/*
			 * Don't call a strict function with NULL inputs.  Note it is
			 * possible to get here despite the above tests, if the transfn is
			 * strict *and* returned a NULL on a prior cycle. If that happens
			 * we will propagate the NULL all the way to the end.
			 */
			return;
		}
	}

	/* We run the transition functions in per-input-tuple memory context */
	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	/* set up aggstate->curpertrans for AggGetAggref() */
	aggstate->curpertrans = pertrans;

	/*
	 * OK to call the transition function
	 */
	fcinfo->args[0].value = pergroupstate->transValue;
	fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
	fcinfo->isnull = false;		/* just in case transfn doesn't set it */

	newVal = FunctionCallInvoke(fcinfo);

	aggstate->curpertrans = NULL;

	/*
	 * If pass-by-ref datatype, must copy the new value into aggcontext and
	 * free the prior transValue.  But if transfn returned a pointer to its
	 * first input, we don't need to do anything.  Also, if transfn returned a
	 * pointer to a R/W expanded object that is already a child of the
	 * aggcontext, assume we can adopt that value without copying it.
	 */
	if (!pertrans->transtypeByVal &&
		DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue))
	{
		if (!fcinfo->isnull)
		{
			MemoryContextSwitchTo(aggstate->curaggcontext->ecxt_per_tuple_memory);
			if (DatumIsReadWriteExpandedObject(newVal,
											   false,
											   pertrans->transtypeLen) &&
				MemoryContextGetParent(DatumGetEOHP(newVal)->eoh_context) == CurrentMemoryContext)
				 /* do nothing */ ;
			else
				newVal = datumCopy(newVal,
								   pertrans->transtypeByVal,
								   pertrans->transtypeLen);
		}
		if (!pergroupstate->transValueIsNull)
		{
			if (DatumIsReadWriteExpandedObject(pergroupstate->transValue,
											   false,
											   pertrans->transtypeLen))
				DeleteExpandedObject(pergroupstate->transValue);
			else
				pfree(DatumGetPointer(pergroupstate->transValue));
		}
	}

	pergroupstate->transValue = newVal;
	pergroupstate->transValueIsNull = fcinfo->isnull;

	MemoryContextSwitchTo(oldContext);
}

/*
 * Advance each aggregate transition state for one input tuple.  The input
 * tuple has been stored in tmpcontext->ecxt_outertuple, so that it is
 * accessible to ExecEvalExpr.
 *
 * We have two sets of transition states to handle: one for sorted aggregation
 * and one for hashed; we do them both here, to avoid multiple evaluation of
 * the inputs.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
advance_aggregates(AggState *aggstate)
{
	bool		dummynull;

	ExecEvalExprSwitchContext(aggstate->phase->evaltrans,
							  aggstate->tmpcontext,
							  &dummynull);
}

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with only one input.  This is called after we have completed
 * entering all the input values into the sort object.  We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * Note that the strictness of the transition function was checked when
 * entering the values into the sort, so we don't check it again here;
 * we just apply standard SQL DISTINCT logic.
 *
 * The one-input case is handled separately from the multi-input case
 * for performance reasons: for single by-value inputs, such as the
 * common case of count(distinct id), the tuplesort_getdatum code path
 * is around 300% faster.  (The speedup for by-reference types is less
 * but still noticeable.)
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
process_ordered_aggregate_single(AggState *aggstate,
								 AggStatePerTrans pertrans,
								 AggStatePerGroup pergroupstate)
{
	Datum		oldVal = (Datum) 0;
	bool		oldIsNull = true;
	bool		haveOldVal = false;
	MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
	MemoryContext oldContext;
	bool		isDistinct = (pertrans->numDistinctCols > 0);
	Datum		newAbbrevVal = (Datum) 0;
	Datum		oldAbbrevVal = (Datum) 0;
	FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
	Datum	   *newVal;
	bool	   *isNull;

	Assert(pertrans->numDistinctCols < 2);

	tuplesort_performsort(pertrans->sortstates[aggstate->current_set]);

	/* Load the column into argument 1 (arg 0 will be transition value) */
	newVal = &fcinfo->args[1].value;
	isNull = &fcinfo->args[1].isnull;

	/*
	 * Note: if input type is pass-by-ref, the datums returned by the sort are
	 * freshly palloc'd in the per-query context, so we must be careful to
	 * pfree them when they are no longer needed.
	 */

	while (tuplesort_getdatum(pertrans->sortstates[aggstate->current_set],
							  true, newVal, isNull, &newAbbrevVal))
	{
		/*
		 * Clear and select the working context for evaluation of the equality
		 * function and transition function.
		 */
		MemoryContextReset(workcontext);
		oldContext = MemoryContextSwitchTo(workcontext);

		/*
		 * If DISTINCT mode, and not distinct from prior, skip it.
		 */
		if (isDistinct &&
			haveOldVal &&
			((oldIsNull && *isNull) ||
			 (!oldIsNull && !*isNull &&
			  oldAbbrevVal == newAbbrevVal &&
			  DatumGetBool(FunctionCall2Coll(&pertrans->equalfnOne,
											 pertrans->aggCollation,
											 oldVal, *newVal)))))
		{
			/* equal to prior, so forget this one */
			if (!pertrans->inputtypeByVal && !*isNull)
				pfree(DatumGetPointer(*newVal));
		}
		else
		{
			advance_transition_function(aggstate, pertrans, pergroupstate);
			/* forget the old value, if any */
			if (!oldIsNull && !pertrans->inputtypeByVal)
				pfree(DatumGetPointer(oldVal));
			/* and remember the new one for subsequent equality checks */
			oldVal = *newVal;
			oldAbbrevVal = newAbbrevVal;
			oldIsNull = *isNull;
			haveOldVal = true;
		}

		MemoryContextSwitchTo(oldContext);
	}

	if (!oldIsNull && !pertrans->inputtypeByVal)
		pfree(DatumGetPointer(oldVal));

	tuplesort_end(pertrans->sortstates[aggstate->current_set]);
	pertrans->sortstates[aggstate->current_set] = NULL;
}

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with more than one input.  This is called after we have completed
 * entering all the input values into the sort object.  We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
process_ordered_aggregate_multi(AggState *aggstate,
								AggStatePerTrans pertrans,
								AggStatePerGroup pergroupstate)
{
	ExprContext *tmpcontext = aggstate->tmpcontext;
	FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
	TupleTableSlot *slot1 = pertrans->sortslot;
	TupleTableSlot *slot2 = pertrans->uniqslot;
	int			numTransInputs = pertrans->numTransInputs;
	int			numDistinctCols = pertrans->numDistinctCols;
	Datum		newAbbrevVal = (Datum) 0;
	Datum		oldAbbrevVal = (Datum) 0;
	bool		haveOldValue = false;
	TupleTableSlot *save = aggstate->tmpcontext->ecxt_outertuple;
	int			i;

	tuplesort_performsort(pertrans->sortstates[aggstate->current_set]);

	ExecClearTuple(slot1);
	if (slot2)
		ExecClearTuple(slot2);

	while (tuplesort_gettupleslot(pertrans->sortstates[aggstate->current_set],
								  true, true, slot1, &newAbbrevVal))
	{
		CHECK_FOR_INTERRUPTS();

		tmpcontext->ecxt_outertuple = slot1;
		tmpcontext->ecxt_innertuple = slot2;

		if (numDistinctCols == 0 ||
			!haveOldValue ||
			newAbbrevVal != oldAbbrevVal ||
			!ExecQual(pertrans->equalfnMulti, tmpcontext))
		{
			/*
			 * Extract the first numTransInputs columns as datums to pass to
			 * the transfn.
			 */
			slot_getsomeattrs(slot1, numTransInputs);

			/* Load values into fcinfo */
			/* Start from 1, since the 0th arg will be the transition value */
			for (i = 0; i < numTransInputs; i++)
			{
				fcinfo->args[i + 1].value = slot1->tts_values[i];
				fcinfo->args[i + 1].isnull = slot1->tts_isnull[i];
			}

			advance_transition_function(aggstate, pertrans, pergroupstate);

			if (numDistinctCols > 0)
			{
				/* swap the slot pointers to retain the current tuple */
				TupleTableSlot *tmpslot = slot2;

				slot2 = slot1;
				slot1 = tmpslot;
				/* avoid ExecQual() calls by reusing abbreviated keys */
				oldAbbrevVal = newAbbrevVal;
				haveOldValue = true;
			}
		}

		/* Reset context each time */
		ResetExprContext(tmpcontext);

		ExecClearTuple(slot1);
	}

	if (slot2)
		ExecClearTuple(slot2);

	tuplesort_end(pertrans->sortstates[aggstate->current_set]);
	pertrans->sortstates[aggstate->current_set] = NULL;

	/* restore previous slot, potentially in use for grouping sets */
	tmpcontext->ecxt_outertuple = save;
}

/*
 * Compute the final value of one aggregate.
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * The finalfn will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 *
 * The finalfn uses the state as set in the transno. This also might be
 * being used by another aggregate function, so it's important that we do
 * nothing destructive here.
 */
static void
finalize_aggregate(AggState *aggstate,
				   AggStatePerAgg peragg,
				   AggStatePerGroup pergroupstate,
				   Datum *resultVal, bool *resultIsNull)
{
	LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
	bool		anynull = false;
	MemoryContext oldContext;
	int			i;
	ListCell   *lc;
	AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];

	oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	/*
	 * Evaluate any direct arguments.  We do this even if there's no finalfn
	 * (which is unlikely anyway), so that side-effects happen as expected.
	 * The direct arguments go into arg positions 1 and up, leaving position 0
	 * for the transition state value.
	 */
	i = 1;
	foreach(lc, peragg->aggdirectargs)
	{
		ExprState  *expr = (ExprState *) lfirst(lc);

		fcinfo->args[i].value = ExecEvalExpr(expr,
											 aggstate->ss.ps.ps_ExprContext,
											 &fcinfo->args[i].isnull);
		anynull |= fcinfo->args[i].isnull;
		i++;
	}

	/*
	 * Apply the agg's finalfn if one is provided, else return transValue.
	 */
	if (OidIsValid(peragg->finalfn_oid))
	{
		int			numFinalArgs = peragg->numFinalArgs;

		/* set up aggstate->curperagg for AggGetAggref() */
		aggstate->curperagg = peragg;

		InitFunctionCallInfoData(*fcinfo, &peragg->finalfn,
								 numFinalArgs,
								 pertrans->aggCollation,
								 (void *) aggstate, NULL);

		/* Fill in the transition state value */
		fcinfo->args[0].value =
			MakeExpandedObjectReadOnly(pergroupstate->transValue,
									   pergroupstate->transValueIsNull,
									   pertrans->transtypeLen);
		fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
		anynull |= pergroupstate->transValueIsNull;

		/* Fill any remaining argument positions with nulls */
		for (; i < numFinalArgs; i++)
		{
			fcinfo->args[i].value = (Datum) 0;
			fcinfo->args[i].isnull = true;
			anynull = true;
		}

		if (fcinfo->flinfo->fn_strict && anynull)
		{
			/* don't call a strict function with NULL inputs */
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			*resultVal = FunctionCallInvoke(fcinfo);
			*resultIsNull = fcinfo->isnull;
		}
		aggstate->curperagg = NULL;
	}
	else
	{
		/* Don't need MakeExpandedObjectReadOnly; datumCopy will copy it */
		*resultVal = pergroupstate->transValue;
		*resultIsNull = pergroupstate->transValueIsNull;
	}

	/*
	 * If result is pass-by-ref, make sure it is in the right context.
	 */
	if (!peragg->resulttypeByVal && !*resultIsNull &&
		!MemoryContextContains(CurrentMemoryContext,
							   DatumGetPointer(*resultVal)))
		*resultVal = datumCopy(*resultVal,
							   peragg->resulttypeByVal,
							   peragg->resulttypeLen);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Compute the output value of one partial aggregate.
 *
 * The serialization function will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 */
static void
finalize_partialaggregate(AggState *aggstate,
						  AggStatePerAgg peragg,
						  AggStatePerGroup pergroupstate,
						  Datum *resultVal, bool *resultIsNull)
{
	AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	/*
	 * serialfn_oid will be set if we must serialize the transvalue before
	 * returning it
	 */
	if (OidIsValid(pertrans->serialfn_oid))
	{
		/* Don't call a strict serialization function with NULL input. */
		if (pertrans->serialfn.fn_strict && pergroupstate->transValueIsNull)
		{
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			FunctionCallInfo fcinfo = pertrans->serialfn_fcinfo;

			fcinfo->args[0].value =
				MakeExpandedObjectReadOnly(pergroupstate->transValue,
										   pergroupstate->transValueIsNull,
										   pertrans->transtypeLen);
			fcinfo->args[0].isnull = pergroupstate->transValueIsNull;

			*resultVal = FunctionCallInvoke(fcinfo);
			*resultIsNull = fcinfo->isnull;
		}
	}
	else
	{
		/* Don't need MakeExpandedObjectReadOnly; datumCopy will copy it */
		*resultVal = pergroupstate->transValue;
		*resultIsNull = pergroupstate->transValueIsNull;
	}

	/* If result is pass-by-ref, make sure it is in the right context. */
	if (!peragg->resulttypeByVal && !*resultIsNull &&
		!MemoryContextContains(CurrentMemoryContext,
							   DatumGetPointer(*resultVal)))
		*resultVal = datumCopy(*resultVal,
							   peragg->resulttypeByVal,
							   peragg->resulttypeLen);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Prepare to finalize and project based on the specified representative tuple
 * slot and grouping set.
 *
 * In the specified tuple slot, force to null all attributes that should be
 * read as null in the context of the current grouping set.  Also stash the
 * current group bitmap where GroupingExpr can get at it.
 *
 * This relies on three conditions:
 *
 * 1) Nothing is ever going to try and extract the whole tuple from this slot,
 * only reference it in evaluations, which will only access individual
 * attributes.
 *
 * 2) No system columns are going to need to be nulled. (If a system column is
 * referenced in a group clause, it is actually projected in the outer plan
 * tlist.)
 *
 * 3) Within a given phase, we never need to recover the value of an attribute
 * once it has been set to null.
 *
 * Poking into the slot this way is a bit ugly, but the consensus is that the
 * alternative was worse.
 */
static void
prepare_projection_slot(AggState *aggstate, TupleTableSlot *slot, int currentSet)
{
	if (aggstate->phase->grouped_cols)
	{
		Bitmapset  *grouped_cols = aggstate->phase->grouped_cols[currentSet];

		aggstate->grouped_cols = grouped_cols;

		if (TTS_EMPTY(slot))
		{
			/*
			 * Force all values to be NULL if working on an empty input tuple
			 * (i.e. an empty grouping set for which no input rows were
			 * supplied).
			 */
			ExecStoreAllNullTuple(slot);
		}
		else if (aggstate->all_grouped_cols)
		{
			ListCell   *lc;

			/* all_grouped_cols is arranged in desc order */
			slot_getsomeattrs(slot, linitial_int(aggstate->all_grouped_cols));

			foreach(lc, aggstate->all_grouped_cols)
			{
				int			attnum = lfirst_int(lc);

				if (!bms_is_member(attnum, grouped_cols))
					slot->tts_isnull[attnum - 1] = true;
			}
		}
	}
}

/*
 * Compute the final value of all aggregates for one group.
 *
 * This function handles only one grouping set at a time, which the caller must
 * have selected.  It's also the caller's responsibility to adjust the supplied
 * pergroup parameter to point to the current set's transvalues.
 *
 * Results are stored in the output econtext aggvalues/aggnulls.
 */
static void
finalize_aggregates(AggState *aggstate,
					AggStatePerAgg peraggs,
					AggStatePerGroup pergroup)
{
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
	Datum	   *aggvalues = econtext->ecxt_aggvalues;
	bool	   *aggnulls = econtext->ecxt_aggnulls;
	int			aggno;
	int			transno;

	/*
	 * If there were any DISTINCT and/or ORDER BY aggregates, sort their
	 * inputs and run the transition functions.
	 */
	for (transno = 0; transno < aggstate->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];
		AggStatePerGroup pergroupstate;

		pergroupstate = &pergroup[transno];

		if (pertrans->numSortCols > 0)
		{
			Assert(aggstate->aggstrategy != AGG_HASHED &&
				   aggstate->aggstrategy != AGG_MIXED);

			if (pertrans->numInputs == 1)
				process_ordered_aggregate_single(aggstate,
												 pertrans,
												 pergroupstate);
			else
				process_ordered_aggregate_multi(aggstate,
												pertrans,
												pergroupstate);
		}
	}

	/*
	 * Run the final functions.
	 */
	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peragg = &peraggs[aggno];
		int			transno = peragg->transno;
		AggStatePerGroup pergroupstate;

		pergroupstate = &pergroup[transno];

		if (DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit))
			finalize_partialaggregate(aggstate, peragg, pergroupstate,
									  &aggvalues[aggno], &aggnulls[aggno]);
		else
			finalize_aggregate(aggstate, peragg, pergroupstate,
							   &aggvalues[aggno], &aggnulls[aggno]);
	}
}

/*
 * Project the result of a group (whose aggs have already been calculated by
 * finalize_aggregates). Returns the result slot, or NULL if no row is
 * projected (suppressed by qual).
 */
static TupleTableSlot *
project_aggregates(AggState *aggstate)
{
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;

	/*
	 * Check the qual (HAVING clause); if the group does not match, ignore it.
	 */
	if (ExecQual(aggstate->ss.ps.qual, econtext))
	{
		/*
		 * Form and return projection tuple using the aggregate results and
		 * the representative input tuple.
		 */
		return ExecProject(aggstate->ss.ps.ps_ProjInfo);
	}
	else
		InstrCountFiltered1(aggstate, 1);

	return NULL;
}

static bool
find_aggregated_cols_walker(Node *node, Bitmapset **colnos)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		*colnos = bms_add_member(*colnos, var->varattno);

		return false;
	}
	return expression_tree_walker(node, find_aggregated_cols_walker,
								  (void *) colnos);
}

/*
 * find_aggregated_cols
 *	  Construct a bitmapset of the column numbers of aggregated Vars
 *	  appearing in our targetlist and qual (HAVING clause)
 */
static Bitmapset *
find_aggregated_cols(AggState *aggstate)
{
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	Bitmapset  *colnos = NULL;
	ListCell   *temp;

	/*
	 * We only want the columns used by aggregations in the targetlist or qual
	 */
	if (node->plan.targetlist != NULL)
	{
		foreach(temp, (List *) node->plan.targetlist)
		{
			if (IsA(lfirst(temp), TargetEntry))
			{
				Node *node = (Node *)((TargetEntry *)lfirst(temp))->expr;
				if (IsA(node, Aggref) || IsA(node, GroupingFunc))
					find_aggregated_cols_walker(node, &colnos);
			}
		}
	}

	if (node->plan.qual != NULL)
	{
		foreach(temp, (List *) node->plan.qual)
		{
			if (IsA(lfirst(temp), TargetEntry))
			{
				Node *node = (Node *)((TargetEntry *)lfirst(temp))->expr;
				if (IsA(node, Aggref) || IsA(node, GroupingFunc))
					find_aggregated_cols_walker(node, &colnos);
			}
		}
	}

	return colnos;
}

/*
 * find_unaggregated_cols
 *	  Construct a bitmapset of the column numbers of un-aggregated Vars
 *	  appearing in our targetlist and qual (HAVING clause)
 */
static Bitmapset *
find_unaggregated_cols(AggState *aggstate)
{
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	Bitmapset  *colnos;

	colnos = NULL;
	(void) find_unaggregated_cols_walker((Node *) node->plan.targetlist,
										 &colnos);
	(void) find_unaggregated_cols_walker((Node *) node->plan.qual,
										 &colnos);
	return colnos;
}

static bool
find_unaggregated_cols_walker(Node *node, Bitmapset **colnos)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		/* setrefs.c should have set the varno to OUTER_VAR */
		Assert(var->varno == OUTER_VAR);
		Assert(var->varlevelsup == 0);
		*colnos = bms_add_member(*colnos, var->varattno);
		return false;
	}
	if (IsA(node, Aggref) ||IsA(node, GroupingFunc))
	{
		/* do not descend into aggregate exprs */
		return false;
	}
	return expression_tree_walker(node, find_unaggregated_cols_walker,
								  (void *) colnos);
}

/*
 * (Re-)initialize the hash table(s) to empty.
 *
 * To implement hashed aggregation, we need a hashtable that stores a
 * representative tuple and an array of AggStatePerGroup structs for each
 * distinct set of GROUP BY column values.  We compute the hash key from the
 * GROUP BY columns.  The per-group data is allocated in lookup_hash_entry(),
 * for each entry.
 *
 * We have a separate hashtable and associated perhash data structure for each
 * grouping set for which we're doing hashing. If setno is -1, build hash
 * tables for all grouping sets. Otherwise, build only for the specified
 * grouping set.
 *
 * The contents of the hash tables always live in the hashcontext's per-tuple
 * memory context (there is only one of these for all tables together, since
 * they are all reset at the same time).
 */
static void
build_hash_table(AggState *aggstate, int setno, long ngroups_estimate)
{
	Agg			   *agg = (Agg *)aggstate->ss.ps.plan;
	MemoryContext	tmpmem = aggstate->tmpcontext->ecxt_per_tuple_memory;
	Size            additionalsize;
	int				i;

	Assert(aggstate->aggstrategy == AGG_HASHED || aggstate->aggstrategy == AGG_MIXED);

	additionalsize = aggstate->numtrans * sizeof(AggStatePerGroupData) +
		agg->transSpace;

	for (i = 0; i < aggstate->num_hashes; ++i)
	{
		AggStatePerHash perhash = &aggstate->perhash[i];
		int64			ngroups;
		long			nbuckets;
		Size			memory;

		Assert(perhash->aggnode->numGroups > 0);

		if (perhash->hashtable)
			DestroyTupleHashTable(perhash->hashtable);
		perhash->hashtable = NULL;

		/*
		 * If we are building a hash table for only a single grouping set,
		 * skip the others.
		 */
		if (setno >= 0 && setno != i)
			continue;

		/*
		 * Use an estimate from execution time if we have it; otherwise fall
		 * back to the planner estimate.
		 */
		ngroups = ngroups_estimate > 0 ?
			ngroups_estimate : perhash->aggnode->numGroups;

		/* divide memory by the number of hash tables we are initializing */
		memory = (long)work_mem * 1024L /
			(setno >= 0 ? 1 : aggstate->num_hashes);

		/* choose reasonable number of buckets per hashtable */
		nbuckets = hash_choose_num_buckets(aggstate, ngroups, memory);

		perhash->hashtable = BuildTupleHashTableExt(&aggstate->ss.ps,
													perhash->hashslot->tts_tupleDescriptor,
													perhash->numCols,
													perhash->hashGrpColIdxHash,
													perhash->eqfuncoids,
													perhash->hashfunctions,
													perhash->aggnode->grpCollations,
													nbuckets,
													additionalsize,
													aggstate->ss.ps.state->es_query_cxt,
													aggstate->hashcontext->ecxt_per_tuple_memory,
													tmpmem,
													DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit));
	}

	aggstate->hash_mem_current = MemoryContextMemAllocated(
		aggstate->hashcontext->ecxt_per_tuple_memory, true);
	aggstate->hash_ngroups_current = 0;
	aggstate->hash_no_new_groups = false;
}

/*
 * Compute columns that actually need to be stored in hashtable entries.  The
 * incoming tuples from the child plan node will contain grouping columns,
 * other columns referenced in our targetlist and qual, columns used to
 * compute the aggregate functions, and perhaps just junk columns we don't use
 * at all.  Only columns of the first two types need to be stored in the
 * hashtable, and getting rid of the others can make the table entries
 * significantly smaller.  The hashtable only contains the relevant columns,
 * and is packed/unpacked in lookup_hash_entry() / agg_retrieve_hash_table()
 * into the format of the normal input descriptor.
 *
 * Additional columns, in addition to the columns grouped by, come from two
 * sources: Firstly functionally dependent columns that we don't need to group
 * by themselves, and secondly ctids for row-marks.
 *
 * To eliminate duplicates, we build a bitmapset of the needed columns, and
 * then build an array of the columns included in the hashtable. We might
 * still have duplicates if the passed-in grpColIdx has them, which can happen
 * in edge cases from semijoins/distinct; these can't always be removed,
 * because it's not certain that the duplicate cols will be using the same
 * hash function.
 *
 * Note that the array is preserved over ExecReScanAgg, so we allocate it in
 * the per-query context (unlike the hash table itself).
 */
static void
find_hash_columns(AggState *aggstate)
{
	Bitmapset  *base_colnos;
	Bitmapset  *aggregated_colnos;
	List	   *outerTlist = outerPlanState(aggstate)->plan->targetlist;
	int			numHashes = aggstate->num_hashes;
	EState	   *estate = aggstate->ss.ps.state;
	int			j;

	/* Find Vars that will be needed in tlist and qual */
	base_colnos = find_unaggregated_cols(aggstate);
	aggregated_colnos = find_aggregated_cols(aggstate);

	for (j = 0; j < numHashes; ++j)
	{
		AggStatePerHash perhash = &aggstate->perhash[j];
		Bitmapset  *colnos = bms_copy(base_colnos);
		Bitmapset  *allNeededColsInput;
		AttrNumber *grpColIdx = perhash->aggnode->grpColIdx;
		List	   *hashTlist = NIL;
		TupleDesc	hashDesc;
		int			maxCols;
		int			i;

		perhash->largestGrpColIdx = 0;

		/*
		 * If we're doing grouping sets, then some Vars might be referenced in
		 * tlist/qual for the benefit of other grouping sets, but not needed
		 * when hashing; i.e. prepare_projection_slot will null them out, so
		 * there'd be no point storing them.  Use prepare_projection_slot's
		 * logic to determine which.
		 */
		if (aggstate->phases[0].grouped_cols)
		{
			Bitmapset  *grouped_cols = aggstate->phases[0].grouped_cols[j];
			ListCell   *lc;

			foreach(lc, aggstate->all_grouped_cols)
			{
				int			attnum = lfirst_int(lc);

				if (!bms_is_member(attnum, grouped_cols))
					colnos = bms_del_member(colnos, attnum);
			}
		}

		/*
		 * Compute maximum number of input columns accounting for possible
		 * duplications in the grpColIdx array, which can happen in some edge
		 * cases where HashAggregate was generated as part of a semijoin or a
		 * DISTINCT.
		 */
		maxCols = bms_num_members(colnos) + perhash->numCols;

		perhash->hashGrpColIdxInput =
			palloc(maxCols * sizeof(AttrNumber));
		perhash->hashGrpColIdxHash =
			palloc(perhash->numCols * sizeof(AttrNumber));

		/* Add all the grouping columns to colnos */
		for (i = 0; i < perhash->numCols; i++)
			colnos = bms_add_member(colnos, grpColIdx[i]);

		/*
		 * Track the necessary columns from the input. This is important for
		 * spilling tuples so that we don't waste disk space with unneeded
		 * columns.
		 */
		allNeededColsInput = bms_union(colnos, aggregated_colnos);
		perhash->numNeededColsInput = 0;
		perhash->allNeededColsInput = palloc(
			bms_num_members(allNeededColsInput) * sizeof(AttrNumber));

		while ((i = bms_first_member(allNeededColsInput)) >= 0)
			perhash->allNeededColsInput[perhash->numNeededColsInput++] = i;

		/*
		 * First build mapping for columns directly hashed. These are the
		 * first, because they'll be accessed when computing hash values and
		 * comparing tuples for exact matches. We also build simple mapping
		 * for execGrouping, so it knows where to find the to-be-hashed /
		 * compared columns in the input.
		 */
		for (i = 0; i < perhash->numCols; i++)
		{
			perhash->hashGrpColIdxInput[i] = grpColIdx[i];
			perhash->hashGrpColIdxHash[i] = i + 1;
			perhash->numhashGrpCols++;
			/* delete already mapped columns */
			bms_del_member(colnos, grpColIdx[i]);
		}

		/* and add the remaining columns */
		while ((i = bms_first_member(colnos)) >= 0)
		{
			perhash->hashGrpColIdxInput[perhash->numhashGrpCols] = i;
			perhash->numhashGrpCols++;
		}

		/* and build a tuple descriptor for the hashtable */
		for (i = 0; i < perhash->numhashGrpCols; i++)
		{
			int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

			hashTlist = lappend(hashTlist, list_nth(outerTlist, varNumber));
			perhash->largestGrpColIdx =
				Max(varNumber + 1, perhash->largestGrpColIdx);
		}

		hashDesc = ExecTypeFromTL(hashTlist);

		execTuplesHashPrepare(perhash->numCols,
							  perhash->aggnode->grpOperators,
							  &perhash->eqfuncoids,
							  &perhash->hashfunctions);
		perhash->hashslot =
			ExecAllocTableSlot(&estate->es_tupleTable, hashDesc,
							   &TTSOpsMinimalTuple);

		list_free(hashTlist);
		bms_free(colnos);
	}

	bms_free(base_colnos);
}

/*
 * Estimate per-hash-table-entry overhead for the planner.
 *
 * Note that the estimate does not include space for pass-by-reference
 * transition data values, nor for the representative tuple of each group.
 * Nor does this account of the target fill-factor and growth policy of the
 * hash table.
 */
Size
hash_agg_entry_size(int numAggs)
{
	Size		entrysize;

	/* This must match build_hash_table */
	entrysize = sizeof(TupleHashEntryData) +
		numAggs * sizeof(AggStatePerGroupData);
	entrysize = MAXALIGN(entrysize);

	return entrysize;
}

/*
 * Extract the attributes that make up the grouping key into the
 * hashslot. This is necessary to compute the hash of the grouping key.
 */
static void
prepare_hash_slot(AggState *aggstate)
{
	TupleTableSlot	*inputslot = aggstate->tmpcontext->ecxt_outertuple;
	AggStatePerHash	 perhash   = &aggstate->perhash[aggstate->current_set];
	TupleTableSlot	*hashslot  = perhash->hashslot;
	int				 i;

	/* transfer just the needed columns into hashslot */
	slot_getsomeattrs(inputslot, perhash->largestGrpColIdx);
	ExecClearTuple(hashslot);

	for (i = 0; i < perhash->numhashGrpCols; i++)
	{
		int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

		hashslot->tts_values[i] = inputslot->tts_values[varNumber];
		hashslot->tts_isnull[i] = inputslot->tts_isnull[varNumber];
	}
	ExecStoreVirtualTuple(hashslot);
}

/*
 * Recompile the expressions for advancing aggregates while hashing. This is
 * necessary for certain kinds of state changes that affect the resulting
 * expression. For instance, changing aggstate->hash_spilled or
 * aggstate->ss.ps.outerops require recompilation.
 */
static void
hash_recompile_expressions(AggState *aggstate)
{
	AggStatePerPhase phase;

	Assert(aggstate->aggstrategy == AGG_HASHED ||
		   aggstate->aggstrategy == AGG_MIXED);

	if (aggstate->aggstrategy == AGG_HASHED)
		phase = &aggstate->phases[0];
	else /* AGG_MIXED */
		phase = &aggstate->phases[1];

	phase->evaltrans = ExecBuildAggTrans(
		aggstate, phase,
		aggstate->aggstrategy == AGG_MIXED ? true : false, /* dosort */
		true, /* dohash */
		aggstate->hash_spilled /* spilled */);
}

/*
 * Calculate the hash value for a tuple. It's useful to do this outside of the
 * hash table so that we can reuse saved hash values rather than recomputing.
 */
static uint32
calculate_hash(AggState *aggstate)
{
	AggStatePerHash	 perhash   = &aggstate->perhash[aggstate->current_set];
	TupleHashTable	 hashtable = perhash->hashtable;
	MemoryContext	 oldContext;
	uint32			 hash;

	/* set up data needed by hash and match functions */
	hashtable->inputslot = perhash->hashslot;
	hashtable->in_hash_funcs = hashtable->tab_hash_funcs;
	hashtable->cur_eq_func = hashtable->tab_eq_func;

	/* Need to run the hash functions in short-lived context */
	oldContext = MemoryContextSwitchTo(hashtable->tempcxt);

	hash = TupleHashTableHash(hashtable->hashtab, NULL);

	MemoryContextSwitchTo(oldContext);

	return hash;
}

/*
 * Choose a reasonable number of buckets for the initial hash table size.
 */
static long
hash_choose_num_buckets(AggState *aggstate, long ngroups, Size memory)
{
	long	max_nbuckets;
	int		log2_ngroups;
	long	nbuckets;

	max_nbuckets = memory / aggstate->hashentrysize;

	/*
	 * Lowest power of two greater than ngroups, without exceeding
	 * max_nbuckets.
	 */
	for (log2_ngroups = 1, nbuckets = 2;
		 nbuckets < ngroups && nbuckets < max_nbuckets;
		 log2_ngroups++, nbuckets <<= 1);

	if (nbuckets > max_nbuckets && nbuckets > 2)
		nbuckets >>= 1;

	return nbuckets;
}

/*
 * Determine the number of partitions to create when spilling.
 */
static int
hash_choose_num_spill_partitions(uint64 input_groups, double hashentrysize)
{
	Size	mem_needed;
	int		partition_limit;
	int		npartitions;

	/*
	 * Avoid creating so many partitions that the memory requirements of the
	 * open partition files (estimated at BLCKSZ for buffering) are greater
	 * than 1/4 of work_mem.
	 */
	partition_limit = (work_mem * 1024L * 0.25) / BLCKSZ;

	/* pessimistically estimate that each input tuple creates a new group */
	mem_needed = HASH_PARTITION_FACTOR * input_groups * hashentrysize;

	/* make enough partitions so that each one is likely to fit in memory */
	npartitions = 1 + (mem_needed / (work_mem * 1024L));

	if (npartitions > partition_limit)
		npartitions = partition_limit;

	if (npartitions < HASH_MIN_PARTITIONS)
		npartitions = HASH_MIN_PARTITIONS;
	if (npartitions > HASH_MAX_PARTITIONS)
		npartitions = HASH_MAX_PARTITIONS;

	return npartitions;
}

/*
 * Find or create a hashtable entry for the tuple group containing the current
 * tuple (already set in tmpcontext's outertuple slot), in the current grouping
 * set (which the caller must have selected - note that initialize_aggregate
 * depends on this).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 *
 * If the hash table is at the memory limit, then only find existing hashtable
 * entries; don't create new ones. If a tuple's group is not already present
 * in the hash table for the current grouping set, return NULL and the caller
 * will spill it to disk.
 */
static AggStatePerGroup
lookup_hash_entry(AggState *aggstate, uint32 hash)
{
	AggStatePerHash perhash = &aggstate->perhash[aggstate->current_set];
	TupleTableSlot *hashslot = perhash->hashslot;
	TupleHashEntryData *entry;
	bool		isnew = false;
	bool	   *p_isnew;

	/* if hash table already spilled, don't create new entries */
	p_isnew = aggstate->hash_no_new_groups ? NULL : &isnew;

	/* find or create the hashtable entry using the filtered tuple */
	entry = LookupTupleHashEntryHash(perhash->hashtable, hashslot, p_isnew,
									 hash);

	if (entry == NULL)
		return NULL;

	if (isnew)
	{
		AggStatePerGroup	pergroup;
		int					transno;

		aggstate->hash_ngroups_current++;

		aggstate->hash_mem_current = MemoryContextMemAllocated(
			aggstate->hashcontext->ecxt_per_tuple_memory, true);

		if (aggstate->hash_mem_current > aggstate->hash_mem_peak)
			aggstate->hash_mem_peak = aggstate->hash_mem_current;

		/*
		 * Check whether we need to spill. For small values of work_mem, the
		 * empty hash tables might exceed it; so don't spill unless there's at
		 * least one group in the hash table.
		 */
		if (aggstate->hash_ngroups_current > 0 &&
			(aggstate->hash_mem_current     > aggstate->hash_mem_limit ||
			 aggstate->hash_ngroups_current > aggstate->hash_ngroups_limit))
		{
			aggstate->hash_no_new_groups = true;
			if (!aggstate->hash_spilled)
			{
				aggstate->hash_spilled = true;
				aggstate->hash_spills = palloc0(
					sizeof(HashAggSpill) * aggstate->num_hashes);

				hash_recompile_expressions(aggstate);
			}
		}

		pergroup = (AggStatePerGroup)
			MemoryContextAlloc(perhash->hashtable->tablecxt,
							   sizeof(AggStatePerGroupData) * aggstate->numtrans);
		entry->additional = pergroup;

		/*
		 * Initialize aggregates for new tuple group, lookup_hash_entries()
		 * already has selected the relevant grouping set.
		 */
		for (transno = 0; transno < aggstate->numtrans; transno++)
		{
			AggStatePerTrans pertrans = &aggstate->pertrans[transno];
			AggStatePerGroup pergroupstate = &pergroup[transno];

			initialize_aggregate(aggstate, pertrans, pergroupstate);
		}
	}

	return entry->additional;
}

/*
 * Look up hash entries for the current tuple in all hashed grouping sets,
 * returning an array of pergroup pointers suitable for advance_aggregates.
 *
 * Be aware that lookup_hash_entry can reset the tmpcontext.
 *
 * Some entries may be left NULL if we have reached the limit and have begun
 * to spill. The same tuple will belong to different groups for each set, so
 * may match a group already in memory for one set and match a group not in
 * memory for another set. If we have begun to spill and a tuple doesn't match
 * a group in memory for a particular set, it will be spilled.
 *
 * NB: It's possible to spill the same tuple for several different grouping
 * sets. This may seem wasteful, but it's actually a trade-off: if we spill
 * the tuple multiple times for multiple grouping sets, it can be partitioned
 * for each grouping set, making the refilling of the hash table very
 * efficient.
 */
static void
lookup_hash_entries(AggState *aggstate)
{
	AggStatePerGroup *pergroup = aggstate->hash_pergroup;
	int			setno;

	for (setno = 0; setno < aggstate->num_hashes; setno++)
	{
		uint32			hash;

		select_current_set(aggstate, setno, true);
		prepare_hash_slot(aggstate);
		hash = calculate_hash(aggstate);
		pergroup[setno] = lookup_hash_entry(aggstate, hash);

		/* check to see if we need to spill the tuple for this grouping set */
		if (pergroup[setno] == NULL)
		{
			AggStatePerHash perhash = &aggstate->perhash[setno];
			TupleTableSlot *inputslot = aggstate->tmpcontext->ecxt_outertuple;
			TupleTableSlot *spillslot = aggstate->hash_spill_slot;
			HashAggSpill   *spill = &aggstate->hash_spills[setno];
			int				idx;

			if (spill->partitions == NULL)
				hash_spill_init(spill, 0, perhash->aggnode->numGroups,
								aggstate->hashentrysize);

			/*
			 * Copy only necessary attributes to spill slot before writing to
			 * disk.
			 */
			ExecClearTuple(spillslot);
			memset(spillslot->tts_isnull, true,
				   spillslot->tts_tupleDescriptor->natts);

			/* deserialize needed attributes */
			if (perhash->numNeededColsInput > 0)
			{
				int maxNeededAttrIdx = perhash->numNeededColsInput - 1;
				AttrNumber maxNeededAttr =
					perhash->allNeededColsInput[maxNeededAttrIdx];
				slot_getsomeattrs(inputslot, maxNeededAttr);
			}

			for (idx = 0; idx < perhash->numNeededColsInput; idx++)
			{
				AttrNumber att = perhash->allNeededColsInput[idx];
				spillslot->tts_values[att-1] = inputslot->tts_values[att-1];
				spillslot->tts_isnull[att-1] = inputslot->tts_isnull[att-1];
			}

			ExecStoreVirtualTuple(spillslot);
			aggstate->hash_disk_used += hash_spill_tuple(spill, 0, spillslot, hash);
		}
	}
}

/*
 * ExecAgg -
 *
 *	  ExecAgg receives tuples from its outer subplan and aggregates over
 *	  the appropriate attribute for each aggregate function use (Aggref
 *	  node) appearing in the targetlist or qual of the node.  The number
 *	  of tuples to aggregate over depends on whether grouped or plain
 *	  aggregation is selected.  In grouped aggregation, we produce a result
 *	  row for each group; in plain aggregation there's a single result row
 *	  for the whole query.  In either case, the value of each aggregate is
 *	  stored in the expression context to be used when ExecProject evaluates
 *	  the result tuple.
 */
static TupleTableSlot *
ExecAgg(PlanState *pstate)
{
	AggState   *node = castNode(AggState, pstate);
	TupleTableSlot *result = NULL;

	CHECK_FOR_INTERRUPTS();

	if (!node->agg_done)
	{
		/* Dispatch based on strategy */
		switch (node->phase->aggstrategy)
		{
			case AGG_HASHED:
				if (!node->table_filled)
					agg_fill_hash_table(node);
				/* FALLTHROUGH */
			case AGG_MIXED:
				result = agg_retrieve_hash_table(node);
				break;
			case AGG_PLAIN:
			case AGG_SORTED:
				result = agg_retrieve_direct(node);
				break;
		}

		if (!TupIsNull(result))
			return result;
	}

	return NULL;
}

/*
 * ExecAgg for non-hashed case
 */
static TupleTableSlot *
agg_retrieve_direct(AggState *aggstate)
{
	Agg		   *node = aggstate->phase->aggnode;
	ExprContext *econtext;
	ExprContext *tmpcontext;
	AggStatePerAgg peragg;
	AggStatePerGroup *pergroups;
	TupleTableSlot *outerslot;
	TupleTableSlot *firstSlot;
	TupleTableSlot *result;
	bool		hasGroupingSets = aggstate->phase->numsets > 0;
	int			numGroupingSets = Max(aggstate->phase->numsets, 1);
	int			currentSet;
	int			nextSetSize;
	int			numReset;
	int			i;

	/*
	 * get state info from node
	 *
	 * econtext is the per-output-tuple expression context
	 *
	 * tmpcontext is the per-input-tuple expression context
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	tmpcontext = aggstate->tmpcontext;

	peragg = aggstate->peragg;
	pergroups = aggstate->pergroups;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * We loop retrieving groups until we find one matching
	 * aggstate->ss.ps.qual
	 *
	 * For grouping sets, we have the invariant that aggstate->projected_set
	 * is either -1 (initial call) or the index (starting from 0) in
	 * gset_lengths for the group we just completed (either by projecting a
	 * row or by discarding it in the qual).
	 */
	while (!aggstate->agg_done)
	{
		/*
		 * Clear the per-output-tuple context for each group, as well as
		 * aggcontext (which contains any pass-by-ref transvalues of the old
		 * group).  Some aggregate functions store working state in child
		 * contexts; those now get reset automatically without us needing to
		 * do anything special.
		 *
		 * We use ReScanExprContext not just ResetExprContext because we want
		 * any registered shutdown callbacks to be called.  That allows
		 * aggregate functions to ensure they've cleaned up any non-memory
		 * resources.
		 */
		ReScanExprContext(econtext);

		/*
		 * Determine how many grouping sets need to be reset at this boundary.
		 */
		if (aggstate->projected_set >= 0 &&
			aggstate->projected_set < numGroupingSets)
			numReset = aggstate->projected_set + 1;
		else
			numReset = numGroupingSets;

		/*
		 * numReset can change on a phase boundary, but that's OK; we want to
		 * reset the contexts used in _this_ phase, and later, after possibly
		 * changing phase, initialize the right number of aggregates for the
		 * _new_ phase.
		 */

		for (i = 0; i < numReset; i++)
		{
			ReScanExprContext(aggstate->aggcontexts[i]);
		}

		/*
		 * Check if input is complete and there are no more groups to project
		 * in this phase; move to next phase or mark as done.
		 */
		if (aggstate->input_done == true &&
			aggstate->projected_set >= (numGroupingSets - 1))
		{
			if (aggstate->current_phase < aggstate->numphases - 1)
			{
				initialize_phase(aggstate, aggstate->current_phase + 1);
				aggstate->input_done = false;
				aggstate->projected_set = -1;
				numGroupingSets = Max(aggstate->phase->numsets, 1);
				node = aggstate->phase->aggnode;
				numReset = numGroupingSets;
			}
			else if (aggstate->aggstrategy == AGG_MIXED)
			{
				/*
				 * Mixed mode; we've output all the grouped stuff and have
				 * full hashtables, so switch to outputting those.
				 */
				initialize_phase(aggstate, 0);
				aggstate->table_filled = true;
				ResetTupleHashIterator(aggstate->perhash[0].hashtable,
									   &aggstate->perhash[0].hashiter);
				select_current_set(aggstate, 0, true);
				return agg_retrieve_hash_table(aggstate);
			}
			else
			{
				aggstate->agg_done = true;
				break;
			}
		}

		/*
		 * Get the number of columns in the next grouping set after the last
		 * projected one (if any). This is the number of columns to compare to
		 * see if we reached the boundary of that set too.
		 */
		if (aggstate->projected_set >= 0 &&
			aggstate->projected_set < (numGroupingSets - 1))
			nextSetSize = aggstate->phase->gset_lengths[aggstate->projected_set + 1];
		else
			nextSetSize = 0;

		/*----------
		 * If a subgroup for the current grouping set is present, project it.
		 *
		 * We have a new group if:
		 *	- we're out of input but haven't projected all grouping sets
		 *	  (checked above)
		 * OR
		 *	  - we already projected a row that wasn't from the last grouping
		 *		set
		 *	  AND
		 *	  - the next grouping set has at least one grouping column (since
		 *		empty grouping sets project only once input is exhausted)
		 *	  AND
		 *	  - the previous and pending rows differ on the grouping columns
		 *		of the next grouping set
		 *----------
		 */
		tmpcontext->ecxt_innertuple = econtext->ecxt_outertuple;
		if (aggstate->input_done ||
			(node->aggstrategy != AGG_PLAIN &&
			 aggstate->projected_set != -1 &&
			 aggstate->projected_set < (numGroupingSets - 1) &&
			 nextSetSize > 0 &&
			 !ExecQualAndReset(aggstate->phase->eqfunctions[nextSetSize - 1],
							   tmpcontext)))
		{
			aggstate->projected_set += 1;

			Assert(aggstate->projected_set < numGroupingSets);
			Assert(nextSetSize > 0 || aggstate->input_done);
		}
		else
		{
			/*
			 * We no longer care what group we just projected, the next
			 * projection will always be the first (or only) grouping set
			 * (unless the input proves to be empty).
			 */
			aggstate->projected_set = 0;

			/*
			 * If we don't already have the first tuple of the new group,
			 * fetch it from the outer plan.
			 */
			if (aggstate->grp_firstTuple == NULL)
			{
				outerslot = fetch_input_tuple(aggstate);
				if (!TupIsNull(outerslot))
				{
					/*
					 * Make a copy of the first input tuple; we will use this
					 * for comparisons (in group mode) and for projection.
					 */
					aggstate->grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
				}
				else
				{
					/* outer plan produced no tuples at all */
					if (hasGroupingSets)
					{
						/*
						 * If there was no input at all, we need to project
						 * rows only if there are grouping sets of size 0.
						 * Note that this implies that there can't be any
						 * references to ungrouped Vars, which would otherwise
						 * cause issues with the empty output slot.
						 *
						 * XXX: This is no longer true, we currently deal with
						 * this in finalize_aggregates().
						 */
						aggstate->input_done = true;

						while (aggstate->phase->gset_lengths[aggstate->projected_set] > 0)
						{
							aggstate->projected_set += 1;
							if (aggstate->projected_set >= numGroupingSets)
							{
								/*
								 * We can't set agg_done here because we might
								 * have more phases to do, even though the
								 * input is empty. So we need to restart the
								 * whole outer loop.
								 */
								break;
							}
						}

						if (aggstate->projected_set >= numGroupingSets)
							continue;
					}
					else
					{
						aggstate->agg_done = true;
						/* If we are grouping, we should produce no tuples too */
						if (node->aggstrategy != AGG_PLAIN)
							return NULL;
					}
				}
			}

			/*
			 * Initialize working state for a new input tuple group.
			 */
			initialize_aggregates(aggstate, pergroups, numReset);

			if (aggstate->grp_firstTuple != NULL)
			{
				/*
				 * Store the copied first input tuple in the tuple table slot
				 * reserved for it.  The tuple will be deleted when it is
				 * cleared from the slot.
				 */
				ExecForceStoreHeapTuple(aggstate->grp_firstTuple,
										firstSlot, true);
				aggstate->grp_firstTuple = NULL;	/* don't keep two pointers */

				/* set up for first advance_aggregates call */
				tmpcontext->ecxt_outertuple = firstSlot;

				/*
				 * Process each outer-plan tuple, and then fetch the next one,
				 * until we exhaust the outer plan or cross a group boundary.
				 */
				for (;;)
				{
					/*
					 * During phase 1 only of a mixed agg, we need to update
					 * hashtables as well in advance_aggregates.
					 */
					if (aggstate->aggstrategy == AGG_MIXED &&
						aggstate->current_phase == 1)
					{
						lookup_hash_entries(aggstate);
					}

					/* Advance the aggregates (or combine functions) */
					advance_aggregates(aggstate);

					/* Reset per-input-tuple context after each tuple */
					ResetExprContext(tmpcontext);

					outerslot = fetch_input_tuple(aggstate);
					if (TupIsNull(outerslot))
					{
						/* no more outer-plan tuples available */

						/* if we built hash tables, finalize any spills */
						if (aggstate->aggstrategy == AGG_MIXED &&
							aggstate->current_phase == 1)
							hash_finish_initial_spills(aggstate);

						if (hasGroupingSets)
						{
							aggstate->input_done = true;
							break;
						}
						else
						{
							aggstate->agg_done = true;
							break;
						}
					}
					/* set up for next advance_aggregates call */
					tmpcontext->ecxt_outertuple = outerslot;

					/*
					 * If we are grouping, check whether we've crossed a group
					 * boundary.
					 */
					if (node->aggstrategy != AGG_PLAIN)
					{
						tmpcontext->ecxt_innertuple = firstSlot;
						if (!ExecQual(aggstate->phase->eqfunctions[node->numCols - 1],
									  tmpcontext))
						{
							aggstate->grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
							break;
						}
					}
				}
			}

			/*
			 * Use the representative input tuple for any references to
			 * non-aggregated input columns in aggregate direct args, the node
			 * qual, and the tlist.  (If we are not grouping, and there are no
			 * input rows at all, we will come here with an empty firstSlot
			 * ... but if not grouping, there can't be any references to
			 * non-aggregated input columns, so no problem.)
			 */
			econtext->ecxt_outertuple = firstSlot;
		}

		Assert(aggstate->projected_set >= 0);

		currentSet = aggstate->projected_set;

		prepare_projection_slot(aggstate, econtext->ecxt_outertuple, currentSet);

		select_current_set(aggstate, currentSet, false);

		finalize_aggregates(aggstate,
							peragg,
							pergroups[currentSet]);

		/*
		 * If there's no row to project right now, we must continue rather
		 * than returning a null since there might be more groups.
		 */
		result = project_aggregates(aggstate);
		if (result)
			return result;
	}

	/* No more groups */
	return NULL;
}

/*
 * ExecAgg for hashed case: read input and build hash table
 */
static void
agg_fill_hash_table(AggState *aggstate)
{
	TupleTableSlot *outerslot;
	ExprContext *tmpcontext = aggstate->tmpcontext;

	/*
	 * Process each outer-plan tuple, and then fetch the next one, until we
	 * exhaust the outer plan.
	 */
	for (;;)
	{
		outerslot = fetch_input_tuple(aggstate);
		if (TupIsNull(outerslot))
			break;

		/* set up for lookup_hash_entries and advance_aggregates */
		tmpcontext->ecxt_outertuple = outerslot;

		/* Find or build hashtable entries */
		lookup_hash_entries(aggstate);

		/* Advance the aggregates (or combine functions) */
		advance_aggregates(aggstate);

		/*
		 * Reset per-input-tuple context after each tuple, but note that the
		 * hash lookups do this too
		 */
		ResetExprContext(aggstate->tmpcontext);
	}

	/* finalize spills, if any */
	hash_finish_initial_spills(aggstate);

	aggstate->table_filled = true;
	/* Initialize to walk the first hash table */
	select_current_set(aggstate, 0, true);
	ResetTupleHashIterator(aggstate->perhash[0].hashtable,
						   &aggstate->perhash[0].hashiter);
}

/*
 * If any data was spilled during hash aggregation, reset the hash table and
 * reprocess one batch of spilled data. After reprocessing a batch, the hash
 * table will again contain data, ready to be consumed by
 * agg_retrieve_hash_table_in_memory().
 *
 * Should only be called after all in memory hash table entries have been
 * consumed.
 *
 * Return false when input is exhausted and there's no more work to be done;
 * otherwise return true.
 */
static bool
agg_refill_hash_table(AggState *aggstate)
{
	HashAggBatch	*batch;

	if (aggstate->hash_batches == NIL)
		return false;

	/*
	 * Each spill file contains spilled data for only a single grouping
	 * set. We want to ignore all others, which is done by setting the other
	 * pergroups to NULL.
	 */
	memset(aggstate->all_pergroups, 0,
		   sizeof(AggStatePerGroup) *
		   (aggstate->maxsets + aggstate->num_hashes));

	batch = linitial(aggstate->hash_batches);
	aggstate->hash_batches = list_delete_first(aggstate->hash_batches);

	/*
	 * Free memory and rebuild a single hash table for this batch's grouping
	 * set.
	 */
	ReScanExprContext(aggstate->hashcontext);

	/* estimate the number of groups to be the number of input tuples */
	build_hash_table(aggstate, batch->setno, batch->input_tuples);

	Assert(aggstate->current_phase == 0);

	if (aggstate->phase->aggstrategy == AGG_MIXED)
	{
		aggstate->current_phase = 1;
		aggstate->phase = &aggstate->phases[aggstate->current_phase];
	}

	/*
	 * The first pass (agg_fill_hash_table) reads whatever kind of slot comes
	 * from the outer plan, and considers the slot fixed. But spilled tuples
	 * are always MinimalTuples, so if that's different from the outer plan we
	 * need to change it and recompile the aggregate expressions.
	 */
	if (aggstate->ss.ps.outerops != &TTSOpsMinimalTuple)
	{
		aggstate->ss.ps.outerops = &TTSOpsMinimalTuple;
		hash_recompile_expressions(aggstate);
	}

	for (;;) {
		TupleTableSlot	*slot = aggstate->hash_spill_slot;
		MinimalTuple	 tuple;
		uint32			 hash;

		CHECK_FOR_INTERRUPTS();

		tuple = hash_read_spilled(batch->lts, batch->input_tape, &hash);
		if (tuple == NULL)
			break;

		ExecStoreMinimalTuple(tuple, slot, true);
		aggstate->tmpcontext->ecxt_outertuple = slot;

		select_current_set(aggstate, batch->setno, true);
		prepare_hash_slot(aggstate);
		aggstate->hash_pergroup[batch->setno] = lookup_hash_entry(aggstate, hash);

		/* if there's no memory for a new group, spill */
		if (aggstate->hash_pergroup[batch->setno] == NULL)
		{
			if (batch->spill.partitions == NULL)
			{
				/*
				 * Estimate the number of groups for this batch as the total
				 * number of tuples in its input file. Although that's a worst
				 * case, it's not bad here for two reasons: (1) overestimating
				 * is better than underestimating; and (2) we've already
				 * scanned the relation once, so it's likely that we've
				 * already finalized many of the common values.
				 */
				hash_spill_init(&batch->spill, batch->input_bits,
								batch->input_tuples, aggstate->hashentrysize);
			}

			/*
			 * we use the same logical tape set, which allocates no
			 * extra space while re-spilling
			 */
			/* aggstate->hash_disk_used += */
			hash_spill_tuple(&batch->spill, batch->input_bits, slot, hash);
		}

		/* Advance the aggregates (or combine functions) */
		advance_aggregates(aggstate);

		/*
		 * Reset per-input-tuple context after each tuple, but note that the
		 * hash lookups do this too
		 */
		ResetExprContext(aggstate->tmpcontext);
	}

	aggstate->current_phase = 0;
	aggstate->phase = &aggstate->phases[aggstate->current_phase];

	/* update hashentrysize estimate based on contents */
	if (aggstate->hash_ngroups_current > 0)
	{
		aggstate->hashentrysize = (double)aggstate->hash_mem_current /
			(double)aggstate->hash_ngroups_current;
	}

	hash_spill_finish(aggstate, &batch->spill, batch->setno,
					  batch->input_bits);

	pfree(batch);

	/* Initialize to walk the first hash table */
	select_current_set(aggstate, 0, true);
	ResetTupleHashIterator(aggstate->perhash[0].hashtable,
						   &aggstate->perhash[0].hashiter);

	return true;
}

/*
 * ExecAgg for hashed case: retrieving groups from hash table
 *
 * After exhausting in-memory tuples, also try refilling the hash table using
 * previously-spilled tuples. Only returns NULL after all in-memory and
 * spilled tuples are exhausted.
 */
static TupleTableSlot *
agg_retrieve_hash_table(AggState *aggstate)
{
	TupleTableSlot *result = NULL;

	while (result == NULL)
	{
		result = agg_retrieve_hash_table_in_memory(aggstate);
		if (result == NULL)
		{
			if (!agg_refill_hash_table(aggstate))
			{
				aggstate->agg_done = true;
				break;
			}
		}
	}

	return result;
}

/*
 * Retrieve the groups from the in-memory hash tables without considering any
 * spilled tuples.
 */
static TupleTableSlot *
agg_retrieve_hash_table_in_memory(AggState *aggstate)
{
	ExprContext *econtext;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	TupleHashEntryData *entry;
	TupleTableSlot *firstSlot;
	TupleTableSlot *result;
	AggStatePerHash perhash;

	/*
	 * get state info from node.
	 *
	 * econtext is the per-output-tuple expression context.
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	peragg = aggstate->peragg;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * Note that perhash (and therefore anything accessed through it) can
	 * change inside the loop, as we change between grouping sets.
	 */
	perhash = &aggstate->perhash[aggstate->current_set];

	/*
	 * We loop retrieving groups until we find one satisfying
	 * aggstate->ss.ps.qual
	 */
	for (;;)
	{
		TupleTableSlot *hashslot = perhash->hashslot;
		int			i;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Find the next entry in the hash table
		 */
		entry = ScanTupleHashTable(perhash->hashtable, &perhash->hashiter);
		if (entry == NULL)
		{
			int			nextset = aggstate->current_set + 1;

			if (nextset < aggstate->num_hashes)
			{
				/*
				 * Switch to next grouping set, reinitialize, and restart the
				 * loop.
				 */
				select_current_set(aggstate, nextset, true);

				perhash = &aggstate->perhash[aggstate->current_set];

				ResetTupleHashIterator(perhash->hashtable, &perhash->hashiter);

				continue;
			}
			else
			{
				return NULL;
			}
		}

		/*
		 * Clear the per-output-tuple context for each group
		 *
		 * We intentionally don't use ReScanExprContext here; if any aggs have
		 * registered shutdown callbacks, they mustn't be called yet, since we
		 * might not be done with that agg.
		 */
		ResetExprContext(econtext);

		/*
		 * Transform representative tuple back into one with the right
		 * columns.
		 */
		ExecStoreMinimalTuple(entry->firstTuple, hashslot, false);
		slot_getallattrs(hashslot);

		ExecClearTuple(firstSlot);
		memset(firstSlot->tts_isnull, true,
			   firstSlot->tts_tupleDescriptor->natts * sizeof(bool));

		for (i = 0; i < perhash->numhashGrpCols; i++)
		{
			int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

			firstSlot->tts_values[varNumber] = hashslot->tts_values[i];
			firstSlot->tts_isnull[varNumber] = hashslot->tts_isnull[i];
		}
		ExecStoreVirtualTuple(firstSlot);

		pergroup = (AggStatePerGroup) entry->additional;

		/*
		 * Use the representative input tuple for any references to
		 * non-aggregated input columns in the qual and tlist.
		 */
		econtext->ecxt_outertuple = firstSlot;

		prepare_projection_slot(aggstate,
								econtext->ecxt_outertuple,
								aggstate->current_set);

		finalize_aggregates(aggstate, peragg, pergroup);

		result = project_aggregates(aggstate);
		if (result)
			return result;
	}

	/* No more groups */
	return NULL;
}

/*
 * hash_spill_init
 *
 * Called after we determined that spilling is necessary. Chooses the number
 * of partitions to create, and initializes them.
 */
static void
hash_spill_init(HashAggSpill *spill, int input_bits, uint64 input_groups,
				double hashentrysize)
{
	int     npartitions;
	int     partition_bits;
	int     i;
	int     j;
	int     old_npartitions;

	npartitions = hash_choose_num_spill_partitions(input_groups,
												   hashentrysize);
	partition_bits = my_log2(npartitions);

	/* make sure that we don't exhaust the hash bits */
	if (partition_bits + input_bits >= 32)
		partition_bits = 32 - input_bits;

	/* number of partitions will be a power of two */
	npartitions = 1L << partition_bits;

	if (spill->lts == NULL)
	{
		spill->partition_bits = partition_bits;
		spill->n_partitions   = npartitions;
		spill->partitions     = palloc0(sizeof(int) * npartitions);
		for (i = 0; i < spill->n_partitions; ++i)
		{
			spill->partitions[i] = i;
		}
		spill->ntuples        = palloc0(sizeof(int64) * spill->n_partitions);
		spill->lts            = LogicalTapeSetCreate(npartitions, NULL, NULL, 0); // TODO: worker is 0?
	}
	else /* re-spilling */
	{
		old_npartitions = LogicalTapeGetNTapes(spill->lts);
		spill->partition_bits = my_log2(npartitions);
		spill->n_partitions   = (1L << spill->partition_bits);
		spill->partitions     = palloc0(sizeof(int) * npartitions);
		j = old_npartitions;
		for (i = 0; i < spill->n_partitions; ++i)
		{
			spill->partitions[i] = j;
			j++;
		}
		spill->ntuples        = palloc0(sizeof(int64) * spill->n_partitions);
		spill->lts            = LogicalTapeSetExtend(spill->lts, spill->n_partitions);
	}
}

/*
 * hash_spill_tuple
 *
 * No room for new groups in the hash table. Save for later in the appropriate
 * partition spill file.
 */
static Size
hash_spill_tuple(HashAggSpill *spill, int input_bits, TupleTableSlot *slot,
				 uint32 hash)
{
	int					 partition;
	MinimalTuple		 tuple;
	int					 total_written = 0;
	bool				 shouldFree;

	Assert(spill->partitions != NULL);

	/*
	 * When spilling tuples from the input, the slot will be virtual
	 * (containing only the needed attributes and the rest as NULL), and we
	 * need to materialize the minimal tuple. When spilling tuples
	 * recursively, the slot will hold a minimal tuple already.
	 */
	tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);

	if (spill->partition_bits == 0)
		partition = 0;
	else
		partition = (hash << input_bits) >>
			(32 - spill->partition_bits);

	spill->ntuples[partition]++;

	LogicalTapeWrite(spill->lts, spill->partitions[partition], (void *) &hash, sizeof(uint32));
	total_written += sizeof(uint32);

	LogicalTapeWrite(spill->lts, spill->partitions[partition], (void *) tuple, tuple->t_len);
	total_written += tuple->t_len;

	if (shouldFree)
		pfree(tuple);

	return total_written;
}

/*
 * read_spilled_tuple
 * 		read the next tuple from a batch file.  Return NULL if no more.
 */
static MinimalTuple
hash_read_spilled(LogicalTapeSet *lts, int tapenum, uint32 *hashp)
{
	MinimalTuple	tuple;
	uint32			t_len;
	size_t			nread;
	uint32			hash;

	nread = LogicalTapeRead(lts, tapenum, &hash, sizeof(uint32));
	if (nread == 0)
		return NULL;
	if (nread != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read the hash from HashAgg spilled tape: %m")));
	if (hashp != NULL)
		*hashp = hash;

	nread = LogicalTapeRead(lts, tapenum, &t_len, sizeof(t_len));
	if (nread != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read the t_len from HashAgg spilled tape: %m")));

	tuple = (MinimalTuple) palloc(t_len);
	tuple->t_len = t_len;

	nread = LogicalTapeRead(lts, tapenum, (void *)((char *)tuple + sizeof(uint32)), t_len - sizeof(uint32));
	if (nread != t_len - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read the data from HashAgg spilled tape: %m")));

	return tuple;
}

/*
 * new_hashagg_batch
 *
 * Construct a HashAggBatch item, which represents one iteration of HashAgg to
 * be done. Should be called in the aggregate's memory context.
 */
static HashAggBatch *
hash_batch_new(LogicalTapeSet *lts, int tapenum, int setno, int64 input_tuples,
			   int input_bits)
{
	HashAggBatch *batch = palloc0(sizeof(HashAggBatch));

	batch->input_tape = tapenum;
	batch->input_bits = input_bits;
	batch->input_tuples = input_tuples;
	batch->setno = setno;
	batch->lts = lts;
	batch->spill.lts = lts; /* share the same logical tape set if this batch re-spills */

	/* batch->spill will be set only after spilling this batch */

	return batch;
}

/*
 * hash_finish_initial_spills
 *
 * After a HashAggBatch has been processed, it may have spilled tuples to
 * disk. If so, turn the spilled partitions into new batches that must later
 * be executed.
 */
static void
hash_finish_initial_spills(AggState *aggstate)
{
	int setno;

	if (aggstate->hash_spills == NULL)
		return;

	/* update hashentrysize estimate based on contents */
	Assert(aggstate->hash_ngroups_current > 0);
	aggstate->hashentrysize = (double)aggstate->hash_mem_current /
		(double)aggstate->hash_ngroups_current;

	for (setno = 0; setno < aggstate->num_hashes; setno++)
		hash_spill_finish(aggstate, &aggstate->hash_spills[setno], setno, 0);

	pfree(aggstate->hash_spills);
	aggstate->hash_spills = NULL;
}

/*
 * hash_spill_finish
 *
 * Transform spill files into new batches.
 */
static void
hash_spill_finish(AggState *aggstate, HashAggSpill *spill, int setno, int input_bits)
{
	int i;

	if (spill->n_partitions == 0)
		return;	/* didn't spill */

	for (i = 0; i < spill->n_partitions; i++)
	{
		MemoryContext    oldContext;
		HashAggBatch    *new_batch;

		oldContext = MemoryContextSwitchTo(aggstate->ss.ps.state->es_query_cxt);
		LogicalTapeRewindForRead(spill->lts, spill->partitions[i], 0);
		new_batch = hash_batch_new(spill->lts, spill->partitions[i], setno, spill->ntuples[i],
					   spill->partition_bits + input_bits);
		aggstate->hash_batches = lappend(aggstate->hash_batches, new_batch);
		aggstate->hash_batches_used++;
		MemoryContextSwitchTo(oldContext);
	}

	/* remember all the logical tape sets for closing */
	if (!list_member_ptr(aggstate->lts_list, spill->lts))
		aggstate->lts_list = lappend(aggstate->lts_list, spill->lts);

	pfree(spill->ntuples);
	pfree(spill->partitions);
}

/*
 * Clear a HashAggSpill, free its memory, and close its files.
 */
static void
hash_reset_spill(HashAggSpill *spill)
{
	if (spill->lts != NULL)
	{
		LogicalTapeSetClose(spill->lts);
		spill->lts = NULL;
	}
	if (spill->ntuples != NULL)
		pfree(spill->ntuples);
	if (spill->partitions != NULL)
		pfree(spill->partitions);
}

/*
 * Find and reset all active HashAggSpills.
 */
static void
hash_reset_spills(AggState *aggstate)
{
	ListCell *lc;

	if (aggstate->hash_spills != NULL)
	{
		int setno;

		for (setno = 0; setno < aggstate->num_hashes; setno++)
			hash_reset_spill(&aggstate->hash_spills[setno]);

		pfree(aggstate->hash_spills);
		aggstate->hash_spills = NULL;
	}

	foreach(lc, aggstate->hash_batches)
	{
		HashAggBatch *batch = (HashAggBatch*) lfirst(lc);
		hash_reset_spill(&batch->spill);
		pfree(batch);
	}
	list_free(aggstate->hash_batches);
	aggstate->hash_batches = NIL;

	foreach(lc, aggstate->lts_list)
	{
		LogicalTapeSet *lts = (LogicalTapeSet *) lfirst(lc);
		LogicalTapeSetClose(lts);
	}
	list_free(aggstate->lts_list);
	aggstate->lts_list = NIL;
}


/* -----------------
 * ExecInitAgg
 *
 *	Creates the run-time information for the agg node produced by the
 *	planner and initializes its outer subtree.
 *
 * -----------------
 */
AggState *
ExecInitAgg(Agg *node, EState *estate, int eflags)
{
	AggState   *aggstate;
	AggStatePerAgg peraggs;
	AggStatePerTrans pertransstates;
	AggStatePerGroup *pergroups;
	Plan	   *outerPlan;
	ExprContext *econtext;
	TupleDesc	scanDesc;
	int			numaggs,
				transno,
				aggno;
	int			phase;
	int			phaseidx;
	ListCell   *l;
	Bitmapset  *all_grouped_cols = NULL;
	int			numGroupingSets = 1;
	int			numPhases;
	int			numHashes;
	int			i = 0;
	int			j = 0;
	bool		use_hashing = (node->aggstrategy == AGG_HASHED ||
							   node->aggstrategy == AGG_MIXED);

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	aggstate = makeNode(AggState);
	aggstate->ss.ps.plan = (Plan *) node;
	aggstate->ss.ps.state = estate;
	aggstate->ss.ps.ExecProcNode = ExecAgg;

	aggstate->aggs = NIL;
	aggstate->numaggs = 0;
	aggstate->numtrans = 0;
	aggstate->aggstrategy = node->aggstrategy;
	aggstate->aggsplit = node->aggsplit;
	aggstate->maxsets = 0;
	aggstate->projected_set = -1;
	aggstate->current_set = 0;
	aggstate->peragg = NULL;
	aggstate->pertrans = NULL;
	aggstate->curperagg = NULL;
	aggstate->curpertrans = NULL;
	aggstate->input_done = false;
	aggstate->agg_done = false;
	aggstate->pergroups = NULL;
	aggstate->grp_firstTuple = NULL;
	aggstate->sort_in = NULL;
	aggstate->sort_out = NULL;

	/*
	 * phases[0] always exists, but is dummy in sorted/plain mode
	 */
	numPhases = (use_hashing ? 1 : 2);
	numHashes = (use_hashing ? 1 : 0);

	/*
	 * Calculate the maximum number of grouping sets in any phase; this
	 * determines the size of some allocations.  Also calculate the number of
	 * phases, since all hashed/mixed nodes contribute to only a single phase.
	 */
	if (node->groupingSets)
	{
		numGroupingSets = list_length(node->groupingSets);

		foreach(l, node->chain)
		{
			Agg		   *agg = lfirst(l);

			numGroupingSets = Max(numGroupingSets,
								  list_length(agg->groupingSets));

			/*
			 * additional AGG_HASHED aggs become part of phase 0, but all
			 * others add an extra phase.
			 */
			if (agg->aggstrategy != AGG_HASHED)
				++numPhases;
			else
				++numHashes;
		}
	}

	aggstate->maxsets = numGroupingSets;
	aggstate->numphases = numPhases;

	aggstate->aggcontexts = (ExprContext **)
		palloc0(sizeof(ExprContext *) * numGroupingSets);

	/*
	 * Create expression contexts.  We need three or more, one for
	 * per-input-tuple processing, one for per-output-tuple processing, one
	 * for all the hashtables, and one for each grouping set.  The per-tuple
	 * memory context of the per-grouping-set ExprContexts (aggcontexts)
	 * replaces the standalone memory context formerly used to hold transition
	 * values.  We cheat a little by using ExecAssignExprContext() to build
	 * all of them.
	 *
	 * NOTE: the details of what is stored in aggcontexts and what is stored
	 * in the regular per-query memory context are driven by a simple
	 * decision: we want to reset the aggcontext at group boundaries (if not
	 * hashing) and in ExecReScanAgg to recover no-longer-wanted space.
	 */
	ExecAssignExprContext(estate, &aggstate->ss.ps);
	aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;

	for (i = 0; i < numGroupingSets; ++i)
	{
		ExecAssignExprContext(estate, &aggstate->ss.ps);
		aggstate->aggcontexts[i] = aggstate->ss.ps.ps_ExprContext;
	}

	if (use_hashing)
	{
		ExecAssignExprContext(estate, &aggstate->ss.ps);
		aggstate->hashcontext = aggstate->ss.ps.ps_ExprContext;
	}

	ExecAssignExprContext(estate, &aggstate->ss.ps);

	/*
	 * Initialize child nodes.
	 *
	 * If we are doing a hashed aggregation then the child plan does not need
	 * to handle REWIND efficiently; see ExecReScanAgg.
	 */
	if (node->aggstrategy == AGG_HASHED)
		eflags &= ~EXEC_FLAG_REWIND;
	outerPlan = outerPlan(node);
	outerPlanState(aggstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * initialize source tuple type.
	 */
	aggstate->ss.ps.outerops =
		ExecGetResultSlotOps(outerPlanState(&aggstate->ss),
							 &aggstate->ss.ps.outeropsfixed);
	aggstate->ss.ps.outeropsset = true;

	ExecCreateScanSlotFromOuterPlan(estate, &aggstate->ss,
									aggstate->ss.ps.outerops);
	scanDesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	/*
	 * If there are more than two phases (including a potential dummy phase
	 * 0), input will be resorted using tuplesort. Need a slot for that.
	 */
	if (numPhases > 2)
	{
		aggstate->sort_slot = ExecInitExtraTupleSlot(estate, scanDesc,
													 &TTSOpsMinimalTuple);

		/*
		 * The output of the tuplesort, and the output from the outer child
		 * might not use the same type of slot. In most cases the child will
		 * be a Sort, and thus return a TTSOpsMinimalTuple type slot - but the
		 * input can also be be presorted due an index, in which case it could
		 * be a different type of slot.
		 *
		 * XXX: For efficiency it would be good to instead/additionally
		 * generate expressions with corresponding settings of outerops* for
		 * the individual phases - deforming is often a bottleneck for
		 * aggregations with lots of rows per group. If there's multiple
		 * sorts, we know that all but the first use TTSOpsMinimalTuple (via
		 * the nodeAgg.c internal tuplesort).
		 */
		if (aggstate->ss.ps.outeropsfixed &&
			aggstate->ss.ps.outerops != &TTSOpsMinimalTuple)
			aggstate->ss.ps.outeropsfixed = false;
	}

	if (use_hashing)
		aggstate->hash_spill_slot = ExecInitExtraTupleSlot(estate, scanDesc,
														   &TTSOpsMinimalTuple);

	/*
	 * Initialize result type, slot and projection.
	 */
	ExecInitResultTupleSlotTL(&aggstate->ss.ps, &TTSOpsVirtual);
	ExecAssignProjectionInfo(&aggstate->ss.ps, NULL);

	/*
	 * initialize child expressions
	 *
	 * We expect the parser to have checked that no aggs contain other agg
	 * calls in their arguments (and just to be sure, we verify it again while
	 * initializing the plan node).  This would make no sense under SQL
	 * semantics, and it's forbidden by the spec.  Because it is true, we
	 * don't need to worry about evaluating the aggs in any particular order.
	 *
	 * Note: execExpr.c finds Aggrefs for us, and adds their AggrefExprState
	 * nodes to aggstate->aggs.  Aggrefs in the qual are found here; Aggrefs
	 * in the targetlist are found during ExecAssignProjectionInfo, below.
	 */
	aggstate->ss.ps.qual =
		ExecInitQual(node->plan.qual, (PlanState *) aggstate);

	/*
	 * We should now have found all Aggrefs in the targetlist and quals.
	 */
	numaggs = aggstate->numaggs;
	Assert(numaggs == list_length(aggstate->aggs));

	/*
	 * For each phase, prepare grouping set data and fmgr lookup data for
	 * compare functions.  Accumulate all_grouped_cols in passing.
	 */
	aggstate->phases = palloc0(numPhases * sizeof(AggStatePerPhaseData));

	aggstate->num_hashes = numHashes;
	if (numHashes)
	{
		aggstate->perhash = palloc0(sizeof(AggStatePerHashData) * numHashes);
		aggstate->phases[0].numsets = 0;
		aggstate->phases[0].gset_lengths = palloc(numHashes * sizeof(int));
		aggstate->phases[0].grouped_cols = palloc(numHashes * sizeof(Bitmapset *));
	}

	phase = 0;
	for (phaseidx = 0; phaseidx <= list_length(node->chain); ++phaseidx)
	{
		Agg		   *aggnode;
		Sort	   *sortnode;

		if (phaseidx > 0)
		{
			aggnode = list_nth_node(Agg, node->chain, phaseidx - 1);
			sortnode = castNode(Sort, aggnode->plan.lefttree);
		}
		else
		{
			aggnode = node;
			sortnode = NULL;
		}

		Assert(phase <= 1 || sortnode);

		if (aggnode->aggstrategy == AGG_HASHED
			|| aggnode->aggstrategy == AGG_MIXED)
		{
			AggStatePerPhase phasedata = &aggstate->phases[0];
			AggStatePerHash perhash;
			Bitmapset  *cols = NULL;

			Assert(phase == 0);
			i = phasedata->numsets++;
			perhash = &aggstate->perhash[i];

			/* phase 0 always points to the "real" Agg in the hash case */
			phasedata->aggnode = node;
			phasedata->aggstrategy = node->aggstrategy;

			/* but the actual Agg node representing this hash is saved here */
			perhash->aggnode = aggnode;

			phasedata->gset_lengths[i] = perhash->numCols = aggnode->numCols;

			for (j = 0; j < aggnode->numCols; ++j)
				cols = bms_add_member(cols, aggnode->grpColIdx[j]);

			phasedata->grouped_cols[i] = cols;

			all_grouped_cols = bms_add_members(all_grouped_cols, cols);
			continue;
		}
		else
		{
			AggStatePerPhase phasedata = &aggstate->phases[++phase];
			int			num_sets;

			phasedata->numsets = num_sets = list_length(aggnode->groupingSets);

			if (num_sets)
			{
				phasedata->gset_lengths = palloc(num_sets * sizeof(int));
				phasedata->grouped_cols = palloc(num_sets * sizeof(Bitmapset *));

				i = 0;
				foreach(l, aggnode->groupingSets)
				{
					int			current_length = list_length(lfirst(l));
					Bitmapset  *cols = NULL;

					/* planner forces this to be correct */
					for (j = 0; j < current_length; ++j)
						cols = bms_add_member(cols, aggnode->grpColIdx[j]);

					phasedata->grouped_cols[i] = cols;
					phasedata->gset_lengths[i] = current_length;

					++i;
				}

				all_grouped_cols = bms_add_members(all_grouped_cols,
												   phasedata->grouped_cols[0]);
			}
			else
			{
				Assert(phaseidx == 0);

				phasedata->gset_lengths = NULL;
				phasedata->grouped_cols = NULL;
			}

			/*
			 * If we are grouping, precompute fmgr lookup data for inner loop.
			 */
			if (aggnode->aggstrategy == AGG_SORTED)
			{
				int			i = 0;

				Assert(aggnode->numCols > 0);

				/*
				 * Build a separate function for each subset of columns that
				 * need to be compared.
				 */
				phasedata->eqfunctions =
					(ExprState **) palloc0(aggnode->numCols * sizeof(ExprState *));

				/* for each grouping set */
				for (i = 0; i < phasedata->numsets; i++)
				{
					int			length = phasedata->gset_lengths[i];

					if (phasedata->eqfunctions[length - 1] != NULL)
						continue;

					phasedata->eqfunctions[length - 1] =
						execTuplesMatchPrepare(scanDesc,
											   length,
											   aggnode->grpColIdx,
											   aggnode->grpOperators,
											   aggnode->grpCollations,
											   (PlanState *) aggstate);
				}

				/* and for all grouped columns, unless already computed */
				if (phasedata->eqfunctions[aggnode->numCols - 1] == NULL)
				{
					phasedata->eqfunctions[aggnode->numCols - 1] =
						execTuplesMatchPrepare(scanDesc,
											   aggnode->numCols,
											   aggnode->grpColIdx,
											   aggnode->grpOperators,
											   aggnode->grpCollations,
											   (PlanState *) aggstate);
				}
			}

			phasedata->aggnode = aggnode;
			phasedata->aggstrategy = aggnode->aggstrategy;
			phasedata->sortnode = sortnode;
		}
	}

	/*
	 * Convert all_grouped_cols to a descending-order list.
	 */
	i = -1;
	while ((i = bms_next_member(all_grouped_cols, i)) >= 0)
		aggstate->all_grouped_cols = lcons_int(i, aggstate->all_grouped_cols);

	/*
	 * Set up aggregate-result storage in the output expr context, and also
	 * allocate my private per-agg working storage
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	econtext->ecxt_aggvalues = (Datum *) palloc0(sizeof(Datum) * numaggs);
	econtext->ecxt_aggnulls = (bool *) palloc0(sizeof(bool) * numaggs);

	peraggs = (AggStatePerAgg) palloc0(sizeof(AggStatePerAggData) * numaggs);
	pertransstates = (AggStatePerTrans) palloc0(sizeof(AggStatePerTransData) * numaggs);

	aggstate->peragg = peraggs;
	aggstate->pertrans = pertransstates;


	aggstate->all_pergroups =
		(AggStatePerGroup *) palloc0(sizeof(AggStatePerGroup)
									 * (numGroupingSets + numHashes));
	pergroups = aggstate->all_pergroups;

	if (node->aggstrategy != AGG_HASHED)
	{
		for (i = 0; i < numGroupingSets; i++)
		{
			pergroups[i] = (AggStatePerGroup) palloc0(sizeof(AggStatePerGroupData)
													  * numaggs);
		}

		aggstate->pergroups = pergroups;
		pergroups += numGroupingSets;
	}

	/*
	 * Hashing can only appear in the initial phase.
	 */
	if (use_hashing)
	{
		/* this is an array of pointers, not structures */
		aggstate->hash_pergroup = pergroups;

		aggstate->hashentrysize =
			hash_agg_entry_size(aggstate->numtrans) +
			node->transSpace;

		/*
		 * Initialize the thresholds at which we stop creating new hash entries
		 * and start spilling.
		 */
		if (hashagg_mem_overflow)
			aggstate->hash_mem_limit = SIZE_MAX;
		else if (work_mem * 1024L > HASH_PARTITION_MEM * 2)
			aggstate->hash_mem_limit = work_mem * 1024L - HASH_PARTITION_MEM;
		else
			aggstate->hash_mem_limit = work_mem * 1024L;

		/*
		 * Set a separate limit on the maximum number of groups to
		 * create. This is important for aggregates where the initial state
		 * size is small, but aggtransspace is large.
		 */
		if (hashagg_mem_overflow)
			aggstate->hash_ngroups_limit = LONG_MAX;
		else if (aggstate->hash_mem_limit > aggstate->hashentrysize)
			aggstate->hash_ngroups_limit =
				aggstate->hash_mem_limit / aggstate->hashentrysize;
		else
			aggstate->hash_ngroups_limit = 1;

		find_hash_columns(aggstate);
		build_hash_table(aggstate, -1, 0);
		aggstate->table_filled = false;
	}

	/*
	 * Initialize current phase-dependent values to initial phase. The initial
	 * phase is 1 (first sort pass) for all strategies that use sorting (if
	 * hashing is being done too, then phase 0 is processed last); but if only
	 * hashing is being done, then phase 0 is all there is.
	 */
	if (node->aggstrategy == AGG_HASHED)
	{
		aggstate->current_phase = 0;
		initialize_phase(aggstate, 0);
		select_current_set(aggstate, 0, true);
	}
	else
	{
		aggstate->current_phase = 1;
		initialize_phase(aggstate, 1);
		select_current_set(aggstate, 0, false);
	}

	/* -----------------
	 * Perform lookups of aggregate function info, and initialize the
	 * unchanging fields of the per-agg and per-trans data.
	 *
	 * We try to optimize by detecting duplicate aggregate functions so that
	 * their state and final values are re-used, rather than needlessly being
	 * re-calculated independently. We also detect aggregates that are not
	 * the same, but which can share the same transition state.
	 *
	 * Scenarios:
	 *
	 * 1. Identical aggregate function calls appear in the query:
	 *
	 *	  SELECT SUM(x) FROM ... HAVING SUM(x) > 0
	 *
	 *	  Since these aggregates are identical, we only need to calculate
	 *	  the value once. Both aggregates will share the same 'aggno' value.
	 *
	 * 2. Two different aggregate functions appear in the query, but the
	 *	  aggregates have the same arguments, transition functions and
	 *	  initial values (and, presumably, different final functions):
	 *
	 *	  SELECT AVG(x), STDDEV(x) FROM ...
	 *
	 *	  In this case we must create a new peragg for the varying aggregate,
	 *	  and we need to call the final functions separately, but we need
	 *	  only run the transition function once.  (This requires that the
	 *	  final functions be nondestructive of the transition state, but
	 *	  that's required anyway for other reasons.)
	 *
	 * For either of these optimizations to be valid, all aggregate properties
	 * used in the transition phase must be the same, including any modifiers
	 * such as ORDER BY, DISTINCT and FILTER, and the arguments mustn't
	 * contain any volatile functions.
	 * -----------------
	 */
	aggno = -1;
	transno = -1;
	foreach(l, aggstate->aggs)
	{
		AggrefExprState *aggrefstate = (AggrefExprState *) lfirst(l);
		Aggref	   *aggref = aggrefstate->aggref;
		AggStatePerAgg peragg;
		AggStatePerTrans pertrans;
		int			existing_aggno;
		int			existing_transno;
		List	   *same_input_transnos;
		Oid			inputTypes[FUNC_MAX_ARGS];
		int			numArguments;
		int			numDirectArgs;
		HeapTuple	aggTuple;
		Form_pg_aggregate aggform;
		AclResult	aclresult;
		Oid			transfn_oid,
					finalfn_oid;
		bool		shareable;
		Oid			serialfn_oid,
					deserialfn_oid;
		Expr	   *finalfnexpr;
		Oid			aggtranstype;
		Datum		textInitVal;
		Datum		initValue;
		bool		initValueIsNull;

		/* Planner should have assigned aggregate to correct level */
		Assert(aggref->agglevelsup == 0);
		/* ... and the split mode should match */
		Assert(aggref->aggsplit == aggstate->aggsplit);

		/* 1. Check for already processed aggs which can be re-used */
		existing_aggno = find_compatible_peragg(aggref, aggstate, aggno,
												&same_input_transnos);
		if (existing_aggno != -1)
		{
			/*
			 * Existing compatible agg found. so just point the Aggref to the
			 * same per-agg struct.
			 */
			aggrefstate->aggno = existing_aggno;
			continue;
		}

		/* Mark Aggref state node with assigned index in the result array */
		peragg = &peraggs[++aggno];
		peragg->aggref = aggref;
		aggrefstate->aggno = aggno;

		/* Fetch the pg_aggregate row */
		aggTuple = SearchSysCache1(AGGFNOID,
								   ObjectIdGetDatum(aggref->aggfnoid));
		if (!HeapTupleIsValid(aggTuple))
			elog(ERROR, "cache lookup failed for aggregate %u",
				 aggref->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

		/* Check permission to call aggregate function */
		aclresult = pg_proc_aclcheck(aggref->aggfnoid, GetUserId(),
									 ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_AGGREGATE,
						   get_func_name(aggref->aggfnoid));
		InvokeFunctionExecuteHook(aggref->aggfnoid);

		/* planner recorded transition state type in the Aggref itself */
		aggtranstype = aggref->aggtranstype;
		Assert(OidIsValid(aggtranstype));

		/*
		 * If this aggregation is performing state combines, then instead of
		 * using the transition function, we'll use the combine function
		 */
		if (DO_AGGSPLIT_COMBINE(aggstate->aggsplit))
		{
			transfn_oid = aggform->aggcombinefn;

			/* If not set then the planner messed up */
			if (!OidIsValid(transfn_oid))
				elog(ERROR, "combinefn not set for aggregate function");
		}
		else
			transfn_oid = aggform->aggtransfn;

		/* Final function only required if we're finalizing the aggregates */
		if (DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit))
			peragg->finalfn_oid = finalfn_oid = InvalidOid;
		else
			peragg->finalfn_oid = finalfn_oid = aggform->aggfinalfn;

		/*
		 * If finalfn is marked read-write, we can't share transition states;
		 * but it is okay to share states for AGGMODIFY_SHAREABLE aggs.  Also,
		 * if we're not executing the finalfn here, we can share regardless.
		 */
		shareable = (aggform->aggfinalmodify != AGGMODIFY_READ_WRITE) ||
			(finalfn_oid == InvalidOid);
		peragg->shareable = shareable;

		serialfn_oid = InvalidOid;
		deserialfn_oid = InvalidOid;

		/*
		 * Check if serialization/deserialization is required.  We only do it
		 * for aggregates that have transtype INTERNAL.
		 */
		if (aggtranstype == INTERNALOID)
		{
			/*
			 * The planner should only have generated a serialize agg node if
			 * every aggregate with an INTERNAL state has a serialization
			 * function.  Verify that.
			 */
			if (DO_AGGSPLIT_SERIALIZE(aggstate->aggsplit))
			{
				/* serialization only valid when not running finalfn */
				Assert(DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit));

				if (!OidIsValid(aggform->aggserialfn))
					elog(ERROR, "serialfunc not provided for serialization aggregation");
				serialfn_oid = aggform->aggserialfn;
			}

			/* Likewise for deserialization functions */
			if (DO_AGGSPLIT_DESERIALIZE(aggstate->aggsplit))
			{
				/* deserialization only valid when combining states */
				Assert(DO_AGGSPLIT_COMBINE(aggstate->aggsplit));

				if (!OidIsValid(aggform->aggdeserialfn))
					elog(ERROR, "deserialfunc not provided for deserialization aggregation");
				deserialfn_oid = aggform->aggdeserialfn;
			}
		}

		/* Check that aggregate owner has permission to call component fns */
		{
			HeapTuple	procTuple;
			Oid			aggOwner;

			procTuple = SearchSysCache1(PROCOID,
										ObjectIdGetDatum(aggref->aggfnoid));
			if (!HeapTupleIsValid(procTuple))
				elog(ERROR, "cache lookup failed for function %u",
					 aggref->aggfnoid);
			aggOwner = ((Form_pg_proc) GETSTRUCT(procTuple))->proowner;
			ReleaseSysCache(procTuple);

			aclresult = pg_proc_aclcheck(transfn_oid, aggOwner,
										 ACL_EXECUTE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, OBJECT_FUNCTION,
							   get_func_name(transfn_oid));
			InvokeFunctionExecuteHook(transfn_oid);
			if (OidIsValid(finalfn_oid))
			{
				aclresult = pg_proc_aclcheck(finalfn_oid, aggOwner,
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, OBJECT_FUNCTION,
								   get_func_name(finalfn_oid));
				InvokeFunctionExecuteHook(finalfn_oid);
			}
			if (OidIsValid(serialfn_oid))
			{
				aclresult = pg_proc_aclcheck(serialfn_oid, aggOwner,
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, OBJECT_FUNCTION,
								   get_func_name(serialfn_oid));
				InvokeFunctionExecuteHook(serialfn_oid);
			}
			if (OidIsValid(deserialfn_oid))
			{
				aclresult = pg_proc_aclcheck(deserialfn_oid, aggOwner,
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, OBJECT_FUNCTION,
								   get_func_name(deserialfn_oid));
				InvokeFunctionExecuteHook(deserialfn_oid);
			}
		}

		/*
		 * Get actual datatypes of the (nominal) aggregate inputs.  These
		 * could be different from the agg's declared input types, when the
		 * agg accepts ANY or a polymorphic type.
		 */
		numArguments = get_aggregate_argtypes(aggref, inputTypes);

		/* Count the "direct" arguments, if any */
		numDirectArgs = list_length(aggref->aggdirectargs);

		/* Detect how many arguments to pass to the finalfn */
		if (aggform->aggfinalextra)
			peragg->numFinalArgs = numArguments + 1;
		else
			peragg->numFinalArgs = numDirectArgs + 1;

		/* Initialize any direct-argument expressions */
		peragg->aggdirectargs = ExecInitExprList(aggref->aggdirectargs,
												 (PlanState *) aggstate);

		/*
		 * build expression trees using actual argument & result types for the
		 * finalfn, if it exists and is required.
		 */
		if (OidIsValid(finalfn_oid))
		{
			build_aggregate_finalfn_expr(inputTypes,
										 peragg->numFinalArgs,
										 aggtranstype,
										 aggref->aggtype,
										 aggref->inputcollid,
										 finalfn_oid,
										 &finalfnexpr);
			fmgr_info(finalfn_oid, &peragg->finalfn);
			fmgr_info_set_expr((Node *) finalfnexpr, &peragg->finalfn);
		}

		/* get info about the output value's datatype */
		get_typlenbyval(aggref->aggtype,
						&peragg->resulttypeLen,
						&peragg->resulttypeByVal);

		/*
		 * initval is potentially null, so don't try to access it as a struct
		 * field. Must do it the hard way with SysCacheGetAttr.
		 */
		textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
									  Anum_pg_aggregate_agginitval,
									  &initValueIsNull);
		if (initValueIsNull)
			initValue = (Datum) 0;
		else
			initValue = GetAggInitVal(textInitVal, aggtranstype);

		/*
		 * 2. Build working state for invoking the transition function, or
		 * look up previously initialized working state, if we can share it.
		 *
		 * find_compatible_peragg() already collected a list of shareable
		 * per-Trans's with the same inputs. Check if any of them have the
		 * same transition function and initial value.
		 */
		existing_transno = find_compatible_pertrans(aggstate, aggref,
													shareable,
													transfn_oid, aggtranstype,
													serialfn_oid, deserialfn_oid,
													initValue, initValueIsNull,
													same_input_transnos);
		if (existing_transno != -1)
		{
			/*
			 * Existing compatible trans found, so just point the 'peragg' to
			 * the same per-trans struct, and mark the trans state as shared.
			 */
			pertrans = &pertransstates[existing_transno];
			pertrans->aggshared = true;
			peragg->transno = existing_transno;
		}
		else
		{
			pertrans = &pertransstates[++transno];
			build_pertrans_for_aggref(pertrans, aggstate, estate,
									  aggref, transfn_oid, aggtranstype,
									  serialfn_oid, deserialfn_oid,
									  initValue, initValueIsNull,
									  inputTypes, numArguments);
			peragg->transno = transno;
		}
		ReleaseSysCache(aggTuple);
	}

	/*
	 * Update aggstate->numaggs to be the number of unique aggregates found.
	 * Also set numstates to the number of unique transition states found.
	 */
	aggstate->numaggs = aggno + 1;
	aggstate->numtrans = transno + 1;

	/*
	 * Last, check whether any more aggregates got added onto the node while
	 * we processed the expressions for the aggregate arguments (including not
	 * only the regular arguments and FILTER expressions handled immediately
	 * above, but any direct arguments we might've handled earlier).  If so,
	 * we have nested aggregate functions, which is semantically nonsensical,
	 * so complain.  (This should have been caught by the parser, so we don't
	 * need to work hard on a helpful error message; but we defend against it
	 * here anyway, just to be sure.)
	 */
	if (numaggs != list_length(aggstate->aggs))
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("aggregate function calls cannot be nested")));

	/*
	 * Build expressions doing all the transition work at once. We build a
	 * different one for each phase, as the number of transition function
	 * invocation can differ between phases. Note this'll work both for
	 * transition and combination functions (although there'll only be one
	 * phase in the latter case).
	 */
	for (phaseidx = 0; phaseidx < aggstate->numphases; phaseidx++)
	{
		AggStatePerPhase phase = &aggstate->phases[phaseidx];
		bool		dohash = false;
		bool		dosort = false;

		/* phase 0 doesn't necessarily exist */
		if (!phase->aggnode)
			continue;

		if (aggstate->aggstrategy == AGG_MIXED && phaseidx == 1)
		{
			/*
			 * Phase one, and only phase one, in a mixed agg performs both
			 * sorting and aggregation.
			 */
			dohash = true;
			dosort = true;
		}
		else if (aggstate->aggstrategy == AGG_MIXED && phaseidx == 0)
		{
			/*
			 * No need to compute a transition function for an AGG_MIXED phase
			 * 0 - the contents of the hashtables will have been computed
			 * during phase 1.
			 */
			continue;
		}
		else if (phase->aggstrategy == AGG_PLAIN ||
				 phase->aggstrategy == AGG_SORTED)
		{
			dohash = false;
			dosort = true;
		}
		else if (phase->aggstrategy == AGG_HASHED)
		{
			dohash = true;
			dosort = false;
		}
		else
			Assert(false);

		phase->evaltrans = ExecBuildAggTrans(aggstate, phase, dosort, dohash, false);

	}

	return aggstate;
}

/*
 * Build the state needed to calculate a state value for an aggregate.
 *
 * This initializes all the fields in 'pertrans'. 'aggref' is the aggregate
 * to initialize the state for. 'aggtransfn', 'aggtranstype', and the rest
 * of the arguments could be calculated from 'aggref', but the caller has
 * calculated them already, so might as well pass them.
 */
static void
build_pertrans_for_aggref(AggStatePerTrans pertrans,
						  AggState *aggstate, EState *estate,
						  Aggref *aggref,
						  Oid aggtransfn, Oid aggtranstype,
						  Oid aggserialfn, Oid aggdeserialfn,
						  Datum initValue, bool initValueIsNull,
						  Oid *inputTypes, int numArguments)
{
	int			numGroupingSets = Max(aggstate->maxsets, 1);
	Expr	   *serialfnexpr = NULL;
	Expr	   *deserialfnexpr = NULL;
	ListCell   *lc;
	int			numInputs;
	int			numDirectArgs;
	List	   *sortlist;
	int			numSortCols;
	int			numDistinctCols;
	int			i;

	/* Begin filling in the pertrans data */
	pertrans->aggref = aggref;
	pertrans->aggshared = false;
	pertrans->aggCollation = aggref->inputcollid;
	pertrans->transfn_oid = aggtransfn;
	pertrans->serialfn_oid = aggserialfn;
	pertrans->deserialfn_oid = aggdeserialfn;
	pertrans->initValue = initValue;
	pertrans->initValueIsNull = initValueIsNull;

	/* Count the "direct" arguments, if any */
	numDirectArgs = list_length(aggref->aggdirectargs);

	/* Count the number of aggregated input columns */
	pertrans->numInputs = numInputs = list_length(aggref->args);

	pertrans->aggtranstype = aggtranstype;

	/*
	 * When combining states, we have no use at all for the aggregate
	 * function's transfn. Instead we use the combinefn.  In this case, the
	 * transfn and transfn_oid fields of pertrans refer to the combine
	 * function rather than the transition function.
	 */
	if (DO_AGGSPLIT_COMBINE(aggstate->aggsplit))
	{
		Expr	   *combinefnexpr;
		size_t		numTransArgs;

		/*
		 * When combining there's only one input, the to-be-combined added
		 * transition value from below (this node's transition value is
		 * counted separately).
		 */
		pertrans->numTransInputs = 1;

		/* account for the current transition state */
		numTransArgs = pertrans->numTransInputs + 1;

		build_aggregate_combinefn_expr(aggtranstype,
									   aggref->inputcollid,
									   aggtransfn,
									   &combinefnexpr);
		fmgr_info(aggtransfn, &pertrans->transfn);
		fmgr_info_set_expr((Node *) combinefnexpr, &pertrans->transfn);

		pertrans->transfn_fcinfo =
			(FunctionCallInfo) palloc(SizeForFunctionCallInfo(2));
		InitFunctionCallInfoData(*pertrans->transfn_fcinfo,
								 &pertrans->transfn,
								 numTransArgs,
								 pertrans->aggCollation,
								 (void *) aggstate, NULL);

		/*
		 * Ensure that a combine function to combine INTERNAL states is not
		 * strict. This should have been checked during CREATE AGGREGATE, but
		 * the strict property could have been changed since then.
		 */
		if (pertrans->transfn.fn_strict && aggtranstype == INTERNALOID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("combine function with transition type %s must not be declared STRICT",
							format_type_be(aggtranstype))));
	}
	else
	{
		Expr	   *transfnexpr;
		size_t		numTransArgs;

		/* Detect how many arguments to pass to the transfn */
		if (AGGKIND_IS_ORDERED_SET(aggref->aggkind))
			pertrans->numTransInputs = numInputs;
		else
			pertrans->numTransInputs = numArguments;

		/* account for the current transition state */
		numTransArgs = pertrans->numTransInputs + 1;

		/*
		 * Set up infrastructure for calling the transfn.  Note that
		 * invtransfn is not needed here.
		 */
		build_aggregate_transfn_expr(inputTypes,
									 numArguments,
									 numDirectArgs,
									 aggref->aggvariadic,
									 aggtranstype,
									 aggref->inputcollid,
									 aggtransfn,
									 InvalidOid,
									 &transfnexpr,
									 NULL);
		fmgr_info(aggtransfn, &pertrans->transfn);
		fmgr_info_set_expr((Node *) transfnexpr, &pertrans->transfn);

		pertrans->transfn_fcinfo =
			(FunctionCallInfo) palloc(SizeForFunctionCallInfo(numTransArgs));
		InitFunctionCallInfoData(*pertrans->transfn_fcinfo,
								 &pertrans->transfn,
								 numTransArgs,
								 pertrans->aggCollation,
								 (void *) aggstate, NULL);

		/*
		 * If the transfn is strict and the initval is NULL, make sure input
		 * type and transtype are the same (or at least binary-compatible), so
		 * that it's OK to use the first aggregated input value as the initial
		 * transValue.  This should have been checked at agg definition time,
		 * but we must check again in case the transfn's strictness property
		 * has been changed.
		 */
		if (pertrans->transfn.fn_strict && pertrans->initValueIsNull)
		{
			if (numArguments <= numDirectArgs ||
				!IsBinaryCoercible(inputTypes[numDirectArgs],
								   aggtranstype))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("aggregate %u needs to have compatible input type and transition type",
								aggref->aggfnoid)));
		}
	}

	/* get info about the state value's datatype */
	get_typlenbyval(aggtranstype,
					&pertrans->transtypeLen,
					&pertrans->transtypeByVal);

	if (OidIsValid(aggserialfn))
	{
		build_aggregate_serialfn_expr(aggserialfn,
									  &serialfnexpr);
		fmgr_info(aggserialfn, &pertrans->serialfn);
		fmgr_info_set_expr((Node *) serialfnexpr, &pertrans->serialfn);

		pertrans->serialfn_fcinfo =
			(FunctionCallInfo) palloc(SizeForFunctionCallInfo(1));
		InitFunctionCallInfoData(*pertrans->serialfn_fcinfo,
								 &pertrans->serialfn,
								 1,
								 InvalidOid,
								 (void *) aggstate, NULL);
	}

	if (OidIsValid(aggdeserialfn))
	{
		build_aggregate_deserialfn_expr(aggdeserialfn,
										&deserialfnexpr);
		fmgr_info(aggdeserialfn, &pertrans->deserialfn);
		fmgr_info_set_expr((Node *) deserialfnexpr, &pertrans->deserialfn);

		pertrans->deserialfn_fcinfo =
			(FunctionCallInfo) palloc(SizeForFunctionCallInfo(2));
		InitFunctionCallInfoData(*pertrans->deserialfn_fcinfo,
								 &pertrans->deserialfn,
								 2,
								 InvalidOid,
								 (void *) aggstate, NULL);

	}

	/*
	 * If we're doing either DISTINCT or ORDER BY for a plain agg, then we
	 * have a list of SortGroupClause nodes; fish out the data in them and
	 * stick them into arrays.  We ignore ORDER BY for an ordered-set agg,
	 * however; the agg's transfn and finalfn are responsible for that.
	 *
	 * Note that by construction, if there is a DISTINCT clause then the ORDER
	 * BY clause is a prefix of it (see transformDistinctClause).
	 */
	if (AGGKIND_IS_ORDERED_SET(aggref->aggkind))
	{
		sortlist = NIL;
		numSortCols = numDistinctCols = 0;
	}
	else if (aggref->aggdistinct)
	{
		sortlist = aggref->aggdistinct;
		numSortCols = numDistinctCols = list_length(sortlist);
		Assert(numSortCols >= list_length(aggref->aggorder));
	}
	else
	{
		sortlist = aggref->aggorder;
		numSortCols = list_length(sortlist);
		numDistinctCols = 0;
	}

	pertrans->numSortCols = numSortCols;
	pertrans->numDistinctCols = numDistinctCols;

	/*
	 * If we have either sorting or filtering to do, create a tupledesc and
	 * slot corresponding to the aggregated inputs (including sort
	 * expressions) of the agg.
	 */
	if (numSortCols > 0 || aggref->aggfilter)
	{
		pertrans->sortdesc = ExecTypeFromTL(aggref->args);
		pertrans->sortslot =
			ExecInitExtraTupleSlot(estate, pertrans->sortdesc,
								   &TTSOpsMinimalTuple);
	}

	if (numSortCols > 0)
	{
		/*
		 * We don't implement DISTINCT or ORDER BY aggs in the HASHED case
		 * (yet)
		 */
		Assert(aggstate->aggstrategy != AGG_HASHED && aggstate->aggstrategy != AGG_MIXED);

		/* If we have only one input, we need its len/byval info. */
		if (numInputs == 1)
		{
			get_typlenbyval(inputTypes[numDirectArgs],
							&pertrans->inputtypeLen,
							&pertrans->inputtypeByVal);
		}
		else if (numDistinctCols > 0)
		{
			/* we will need an extra slot to store prior values */
			pertrans->uniqslot =
				ExecInitExtraTupleSlot(estate, pertrans->sortdesc,
									   &TTSOpsMinimalTuple);
		}

		/* Extract the sort information for use later */
		pertrans->sortColIdx =
			(AttrNumber *) palloc(numSortCols * sizeof(AttrNumber));
		pertrans->sortOperators =
			(Oid *) palloc(numSortCols * sizeof(Oid));
		pertrans->sortCollations =
			(Oid *) palloc(numSortCols * sizeof(Oid));
		pertrans->sortNullsFirst =
			(bool *) palloc(numSortCols * sizeof(bool));

		i = 0;
		foreach(lc, sortlist)
		{
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(sortcl, aggref->args);

			/* the parser should have made sure of this */
			Assert(OidIsValid(sortcl->sortop));

			pertrans->sortColIdx[i] = tle->resno;
			pertrans->sortOperators[i] = sortcl->sortop;
			pertrans->sortCollations[i] = exprCollation((Node *) tle->expr);
			pertrans->sortNullsFirst[i] = sortcl->nulls_first;
			i++;
		}
		Assert(i == numSortCols);
	}

	if (aggref->aggdistinct)
	{
		Oid		   *ops;

		Assert(numArguments > 0);
		Assert(list_length(aggref->aggdistinct) == numDistinctCols);

		ops = palloc(numDistinctCols * sizeof(Oid));

		i = 0;
		foreach(lc, aggref->aggdistinct)
			ops[i++] = ((SortGroupClause *) lfirst(lc))->eqop;

		/* lookup / build the necessary comparators */
		if (numDistinctCols == 1)
			fmgr_info(get_opcode(ops[0]), &pertrans->equalfnOne);
		else
			pertrans->equalfnMulti =
				execTuplesMatchPrepare(pertrans->sortdesc,
									   numDistinctCols,
									   pertrans->sortColIdx,
									   ops,
									   pertrans->sortCollations,
									   &aggstate->ss.ps);
		pfree(ops);
	}

	pertrans->sortstates = (Tuplesortstate **)
		palloc0(sizeof(Tuplesortstate *) * numGroupingSets);
}


static Datum
GetAggInitVal(Datum textInitVal, Oid transtype)
{
	Oid			typinput,
				typioparam;
	char	   *strInitVal;
	Datum		initVal;

	getTypeInputInfo(transtype, &typinput, &typioparam);
	strInitVal = TextDatumGetCString(textInitVal);
	initVal = OidInputFunctionCall(typinput, strInitVal,
								   typioparam, -1);
	pfree(strInitVal);
	return initVal;
}

/*
 * find_compatible_peragg - search for a previously initialized per-Agg struct
 *
 * Searches the previously looked at aggregates to find one which is compatible
 * with this one, with the same input parameters. If no compatible aggregate
 * can be found, returns -1.
 *
 * As a side-effect, this also collects a list of existing, shareable per-Trans
 * structs with matching inputs. If no identical Aggref is found, the list is
 * passed later to find_compatible_pertrans, to see if we can at least reuse
 * the state value of another aggregate.
 */
static int
find_compatible_peragg(Aggref *newagg, AggState *aggstate,
					   int lastaggno, List **same_input_transnos)
{
	int			aggno;
	AggStatePerAgg peraggs;

	*same_input_transnos = NIL;

	/* we mustn't reuse the aggref if it contains volatile function calls */
	if (contain_volatile_functions((Node *) newagg))
		return -1;

	peraggs = aggstate->peragg;

	/*
	 * Search through the list of already seen aggregates. If we find an
	 * existing identical aggregate call, then we can re-use that one. While
	 * searching, we'll also collect a list of Aggrefs with the same input
	 * parameters. If no matching Aggref is found, the caller can potentially
	 * still re-use the transition state of one of them.  (At this stage we
	 * just compare the parsetrees; whether different aggregates share the
	 * same transition function will be checked later.)
	 */
	for (aggno = 0; aggno <= lastaggno; aggno++)
	{
		AggStatePerAgg peragg;
		Aggref	   *existingRef;

		peragg = &peraggs[aggno];
		existingRef = peragg->aggref;

		/* all of the following must be the same or it's no match */
		if (newagg->inputcollid != existingRef->inputcollid ||
			newagg->aggtranstype != existingRef->aggtranstype ||
			newagg->aggstar != existingRef->aggstar ||
			newagg->aggvariadic != existingRef->aggvariadic ||
			newagg->aggkind != existingRef->aggkind ||
			!equal(newagg->args, existingRef->args) ||
			!equal(newagg->aggorder, existingRef->aggorder) ||
			!equal(newagg->aggdistinct, existingRef->aggdistinct) ||
			!equal(newagg->aggfilter, existingRef->aggfilter))
			continue;

		/* if it's the same aggregate function then report exact match */
		if (newagg->aggfnoid == existingRef->aggfnoid &&
			newagg->aggtype == existingRef->aggtype &&
			newagg->aggcollid == existingRef->aggcollid &&
			equal(newagg->aggdirectargs, existingRef->aggdirectargs))
		{
			list_free(*same_input_transnos);
			*same_input_transnos = NIL;
			return aggno;
		}

		/*
		 * Not identical, but it had the same inputs.  If the final function
		 * permits sharing, return its transno to the caller, in case we can
		 * re-use its per-trans state.  (If there's already sharing going on,
		 * we might report a transno more than once.  find_compatible_pertrans
		 * is cheap enough that it's not worth spending cycles to avoid that.)
		 */
		if (peragg->shareable)
			*same_input_transnos = lappend_int(*same_input_transnos,
											   peragg->transno);
	}

	return -1;
}

/*
 * find_compatible_pertrans - search for a previously initialized per-Trans
 * struct
 *
 * Searches the list of transnos for a per-Trans struct with the same
 * transition function and initial condition. (The inputs have already been
 * verified to match.)
 */
static int
find_compatible_pertrans(AggState *aggstate, Aggref *newagg, bool shareable,
						 Oid aggtransfn, Oid aggtranstype,
						 Oid aggserialfn, Oid aggdeserialfn,
						 Datum initValue, bool initValueIsNull,
						 List *transnos)
{
	ListCell   *lc;

	/* If this aggregate can't share transition states, give up */
	if (!shareable)
		return -1;

	foreach(lc, transnos)
	{
		int			transno = lfirst_int(lc);
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];

		/*
		 * if the transfns or transition state types are not the same then the
		 * state can't be shared.
		 */
		if (aggtransfn != pertrans->transfn_oid ||
			aggtranstype != pertrans->aggtranstype)
			continue;

		/*
		 * The serialization and deserialization functions must match, if
		 * present, as we're unable to share the trans state for aggregates
		 * which will serialize or deserialize into different formats.
		 * Remember that these will be InvalidOid if they're not required for
		 * this agg node.
		 */
		if (aggserialfn != pertrans->serialfn_oid ||
			aggdeserialfn != pertrans->deserialfn_oid)
			continue;

		/*
		 * Check that the initial condition matches, too.
		 */
		if (initValueIsNull && pertrans->initValueIsNull)
			return transno;

		if (!initValueIsNull && !pertrans->initValueIsNull &&
			datumIsEqual(initValue, pertrans->initValue,
						 pertrans->transtypeByVal, pertrans->transtypeLen))
			return transno;
	}
	return -1;
}

void
ExecEndAgg(AggState *node)
{
	PlanState  *outerPlan;
	int			transno;
	int			numGroupingSets = Max(node->maxsets, 1);
	int			setno;

	/* Make sure we have closed any open tuplesorts */

	if (node->sort_in)
		tuplesort_end(node->sort_in);
	if (node->sort_out)
		tuplesort_end(node->sort_out);

	hash_reset_spills(node);

	for (transno = 0; transno < node->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &node->pertrans[transno];

		for (setno = 0; setno < numGroupingSets; setno++)
		{
			if (pertrans->sortstates[setno])
				tuplesort_end(pertrans->sortstates[setno]);
		}
	}

	/* And ensure any agg shutdown callbacks have been called */
	for (setno = 0; setno < numGroupingSets; setno++)
		ReScanExprContext(node->aggcontexts[setno]);
	if (node->hashcontext)
		ReScanExprContext(node->hashcontext);

	/*
	 * We don't actually free any ExprContexts here (see comment in
	 * ExecFreeExprContext), just unlinking the output one from the plan node
	 * suffices.
	 */
	ExecFreeExprContext(&node->ss.ps);

	/* clean up tuple table */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);
}

void
ExecReScanAgg(AggState *node)
{
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	PlanState  *outerPlan = outerPlanState(node);
	Agg		   *aggnode = (Agg *) node->ss.ps.plan;
	int			transno;
	int			numGroupingSets = Max(node->maxsets, 1);
	int			setno;

	node->agg_done = false;

	if (node->aggstrategy == AGG_HASHED)
	{
		/*
		 * In the hashed case, if we haven't yet built the hash table then we
		 * can just return; nothing done yet, so nothing to undo. If subnode's
		 * chgParam is not NULL then it will be re-scanned by ExecProcNode,
		 * else no reason to re-scan it at all.
		 */
		if (!node->table_filled)
			return;

		/*
		 * If we do have the hash table, and it never spilled, and the subplan
		 * does not have any parameter changes, and none of our own parameter
		 * changes affect input expressions of the aggregated functions, then
		 * we can just rescan the existing hash table; no need to build it
		 * again.
		 */
		if (outerPlan->chgParam == NULL && !node->hash_spilled &&
			!bms_overlap(node->ss.ps.chgParam, aggnode->aggParams))
		{
			ResetTupleHashIterator(node->perhash[0].hashtable,
								   &node->perhash[0].hashiter);
			select_current_set(node, 0, true);
			return;
		}
	}

	/* Make sure we have closed any open tuplesorts */
	for (transno = 0; transno < node->numtrans; transno++)
	{
		for (setno = 0; setno < numGroupingSets; setno++)
		{
			AggStatePerTrans pertrans = &node->pertrans[transno];

			if (pertrans->sortstates[setno])
			{
				tuplesort_end(pertrans->sortstates[setno]);
				pertrans->sortstates[setno] = NULL;
			}
		}
	}

	/*
	 * We don't need to ReScanExprContext the output tuple context here;
	 * ExecReScan already did it. But we do need to reset our per-grouping-set
	 * contexts, which may have transvalues stored in them. (We use rescan
	 * rather than just reset because transfns may have registered callbacks
	 * that need to be run now.) For the AGG_HASHED case, see below.
	 */

	for (setno = 0; setno < numGroupingSets; setno++)
	{
		ReScanExprContext(node->aggcontexts[setno]);
	}

	/* Release first tuple of group, if we have made a copy */
	if (node->grp_firstTuple != NULL)
	{
		heap_freetuple(node->grp_firstTuple);
		node->grp_firstTuple = NULL;
	}
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/* Forget current agg values */
	MemSet(econtext->ecxt_aggvalues, 0, sizeof(Datum) * node->numaggs);
	MemSet(econtext->ecxt_aggnulls, 0, sizeof(bool) * node->numaggs);

	/*
	 * With AGG_HASHED/MIXED, the hash table is allocated in a sub-context of
	 * the hashcontext. This used to be an issue, but now, resetting a context
	 * automatically deletes sub-contexts too.
	 */
	if (node->aggstrategy == AGG_HASHED || node->aggstrategy == AGG_MIXED)
	{
		hash_reset_spills(node);

		node->hash_spilled = false;
		node->hash_no_new_groups = false;
		node->hash_mem_current = 0;
		node->hash_ngroups_current = 0;

		/* reset stats */
		node->hash_mem_peak = 0;
		node->hash_disk_used = 0;
		node->hash_batches_used = 0;

		ReScanExprContext(node->hashcontext);
		/* Rebuild an empty hash table */
		build_hash_table(node, -1, 0);
		node->table_filled = false;
		/* iterator will be reset when the table is filled */
	}

	if (node->aggstrategy != AGG_HASHED)
	{
		/*
		 * Reset the per-group state (in particular, mark transvalues null)
		 */
		for (setno = 0; setno < numGroupingSets; setno++)
		{
			MemSet(node->pergroups[setno], 0,
				   sizeof(AggStatePerGroupData) * node->numaggs);
		}

		/* reset to phase 1 */
		initialize_phase(node, 1);

		node->input_done = false;
		node->projected_set = -1;
	}

	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
}


/***********************************************************************
 * API exposed to aggregate functions
 ***********************************************************************/


/*
 * AggCheckCallContext - test if a SQL function is being called as an aggregate
 *
 * The transition and/or final functions of an aggregate may want to verify
 * that they are being called as aggregates, rather than as plain SQL
 * functions.  They should use this function to do so.  The return value
 * is nonzero if being called as an aggregate, or zero if not.  (Specific
 * nonzero values are AGG_CONTEXT_AGGREGATE or AGG_CONTEXT_WINDOW, but more
 * values could conceivably appear in future.)
 *
 * If aggcontext isn't NULL, the function also stores at *aggcontext the
 * identity of the memory context that aggregate transition values are being
 * stored in.  Note that the same aggregate call site (flinfo) may be called
 * interleaved on different transition values in different contexts, so it's
 * not kosher to cache aggcontext under fn_extra.  It is, however, kosher to
 * cache it in the transvalue itself (for internal-type transvalues).
 */
int
AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext *aggcontext)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		if (aggcontext)
		{
			AggState   *aggstate = ((AggState *) fcinfo->context);
			ExprContext *cxt = aggstate->curaggcontext;

			*aggcontext = cxt->ecxt_per_tuple_memory;
		}
		return AGG_CONTEXT_AGGREGATE;
	}
	if (fcinfo->context && IsA(fcinfo->context, WindowAggState))
	{
		if (aggcontext)
			*aggcontext = ((WindowAggState *) fcinfo->context)->curaggcontext;
		return AGG_CONTEXT_WINDOW;
	}

	/* this is just to prevent "uninitialized variable" warnings */
	if (aggcontext)
		*aggcontext = NULL;
	return 0;
}

/*
 * AggGetAggref - allow an aggregate support function to get its Aggref
 *
 * If the function is being called as an aggregate support function,
 * return the Aggref node for the aggregate call.  Otherwise, return NULL.
 *
 * Aggregates sharing the same inputs and transition functions can get
 * merged into a single transition calculation.  If the transition function
 * calls AggGetAggref, it will get some one of the Aggrefs for which it is
 * executing.  It must therefore not pay attention to the Aggref fields that
 * relate to the final function, as those are indeterminate.  But if a final
 * function calls AggGetAggref, it will get a precise result.
 *
 * Note that if an aggregate is being used as a window function, this will
 * return NULL.  We could provide a similar function to return the relevant
 * WindowFunc node in such cases, but it's not needed yet.
 */
Aggref *
AggGetAggref(FunctionCallInfo fcinfo)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggState   *aggstate = (AggState *) fcinfo->context;
		AggStatePerAgg curperagg;
		AggStatePerTrans curpertrans;

		/* check curperagg (valid when in a final function) */
		curperagg = aggstate->curperagg;

		if (curperagg)
			return curperagg->aggref;

		/* check curpertrans (valid when in a transition function) */
		curpertrans = aggstate->curpertrans;

		if (curpertrans)
			return curpertrans->aggref;
	}
	return NULL;
}

/*
 * AggGetTempMemoryContext - fetch short-term memory context for aggregates
 *
 * This is useful in agg final functions; the context returned is one that
 * the final function can safely reset as desired.  This isn't useful for
 * transition functions, since the context returned MAY (we don't promise)
 * be the same as the context those are called in.
 *
 * As above, this is currently not useful for aggs called as window functions.
 */
MemoryContext
AggGetTempMemoryContext(FunctionCallInfo fcinfo)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggState   *aggstate = (AggState *) fcinfo->context;

		return aggstate->tmpcontext->ecxt_per_tuple_memory;
	}
	return NULL;
}

/*
 * AggStateIsShared - find out whether transition state is shared
 *
 * If the function is being called as an aggregate support function,
 * return true if the aggregate's transition state is shared across
 * multiple aggregates, false if it is not.
 *
 * Returns true if not called as an aggregate support function.
 * This is intended as a conservative answer, ie "no you'd better not
 * scribble on your input".  In particular, will return true if the
 * aggregate is being used as a window function, which is a scenario
 * in which changing the transition state is a bad idea.  We might
 * want to refine the behavior for the window case in future.
 */
bool
AggStateIsShared(FunctionCallInfo fcinfo)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggState   *aggstate = (AggState *) fcinfo->context;
		AggStatePerAgg curperagg;
		AggStatePerTrans curpertrans;

		/* check curperagg (valid when in a final function) */
		curperagg = aggstate->curperagg;

		if (curperagg)
			return aggstate->pertrans[curperagg->transno].aggshared;

		/* check curpertrans (valid when in a transition function) */
		curpertrans = aggstate->curpertrans;

		if (curpertrans)
			return curpertrans->aggshared;
	}
	return true;
}

/*
 * AggRegisterCallback - register a cleanup callback for an aggregate
 *
 * This is useful for aggs to register shutdown callbacks, which will ensure
 * that non-memory resources are freed.  The callback will occur just before
 * the associated aggcontext (as returned by AggCheckCallContext) is reset,
 * either between groups or as a result of rescanning the query.  The callback
 * will NOT be called on error paths.  The typical use-case is for freeing of
 * tuplestores or tuplesorts maintained in aggcontext, or pins held by slots
 * created by the agg functions.  (The callback will not be called until after
 * the result of the finalfn is no longer needed, so it's safe for the finalfn
 * to return data that will be freed by the callback.)
 *
 * As above, this is currently not useful for aggs called as window functions.
 */
void
AggRegisterCallback(FunctionCallInfo fcinfo,
					ExprContextCallbackFunction func,
					Datum arg)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggState   *aggstate = (AggState *) fcinfo->context;
		ExprContext *cxt = aggstate->curaggcontext;

		RegisterExprContextCallback(cxt, func, arg);

		return;
	}
	elog(ERROR, "aggregate function cannot register a callback in this context");
}


/*
 * aggregate_dummy - dummy execution routine for aggregate functions
 *
 * This function is listed as the implementation (prosrc field) of pg_proc
 * entries for aggregate functions.  Its only purpose is to throw an error
 * if someone mistakenly executes such a function in the normal way.
 *
 * Perhaps someday we could assign real meaning to the prosrc field of
 * an aggregate?
 */
Datum
aggregate_dummy(PG_FUNCTION_ARGS)
{
	elog(ERROR, "aggregate function %u called as normal function",
		 fcinfo->flinfo->fn_oid);
	return (Datum) 0;			/* keep compiler quiet */
}
