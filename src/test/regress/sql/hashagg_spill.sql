create schema hashagg_spill;
set search_path to hashagg_spill;

-- start_ignore
CREATE EXTENSION plpythonu;
-- end_ignore

-- Create a function which checks how many batches there are
-- output example: "Memory Usage: 4096kB  Batches: 68  Disk Usage:8625kB"
create or replace function hashagg_spill.num_batches(explain_query text)
returns setof int as
$$
import re
rv = plpy.execute(explain_query)
search_text = 'batches'
result = []
for i in range(len(rv)):
    cur_line = rv[i]['QUERY PLAN']
    if search_text.lower() in cur_line.lower():
        p = re.compile('.*Memory Usage: (\d+).* Batches: (\d+)  Disk.*')
        m = p.match(cur_line)
        batches = int(m.group(2))
        result.append(batches)
return result
$$
language plpythonu;

create table testhagg (i1 int, i2 int, i3 int, i4 int);
insert into testhagg select i,i,i,i from generate_series(1, 30000)i;

set work_mem="1800";

select * from (select max(i1) from testhagg group by i2) foo order by 1 limit 10;
select num_batches > 0 from hashagg_spill.num_batches('explain (analyze, verbose) select max(i1) from testhagg group by i2;') num_batches;
select num_batches > 3 from hashagg_spill.num_batches('explain (analyze, verbose) select max(i1) from testhagg group by i2 limit 90000;') num_batches;

reset all;
set search_path to hashagg_spill;

-- Test agg spilling scenarios
create table aggspill (i int, j int, t text);
insert into aggspill select i, i*2, i::text from generate_series(1, 10000) i;
insert into aggspill select i, i*2, i::text from generate_series(1, 100000) i;
insert into aggspill select i, i*2, i::text from generate_series(1, 1000000) i;

-- No spill with large statement memory
set work_mem = '125MB';
select * from hashagg_spill.num_batches('explain (analyze, verbose)
select count(*) from (select i, count(*) from aggspill group by i,j having count(*) = 1) g;');

-- Reduce the statement memory to induce spilling
set work_mem = '10MB';
select num_batches > 32 from hashagg_spill.num_batches('explain (analyze, verbose)
select count(*) from (select i, count(*) from aggspill group by i,j having count(*) = 2) g;') num_batches;

-- Reduce the statement memory, more batches
set work_mem = '5MB';

select num_batches > 64 from hashagg_spill.num_batches('explain (analyze, verbose)
select count(*) from (select i, count(*) from aggspill group by i,j having count(*) = 3) g;') num_batches;

-- Check spilling to a temp tablespace
SET work_mem='1000kB';

CREATE TABLE hashagg_spill(col1 numeric, col2 int);
INSERT INTO hashagg_spill SELECT id, 1 FROM generate_series(1,20000) id;
ANALYZE hashagg_spill;

CREATE TABLE spill_temptblspace (a numeric);
SET temp_tablespaces=pg_default;
INSERT INTO spill_temptblspace SELECT avg(col2) col2 FROM hashagg_spill GROUP BY col1 HAVING(sum(col1)) < 0;
RESET temp_tablespaces;
RESET work_mem;

drop schema hashagg_spill cascade;
