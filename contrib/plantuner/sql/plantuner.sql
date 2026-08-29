LOAD 'plantuner';

SHOW	plantuner.disable_index;

CREATE TABLE wow (i int, j int);
CREATE INDEX i_idx ON wow (i);
CREATE INDEX j_idx ON wow (j);
CREATE INDEX i1 ON WOW (i);
CREATE INDEX i2 ON WOW (i);
CREATE INDEX i3 ON WOW (i);

SET enable_seqscan=off;

SELECT * FROM wow;

SET plantuner.disable_index="i_idx, j_idx";

SELECT * FROM wow;

SHOW plantuner.disable_index;

SET plantuner.disable_index="i_idx, nonexistent, public.j_idx, wow";

SHOW plantuner.disable_index;

SET plantuner.enable_index="i_idx";

SHOW plantuner.enable_index;

SELECT * FROM wow;
--test only index
RESET plantuner.disable_index;
RESET plantuner.enable_index;

SET enable_seqscan=off;
SET enable_bitmapscan=off;
SET enable_indexonlyscan=off;

SET plantuner.only_index="i1";
SHOW plantuner.only_index;

EXPLAIN (COSTS OFF) SELECT * FROM wow WHERE i = 0;

SET plantuner.disable_index="i1,i2,i3";
EXPLAIN (COSTS OFF) SELECT * FROM wow WHERE i = 0;

SET plantuner.only_index="i2";
EXPLAIN (COSTS OFF) SELECT * FROM wow WHERE i = 0;

RESET plantuner.only_index;
EXPLAIN (COSTS OFF) SELECT * FROM wow WHERE i = 0;
