import { run, bench } from 'https://esm.sh/mitata';
import { DB } from "https://deno.land/x/sqlite/mod.ts";

const db = new DB('/tmp/test-lite.db');

const p = db.prepareQuery(`
WITH RECURSIVE c(x) AS (
  VALUES(1)
  UNION ALL
  SELECT x+1 FROM c WHERE x<100000
)
SELECT x, x as a FROM c;
`);

bench('sqlite', () => {
  p.all();
});

run({ percentiles: false });