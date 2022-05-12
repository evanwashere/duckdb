import duckdb from 'duckdb';
import postgres from 'postgres';
import { promisify } from 'util';
import better from 'better-sqlite3';
import { run, bench } from 'mitata';

const db = new duckdb.Database('/tmp/test.db');
const post = postgres({ database: 'postgres' });

const q = 'select i, i as a from generate_series(1, 100000) s(i)';

const qlite = `
WITH RECURSIVE c(x) AS (
  VALUES(1)
  UNION ALL
  SELECT x+1 FROM c WHERE x<100000
)
SELECT x, x as a FROM c;
`;

const p = db.prepare(q);

console.log('benchmarking query: ' + q + '\nnote: sqlite uses recursive alternative');

bench('duckdb', async () => {
  await new Promise(r => p.all(r));
});

bench('postgres', async () => {
  await post`select i, i as a from generate_series(1, 100000) s(i)`;
});

const dblite = better('/tmp/test-sqlite.db');

const pp = dblite.prepare(qlite);

bench('better-sqlite3', () => {
  pp.all();
});

await run({ percentiles: false });

post.end();