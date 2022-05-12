import { run, bench } from 'mitata';
import { open, close, query, stream, prepare, connect, disconnect } from '../lib.mjs';

const db = open('/tmp/test.db');


const connection = connect(db);
const q = 'select i, i as a from generate_series(1, 100000) s(i)';

const p = prepare(connection, q);
console.log('benchmarking query: ' + q);

bench('duckdb', () => {
  p.query();
});

await run({ percentiles: false });
disconnect(connection); close(db);