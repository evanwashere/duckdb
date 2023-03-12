import { open } from '..';
import { run, bench } from 'mitata';

const db = open('/tmp/test.db');
const connection = db.connect();
connection.query(`INSTALL arrow`)
connection.query(`LOAD arrow`)
const q = 'SELECT * FROM to_arrow_ipc((select i, i as a from generate_series(1, 100000) s(i)))';

const p = connection.prepare(q);
console.log('benchmarking query: ' + q);

bench('duckdb', () => {
  p.query().map(d=>d.ipc);
});

await run({ percentiles: false });

connection.close();
db.close();