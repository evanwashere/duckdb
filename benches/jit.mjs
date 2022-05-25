import { open } from '..';
import { run, bench, group } from 'mitata';

const db = open(':memory:');
const connection = db.connect(db);
const q = 'select i, i as a from generate_series(1, 100000) s(i)';

const p = connection.prepare(q);
console.log('benchmarking query: ' + q);

group('query', () => {
  bench('jit query()', () => p.query());
  bench('query()', () => connection.query(q));
});

group('stream', () => {
  bench('jit stream()', () => {
    for (const x of p.stream()) x;
  });

  bench('stream()', () => {
    for (const x of connection.stream(q)) x;
  });
});

await run({ percentiles: false });

connection.close();
db.close();