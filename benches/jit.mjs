import { run, bench, group } from 'mitata';
import { open, close, query, stream, prepare, connect, disconnect } from '../lib.mjs';

const db = open(':memory:');
const connection = connect(db);
const q = 'select i, i as a from generate_series(1, 100000) s(i)';

const p = prepare(connection, q);
console.log('benchmarking query: ' + q);

group('query', () => {
  bench('query()', () => {
    query(connection, q);
  });

  bench('jit query()', () => {
    p.query();
  });
});

group('stream', () => {
  bench('stream()', () => {
    for (const x of stream(connection, q)) x;
  });

  bench('jit stream()', () => {
    for (const x of p.stream()) x;
  });
});

await run({ percentiles: false });
disconnect(connection); close(db);