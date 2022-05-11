import { open, close, query, stream, prepare, connect, disconnect } from '../lib.mjs';

const db = open(':memory:');
const connection = connect(db);

query(connection, `
  install 'fts'; load 'fts';
`);