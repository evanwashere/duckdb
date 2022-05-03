import { open, close, query, stream, prepare, connect, disconnect } from '../lib.mjs';

const db = open('/tmp/test.db');

const connection = connect(db);
const p = prepare(connection, 'select 1 as number');

console.log(p.query());