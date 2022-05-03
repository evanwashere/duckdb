import postgres from 'postgres';
import better from 'better-sqlite3';

const db = better('/tmp/test-lite.db');
const sql = postgres({ database: 'postgres' });

const q = 'select 1 as number';

const p = db.prepare(q);

console.log(p.all());
console.log(await sql(Object.assign([q], { raw: [q] })));

sql.end();