import { open } from '..';
import { tableFromIPC } from 'apache-arrow';
const db = open(':memory:');
const con = db.connect();
con.query(`INSTALL arrow;`);
con.query(`LOAD arrow;`); 
const ipc = con.query(`
SELECT * FROM to_arrow_ipc((
SELECT 
  42 as value
))`);
const data = tableFromIPC(ipc.map(d => d.ipc));
console.log(data.toString());
con.close();
db.close();
