import { open } from '..';
import { tableFromIPC } from 'apache-arrow';
const vl = require('vega-lite');
const vega = require('vega');
const db = open(':memory:');
const con = db.connect();
con.query(`INSTALL arrow;`);
con.query(`LOAD arrow;`); 
const ipc = con.query(`
  SELECT * FROM to_arrow_ipc((
  SELECT 'a' as id,42 as value
  UNION
  SELECT 'b', 35
))`);
const data = tableFromIPC(ipc.map(d => d.ipc))
const vlSpec = {
  "data": {"values": data},
  "mark": "bar",
  "encoding": {
    "x": {"field": "id", "type": "ordinal"},
    "y": {"field": "value", "type": "quantitative"}
  }
}
const view = new vega.View(vega.parse(vl.compile(vlSpec).spec),{renderer: 'none'});
view.toSVG().then(async function (svg) {
  await Bun.write("tests/vega_lite_test.svg", svg);
  console.log("vega_lite_test.svg written");
}).catch(function(err) {
  console.error(err);
});
con.close();
db.close();