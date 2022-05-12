<h1 align=center>@evan/duckdb</h1>
<div align=center>lightning fast <a href=https://duckdb.org>duckdb</a> bindings for bun runtime</div>

<br />

### Install
`bun add @evan/duckdb`


## Features
- 🔋 batteries included
- 🚀 jit optimized bindings
- 🐇 2-6x faster than node & deno

<br />

## Examples

```js
import { open } from '@evan/duckdb';

const db = open('./example.db');
// or const db = open(':memory:');

const connection = db.connect();

connection.query('select 1 as number') // -> [{ number: 1 }]

for (const row of connection.stream('select 42 as number')) {
  row // -> { number: 42 }
}

const prepared = connection.prepare('select ?::INTEGER as number, ?::VARCHAR as text');

prepared.query(1337, 'foo'); // -> [{ number: 1337, text: 'foo' }]
prepared.query(null, 'bar'); // -> [{ number: null, text: 'bar' }]

connection.close();
db.close();
```

## License

Apache-2.0 © [Evan](https://github.com/evanwashere)