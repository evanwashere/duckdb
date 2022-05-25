import * as lib from './lib.mjs';

export function open(path) {
  return new Database(lib.open(path));
}

const db_gc = new FinalizationRegistry(ptr => lib.close(ptr));
const cc_gc = new FinalizationRegistry(ptr => lib.disconnect(ptr));

class Database {
  #ptr;

  constructor(ptr) {
    this.#ptr = ptr;
    db_gc.register(this, ptr, this);
  }

  close() { lib.close(this.#ptr); db_gc.unregister(this); }
  connect() { return new Connection(this, lib.connect(this.#ptr)); }
}

class Connection {
  #db;
  #ptr;

  constructor(db, ptr) {
    this.#db = db;
    this.#ptr = ptr;
    cc_gc.register(this, ptr, this);
  }

  query(sql) { return lib.query(this.#ptr, sql); }
  stream(sql) { return lib.stream(this.#ptr, sql); }
  close() { lib.disconnect(this.#ptr); cc_gc.unregister(this); }
  prepare(sql) { const p = lib.prepare(this.#ptr, sql); p.c = this; return p; }
}