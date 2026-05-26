"use strict";

class Database {
  constructor(target = ":memory:") {
    this.target = target;
    this.statements = [];
  }

  execute(sql) {
    this.statements.push(sql);
    if (/^SELECT/i.test(sql)) {
      return {
        columns: [
          { name: "id", typeOid: 23 },
          { name: "name", typeOid: 25 },
        ],
        rows: [
          ["1", "Ada"],
          ["2", "Grace"],
        ],
        commandTag: "SELECT 2",
      };
    }
    return {
      columns: [],
      rows: [],
      commandTag: /^INSERT/i.test(sql) ? "INSERT 0 1" : "CREATE TABLE",
    };
  }
}

module.exports = { Database };
