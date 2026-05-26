"use strict";

const { ensureNativeInstalled, loadNative } = require("./scripts/native");

class Database {
  constructor(target = ":memory:", nativeHandle = null) {
    const NativeDatabase = loadNative().Database;
    this._native = nativeHandle || new NativeDatabase(target);
  }

  static async open(target = ":memory:") {
    await ensureNativeInstalled();
    return new Database(target);
  }

  execute(sql, ...params) {
    const statement = params.length === 0 ? String(sql) : bindSql(String(sql), normalizeParams(params));
    return normalizeResult(this._native.execute(statement));
  }

  run(sql, ...params) {
    return this.execute(sql, ...params);
  }

  all(sql, ...params) {
    const result = this.execute(sql, ...params);
    return result.rows.map((row) => rowToObject(result.columns, row));
  }

  get(sql, ...params) {
    return this.all(sql, ...params)[0] || null;
  }

  each(sql, ...args) {
    const callback = args.pop();
    if (typeof callback !== "function") {
      throw new TypeError("Database#each requires a callback");
    }
    const rows = this.all(sql, ...args);
    for (const row of rows) {
      callback(null, row);
    }
    return rows.length;
  }

  prepare(sql) {
    return new Statement(this, String(sql));
  }

  close() {
    this._native = null;
  }
}

class Statement {
  constructor(database, sql) {
    this.database = database;
    this.sql = sql;
    this.closed = false;
  }

  run(...params) {
    return this.database.run(this.boundSql(params));
  }

  all(...params) {
    return this.database.all(this.boundSql(params));
  }

  get(...params) {
    return this.database.get(this.boundSql(params));
  }

  each(...args) {
    const callback = args.pop();
    return this.database.each(this.boundSql(args), callback);
  }

  finalize() {
    this.closed = true;
  }

  boundSql(params) {
    if (this.closed) {
      throw new Error("statement is finalized");
    }
    return bindSql(this.sql, normalizeParams(params));
  }
}

function normalizeResult(result) {
  const columns = (result.columns || []).map((column) => ({
    name: column.name,
    typeOid: column.typeOid ?? column.type_oid,
  }));
  return {
    columns,
    rows: result.rows || [],
    commandTag: result.commandTag ?? result.command_tag ?? "",
  };
}

function rowToObject(columns, row) {
  const object = {};
  for (let index = 0; index < columns.length; index += 1) {
    object[columns[index].name] = row[index] ?? null;
  }
  return object;
}

function normalizeParams(args) {
  if (args.length === 1 && Array.isArray(args[0])) {
    return args[0];
  }
  return args;
}

function bindSql(sql, values) {
  let valueIndex = 0;
  let out = "";
  let state = "normal";
  let dollarTag = "";

  for (let index = 0; index < sql.length; index += 1) {
    const ch = sql[index];
    const next = sql[index + 1];

    if (state === "normal") {
      const tag = dollarQuoteTagAt(sql, index);
      if (tag) {
        dollarTag = tag;
        state = "dollar";
        out += tag;
        index += tag.length - 1;
      } else if (ch === "'") {
        state = "single";
        out += ch;
      } else if (ch === '"') {
        state = "identifier";
        out += ch;
      } else if (ch === "-" && next === "-") {
        state = "line-comment";
        out += ch;
      } else if (ch === "/" && next === "*") {
        state = "block-comment";
        out += ch;
      } else if (ch === "?") {
        if (valueIndex >= values.length) {
          throw new Error("not enough bound values for statement");
        }
        out += sqlLiteral(values[valueIndex]);
        valueIndex += 1;
      } else {
        out += ch;
      }
      continue;
    }

    out += ch;
    if (state === "single") {
      if (ch === "'" && next === "'") {
        out += next;
        index += 1;
      } else if (ch === "'") {
        state = "normal";
      }
    } else if (state === "identifier") {
      if (ch === '"' && next === '"') {
        out += next;
        index += 1;
      } else if (ch === '"') {
        state = "normal";
      }
    } else if (state === "line-comment" && ch === "\n") {
      state = "normal";
    } else if (state === "block-comment" && ch === "*" && next === "/") {
      out += next;
      index += 1;
      state = "normal";
    } else if (state === "dollar" && sql.startsWith(dollarTag, index)) {
      out += dollarTag.slice(1);
      index += dollarTag.length - 1;
      state = "normal";
    }
  }

  if (valueIndex !== values.length) {
    throw new Error("too many bound values for statement");
  }
  return out;
}

function dollarQuoteTagAt(sql, index) {
  const match = /^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/.exec(sql.slice(index));
  return match ? match[0] : null;
}

function sqlLiteral(value) {
  if (value === null || value === undefined) {
    return "NULL";
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError("cannot bind non-finite number");
    }
    return String(value);
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "boolean") {
    return value ? "TRUE" : "FALSE";
  }
  if (value instanceof Date) {
    return sqlLiteral(value.toISOString());
  }
  return `'${String(value).replaceAll("'", "''")}'`;
}

module.exports = {
  Database,
  Statement,
  bindSql,
};
