export interface QueryColumn {
  name: string;
  typeOid: number;
}

export interface QueryResult {
  columns: QueryColumn[];
  rows: Array<Array<string | null>>;
  commandTag: string;
}

export type BindValue = string | number | bigint | boolean | Date | null | undefined;

export class Statement {
  run(params: BindValue[]): QueryResult;
  run(...params: BindValue[]): QueryResult;
  all(params: BindValue[]): Record<string, string | null>[];
  all(...params: BindValue[]): Record<string, string | null>[];
  get(params: BindValue[]): Record<string, string | null> | null;
  get(...params: BindValue[]): Record<string, string | null> | null;
  each(
    ...args: [...BindValue[], (err: Error | null, row: Record<string, string | null>) => void]
  ): number;
  finalize(): void;
}

export class Database {
  constructor(target?: string);
  static open(target?: string): Promise<Database>;
  execute(sql: string, params: BindValue[]): QueryResult;
  execute(sql: string, ...params: BindValue[]): QueryResult;
  run(sql: string, params: BindValue[]): QueryResult;
  run(sql: string, ...params: BindValue[]): QueryResult;
  all(sql: string, params: BindValue[]): Record<string, string | null>[];
  all(sql: string, ...params: BindValue[]): Record<string, string | null>[];
  get(sql: string, params: BindValue[]): Record<string, string | null> | null;
  get(sql: string, ...params: BindValue[]): Record<string, string | null> | null;
  each(
    sql: string,
    ...args: [...BindValue[], (err: Error | null, row: Record<string, string | null>) => void]
  ): number;
  prepare(sql: string): Statement;
  close(): void;
}

export function bindSql(sql: string, values: BindValue[]): string;
