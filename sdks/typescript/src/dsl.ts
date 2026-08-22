export type JsonPrimitive = null | boolean | number | string | bigint;
export type JsonValue = unknown;

type Encodable = { toJSON(): JsonValue };

function hasToJSON(value: unknown): value is Encodable {
  return typeof value === "object" && value !== null && "toJSON" in value && typeof (value as { toJSON: unknown }).toJSON === "function";
}

function encode(value: unknown): JsonValue {
  if (hasToJSON(value)) return encode(value.toJSON());
  if (Array.isArray(value)) return value.map((entry) => encode(entry));
  if (value === undefined) return undefined as unknown as JsonValue;
  if (value === null || typeof value === "boolean" || typeof value === "string" || typeof value === "number" || typeof value === "bigint")
    return value;
  if (typeof value === "object") {
    const out: { [key: string]: JsonValue } = {};
    for (const [key, entry] of Object.entries(value)) {
      if (entry !== undefined) out[key] = encode(entry);
    }
    return out;
  }
  throw new TypeError(`unsupported JSON value: ${String(value)}`);
}

function unit(name: string): JsonValue {
  return name;
}

function newtype(name: string, value: unknown): JsonValue {
  return { [name]: encode(value) };
}

function struct(name: string, fields: Record<string, unknown>): JsonValue {
  const out: Record<string, JsonValue> = {};
  for (const [key, value] of Object.entries(fields)) {
    if (value !== undefined) out[key] = encode(value);
  }
  return { [name]: out };
}

function snakeCase(name: string): string {
  return name
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .toLowerCase();
}

function astTag(name: string, fields: Record<string, unknown>): JsonValue {
  return struct(snakeCase(name), fields);
}

function astNewtype(name: string, value: unknown): JsonValue {
  return newtype(snakeCase(name), value);
}

function astUnit(name: string): JsonValue {
  return unit(snakeCase(name));
}

function astBoundLiteral(value: unknown): JsonValue {
  return astNewtype("Literal", value);
}

function astBoundExpr(value: unknown): JsonValue {
  return astNewtype("Expr", value);
}

export function stringifyJson(value: unknown, pretty = false): string {
  const encoded = encode(value);
  return stringifyEncoded(encoded, pretty ? 0 : undefined);
}

export function parseJsonStructural(json: string): unknown {
  return JSON.parse(quoteUnsafeIntegerTokens(json));
}

/** Parse server JSON while preserving integers outside JavaScript's safe range as `bigint`. */
export function parseJson(json: string): unknown {
  return reviveUnsafeIntegers(parseJsonStructural(json));
}

export function structuralJsonEqual(left: string, right: string): boolean {
  return JSON.stringify(canonicalizeJson(parseJsonStructural(left))) === JSON.stringify(canonicalizeJson(parseJsonStructural(right)));
}

export function canonicalizeJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalizeJson);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, entry]) => [key, canonicalizeJson(entry)]),
  );
}

function quoteUnsafeIntegerTokens(json: string): string {
  let out = "";
  let i = 0;
  let inString = false;
  let escaped = false;

  while (i < json.length) {
    const ch = json[i];
    if (inString) {
      out += ch;
      if (escaped) {
        escaped = false;
      } else if (ch === "\\") {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      i += 1;
      continue;
    }

    if (ch === '"') {
      inString = true;
      out += ch;
      i += 1;
      continue;
    }

    if (ch === "-" || (ch >= "0" && ch <= "9")) {
      const start = i;
      i += 1;
      while (i < json.length && /[0-9.eE+-]/.test(json[i] ?? "")) i += 1;
      const token = json.slice(start, i);
      if (/^-?\d+$/.test(token)) {
        const numeric = BigInt(token);
        if (numeric > BigInt(Number.MAX_SAFE_INTEGER) || numeric < BigInt(Number.MIN_SAFE_INTEGER)) {
          out += `{${JSON.stringify("\u0000helixUnsafeInteger")}:${JSON.stringify(token)}}`;
        } else {
          out += token;
        }
      } else {
        out += token;
      }
      continue;
    }

    out += ch;
    i += 1;
  }

  return out;
}

function reviveUnsafeIntegers(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(reviveUnsafeIntegers);
  if (value === null || typeof value !== "object") return value;
  const entries = Object.entries(value);
  if (entries.length === 1 && entries[0]?.[0] === "\u0000helixUnsafeInteger") {
    return BigInt(entries[0][1] as string);
  }
  return Object.fromEntries(entries.map(([key, entry]) => [key, reviveUnsafeIntegers(entry)]));
}

function stringifyEncoded(value: JsonValue, indent: number | undefined): string {
  const space = indent === undefined ? "" : "  ".repeat(indent);
  const next = indent === undefined ? undefined : indent + 1;
  const nextSpace = next === undefined ? "" : "  ".repeat(next);

  if (value === null) return "null";
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("non-finite numbers cannot be serialized as JSON");
    return String(value);
  }
  if (typeof value === "bigint") return value.toString();
  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    if (indent === undefined) return `[${value.map((entry) => stringifyEncoded(entry, undefined)).join(",")}]`;
    return `[
${nextSpace}${value.map((entry) => stringifyEncoded(entry, next)).join(`,\n${nextSpace}`)}
${space}]`;
  }

  const entries = Object.entries(value as Record<string, JsonValue | undefined>).filter(([, entry]) => entry !== undefined) as [
    string,
    JsonValue,
  ][];
  if (entries.length === 0) return "{}";
  if (indent === undefined) {
    return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${stringifyEncoded(entry, undefined)}`).join(",")}}`;
  }
  return `{
${nextSpace}${entries.map(([key, entry]) => `${JSON.stringify(key)}: ${stringifyEncoded(entry, next)}`).join(`,\n${nextSpace}`)}
${space}}`;
}

export class QueryError extends Error {
  readonly kind: "Serialize" | "Utf8" | "UnsupportedBytesParameter" | "InvalidDateTimeParameter";
  readonly path?: string;
  readonly millis?: bigint;

  private constructor(kind: QueryError["kind"], message: string, path?: string, millis?: bigint) {
    super(message);
    this.name = "QueryError";
    this.kind = kind;
    this.path = path;
    this.millis = millis;
  }

  static serialize(message: string): QueryError {
    return new QueryError("Serialize", `json serialization error: ${message}`);
  }

  static utf8(message: string): QueryError {
    return new QueryError("Utf8", `utf8 conversion error: ${message}`);
  }

  static unsupportedBytes(path: string): QueryError {
    return new QueryError("UnsupportedBytesParameter", `parameter '${path}' uses bytes, which the query JSON route cannot represent`, path);
  }

  static invalidDatetime(path: string, millis: bigint): QueryError {
    return new QueryError(
      "InvalidDateTimeParameter",
      `parameter '${path}' uses datetime millis '${millis}', which cannot be rendered as RFC3339`,
      path,
      millis,
    );
  }
}

export type NodeId = number | bigint;
export type EdgeId = number | bigint;
export type ParamValue = PropertyValue;
export type ParamObject = Record<string, PropertyValue | PropertyValueInput>;

function intToJson(value: number | bigint): number | bigint {
  if (typeof value === "bigint") return value;
  if (!Number.isInteger(value)) throw new TypeError(`expected integer, got ${value}`);
  if (!Number.isSafeInteger(value)) throw new TypeError(`unsafe integer number: ${value}`);
  return value;
}

export class DateTime {
  private readonly value: bigint;

  private constructor(millis: bigint) {
    this.value = millis;
  }

  static fromMillis(millis: number | bigint): DateTime {
    return new DateTime(BigInt(intToJson(millis)));
  }

  static parseRfc3339(input: string): DateTime {
    const parsed = Date.parse(input);
    if (Number.isNaN(parsed)) throw new TypeError(`invalid RFC3339 datetime: ${input}`);
    return DateTime.fromMillis(parsed);
  }

  millis(): bigint {
    return this.value;
  }

  toRfc3339(): string {
    return dateTimeToRfc3339(this, "datetime");
  }
}

function dateTimeToRfc3339(value: DateTime, path: string): string {
  const millis = value.millis();
  const asNumber = Number(millis);
  if (!Number.isSafeInteger(asNumber)) throw QueryError.invalidDatetime(path, millis);
  return new Date(asNumber).toISOString();
}

class I64Literal {
  constructor(readonly value: number | bigint) {}
}
class F32Literal {
  constructor(readonly value: number) {}
}
class F64Literal {
  constructor(readonly value: number) {}
}
class BytesLiteral {
  constructor(readonly value: Uint8Array | number[]) {}
}
class DateTimeLiteral {
  constructor(readonly value: DateTime) {}
}

export function i64(value: number | bigint): I64Literal {
  return new I64Literal(value);
}
export function f32(value: number): F32Literal {
  return new F32Literal(value);
}
export function f64(value: number): F64Literal {
  return new F64Literal(value);
}
export function bytes(value: Uint8Array | number[]): BytesLiteral {
  return new BytesLiteral(value);
}
export function dateTime(value: DateTime): DateTimeLiteral {
  return new DateTimeLiteral(value);
}

export interface PropertyValueInputObject {
  [key: string]: PropertyValueInput;
}
export type PropertyValueInput =
  | null
  | boolean
  | number
  | bigint
  | string
  | DateTime
  | I64Literal
  | F32Literal
  | F64Literal
  | BytesLiteral
  | DateTimeLiteral
  | Uint8Array
  | number[]
  | string[]
  | PropertyValue
  | PropertyValueInput[]
  | PropertyValueInputObject;

export class PropertyValue implements Encodable {
  private constructor(
    readonly variant: string,
    readonly payload?: unknown,
  ) {}

  static null(): PropertyValue {
    return new PropertyValue("Null");
  }
  static bool(value: boolean): PropertyValue {
    return new PropertyValue("Bool", value);
  }
  static i64(value: number | bigint): PropertyValue {
    return new PropertyValue("I64", intToJson(value));
  }
  static dateTime(value: DateTime | number | bigint): PropertyValue {
    return new PropertyValue("DateTime", value instanceof DateTime ? value.millis() : intToJson(value));
  }
  static f64(value: number): PropertyValue {
    return new PropertyValue("F64", value);
  }
  static f32(value: number): PropertyValue {
    return new PropertyValue("F32", value);
  }
  static string(value: string): PropertyValue {
    return new PropertyValue("String", value);
  }
  static bytes(value: Uint8Array | number[]): PropertyValue {
    return new PropertyValue("Bytes", Array.from(value));
  }
  static i64Array(values: (number | bigint)[]): PropertyValue {
    return new PropertyValue("I64Array", values.map(intToJson));
  }
  static f64Array(values: number[]): PropertyValue {
    return new PropertyValue("F64Array", values);
  }
  static f32Array(values: number[]): PropertyValue {
    return new PropertyValue("F32Array", values);
  }
  static stringArray(values: string[]): PropertyValue {
    return new PropertyValue("StringArray", values);
  }
  static array(values: PropertyValueInput[]): PropertyValue {
    return new PropertyValue("Array", values.map(PropertyValue.from));
  }
  static object(values: Record<string, PropertyValueInput>): PropertyValue {
    const out: Record<string, PropertyValue> = {};
    for (const [key, value] of Object.entries(values)) out[key] = PropertyValue.from(value);
    return new PropertyValue("Object", out);
  }

  static from(value: PropertyValueInput): PropertyValue {
    if (value instanceof PropertyValue) return value;
    if (value instanceof I64Literal) return PropertyValue.i64(value.value);
    if (value instanceof F32Literal) return PropertyValue.f32(value.value);
    if (value instanceof F64Literal) return PropertyValue.f64(value.value);
    if (value instanceof BytesLiteral) return PropertyValue.bytes(value.value);
    if (value instanceof DateTimeLiteral) return PropertyValue.dateTime(value.value);
    if (value instanceof DateTime) return PropertyValue.dateTime(value);
    if (value === null) return PropertyValue.null();
    if (typeof value === "boolean") return PropertyValue.bool(value);
    if (typeof value === "string") return PropertyValue.string(value);
    if (typeof value === "bigint") return PropertyValue.i64(value);
    if (typeof value === "number") return Number.isInteger(value) ? PropertyValue.i64(value) : PropertyValue.f64(value);
    if (value instanceof Uint8Array) return PropertyValue.bytes(value);
    if (Array.isArray(value)) {
      if (value.every((entry) => typeof entry === "string")) return PropertyValue.stringArray(value as string[]);
      if (value.every((entry) => typeof entry === "number" && Number.isInteger(entry))) return PropertyValue.i64Array(value as number[]);
      if (value.every((entry) => typeof entry === "number")) return PropertyValue.f64Array(value as number[]);
      return PropertyValue.array(value as PropertyValueInput[]);
    }
    return PropertyValue.object(value as Record<string, PropertyValueInput>);
  }

  asStr(): string | undefined {
    return this.variant === "String" ? (this.payload as string) : undefined;
  }
  asI64(): number | bigint | undefined {
    return this.variant === "I64" ? (this.payload as number | bigint) : undefined;
  }
  static datetimeMillis(millis: number | bigint): PropertyValue {
    return PropertyValue.dateTime(millis);
  }
  asDatetimeMillis(): number | bigint | undefined {
    return this.variant === "DateTime" ? (this.payload as number | bigint) : undefined;
  }
  asF64(): number | undefined {
    return this.variant === "F64" || this.variant === "F32" ? (this.payload as number) : undefined;
  }
  asBool(): boolean | undefined {
    return this.variant === "Bool" ? (this.payload as boolean) : undefined;
  }
  asArray(): PropertyValue[] | undefined {
    return this.variant === "Array" ? (this.payload as PropertyValue[]) : undefined;
  }
  asObject(): Record<string, PropertyValue> | undefined {
    return this.variant === "Object" ? (this.payload as Record<string, PropertyValue>) : undefined;
  }

  toJSON(): JsonValue {
    if (this.variant === "Null") return astUnit("Null");
    return astNewtype(this.variant, this.payload);
  }
}

export class PropertyInput implements Encodable {
  private constructor(
    readonly variant: "Value" | "Expr",
    readonly payload: PropertyValue | Expr,
  ) {}
  static value(value: PropertyValueInput): PropertyInput {
    return new PropertyInput("Value", PropertyValue.from(value));
  }
  static expr(expr: Expr | ParamRef): PropertyInput {
    return new PropertyInput("Expr", expr instanceof ParamRef ? Expr.param(expr.name) : expr);
  }
  static param(name: string): PropertyInput {
    return PropertyInput.expr(Expr.param(name));
  }
  static from(value: PropertyValueInput | Expr | ParamRef | PropertyInput): PropertyInput {
    if (value instanceof PropertyInput) return value;
    if (value instanceof Expr || value instanceof ParamRef) return PropertyInput.expr(value);
    return PropertyInput.value(value as PropertyValueInput);
  }
  // Convert into an Expr, promoting a literal value to Expr::Constant (mirrors Rust PropertyInput::into_expr).
  toExpr(): Expr {
    return this.variant === "Expr" ? (this.payload as Expr) : Expr.val(this.payload as PropertyValue);
  }
  toJSON(): JsonValue {
    return astNewtype(this.variant, this.payload);
  }
}

export class NodeRef implements Encodable {
  private constructor(
    readonly variant: "All" | "Ids" | "Var" | "Param",
    readonly payload?: unknown,
  ) {}
  static all(): NodeRef {
    return new NodeRef("All");
  }
  static id(id: NodeId): NodeRef {
    return new NodeRef("Ids", [intToJson(id)]);
  }
  static ids(ids: Iterable<NodeId>): NodeRef {
    return new NodeRef("Ids", Array.from(ids, intToJson));
  }
  static var(name: string): NodeRef {
    return new NodeRef("Var", name);
  }
  static param(name: string): NodeRef {
    return new NodeRef("Param", name);
  }
  static from(value: NodeRef | NodeId | NodeId[] | string): NodeRef {
    if (value instanceof NodeRef) return value;
    if (typeof value === "string") return NodeRef.var(value);
    if (Array.isArray(value)) return NodeRef.ids(value);
    return NodeRef.id(value);
  }
  toJSON(): JsonValue {
    return this.variant === "All" ? astUnit("All") : astNewtype(this.variant, this.payload);
  }
}

export class EdgeRef implements Encodable {
  private constructor(
    readonly variant: "All" | "Ids" | "Var" | "Param",
    readonly payload?: unknown,
  ) {}
  static all(): EdgeRef {
    return new EdgeRef("All");
  }
  static id(id: EdgeId): EdgeRef {
    return new EdgeRef("Ids", [intToJson(id)]);
  }
  static ids(ids: Iterable<EdgeId>): EdgeRef {
    return new EdgeRef("Ids", Array.from(ids, intToJson));
  }
  static var(name: string): EdgeRef {
    return new EdgeRef("Var", name);
  }
  static param(name: string): EdgeRef {
    return new EdgeRef("Param", name);
  }
  static from(value: EdgeRef | EdgeId | EdgeId[]): EdgeRef {
    if (value instanceof EdgeRef) return value;
    if (Array.isArray(value)) return EdgeRef.ids(value);
    return EdgeRef.id(value);
  }
  toJSON(): JsonValue {
    return this.variant === "All" ? astUnit("All") : astNewtype(this.variant, this.payload);
  }
}

export enum CompareOp {
  Eq = "eq",
  Neq = "neq",
  Gt = "gt",
  Gte = "gte",
  Lt = "lt",
  Lte = "lte",
}
export enum Order {
  Asc = "asc",
  Desc = "desc",
}

export enum ShortestPathDirection {
  Out = "out",
  In = "in",
  Both = "both",
}

export enum RangeIndexDirection {
  Asc = "asc",
  Desc = "desc",
}

export enum VectorDistanceMetric {
  Cosine = "cosine",
  Euclidean = "euclidean",
  Manhattan = "manhattan",
}

function rangeIndexFields(label: string, property: string, direction: RangeIndexDirection): Record<string, unknown> {
  return { label, property, direction };
}
export enum EmitBehavior {
  None = "none",
  Before = "before",
  After = "after",
  All = "all",
}
export enum AggregateFunction {
  Count = "count",
  Sum = "sum",
  Min = "min",
  Max = "max",
  Mean = "mean",
}

export interface WhenThen {
  when: Predicate;
  then: Expr;
}

export const WhenThen = (when: Predicate, then: Expr): WhenThen => ({ when, then });

type WhenThenInput = WhenThen | readonly [Predicate, Expr];

export class Expr implements Encodable {
  private constructor(
    readonly variant: string,
    readonly payload?: unknown,
  ) {}
  static prop(name: string): Expr {
    return new Expr("Property", name);
  }
  static val(value: PropertyValueInput): Expr {
    return new Expr("Constant", PropertyValue.from(value));
  }
  static id(): Expr {
    return new Expr("Id");
  }
  static timestamp(): Expr {
    return new Expr("Timestamp");
  }
  static datetime(): Expr {
    return new Expr("DateTimeNow");
  }
  static param(name: string): Expr {
    return new Expr("Param", name);
  }
  add(other: Expr): Expr {
    return new Expr("Add", [this, other]);
  }
  sub(other: Expr): Expr {
    return new Expr("Sub", [this, other]);
  }
  mul(other: Expr): Expr {
    return new Expr("Mul", [this, other]);
  }
  div(other: Expr): Expr {
    return new Expr("Div", [this, other]);
  }
  modulo(other: Expr): Expr {
    return new Expr("Mod", [this, other]);
  }
  neg(): Expr {
    return new Expr("Neg", this);
  }
  static case(whenThen: readonly WhenThenInput[], elseExpr?: Expr | null): Expr {
    return new Expr("Case", {
      when_then: whenThen.map((branch) => (Array.isArray(branch) ? { when: branch[0], then: branch[1] } : branch)),
      else_expr: elseExpr ?? null,
    });
  }
  toJSON(): JsonValue {
    if (["Id", "Timestamp", "DateTimeNow"].includes(this.variant)) return astUnit(this.variant);
    if (["Add", "Sub", "Mul", "Div", "Mod"].includes(this.variant)) {
      const [left, right] = this.payload as [Expr, Expr];
      return astTag(this.variant, { left, right });
    }
    if (this.variant === "Neg") return astTag("Neg", { expr: this.payload });
    if (this.variant === "Case") {
      const payload = this.payload as { when_then: WhenThen[]; else_expr: Expr | null };
      return astTag("Case", {
        when_then: payload.when_then,
        else_expr: payload.else_expr ?? undefined,
      });
    }
    return astNewtype(this.variant, this.payload);
  }
}

export class StreamBound implements Encodable {
  private constructor(
    readonly variant: "Literal" | "Expr",
    readonly payload: unknown,
  ) {}
  static literal(value: number | bigint): StreamBound {
    const safe = intToJson(value);
    if (typeof safe === "bigint") {
      if (safe > BigInt(Number.MAX_SAFE_INTEGER)) throw new TypeError(`stream bound exceeds JavaScript safe integer range: ${safe}`);
      return new StreamBound("Literal", Number(safe));
    }
    return new StreamBound("Literal", safe);
  }
  static expr(expr: Expr | ParamRef): StreamBound {
    return new StreamBound("Expr", expr instanceof ParamRef ? Expr.param(expr.name) : expr);
  }
  static from(value: StreamBound | number | bigint | Expr | ParamRef): StreamBound {
    if (value instanceof StreamBound) return value;
    if (value instanceof Expr || value instanceof ParamRef) return StreamBound.expr(value);
    if (typeof value === "number" && value < 0) return StreamBound.expr(Expr.val(value));
    if (typeof value === "bigint" && value < 0n) return StreamBound.expr(Expr.val(value));
    return StreamBound.literal(value);
  }
  toJSON(): JsonValue {
    return astNewtype(this.variant, this.payload);
  }
}

export class Predicate implements Encodable {
  private constructor(
    readonly variant: string,
    readonly payload?: unknown,
  ) {}
  static eq(property: string, value: PropertyValueInput | Expr | ParamRef): Predicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value" ? new Predicate("Eq", [property, input.payload]) : new Predicate("EqExpr", [property, input.payload]);
  }
  static neq(property: string, value: PropertyValueInput | Expr | ParamRef): Predicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value"
      ? new Predicate("Neq", [property, input.payload])
      : new Predicate("NeqExpr", [property, input.payload]);
  }
  static gt(property: string, value: PropertyValueInput | Expr | ParamRef): Predicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value" ? new Predicate("Gt", [property, input.payload]) : new Predicate("GtExpr", [property, input.payload]);
  }
  static gte(property: string, value: PropertyValueInput | Expr | ParamRef): Predicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value"
      ? new Predicate("Gte", [property, input.payload])
      : new Predicate("GteExpr", [property, input.payload]);
  }
  static lt(property: string, value: PropertyValueInput | Expr | ParamRef): Predicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value" ? new Predicate("Lt", [property, input.payload]) : new Predicate("LtExpr", [property, input.payload]);
  }
  static lte(property: string, value: PropertyValueInput | Expr | ParamRef): Predicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value"
      ? new Predicate("Lte", [property, input.payload])
      : new Predicate("LteExpr", [property, input.payload]);
  }
  static between(property: string, min: PropertyValueInput | Expr | ParamRef, max: PropertyValueInput | Expr | ParamRef): Predicate {
    const lo = PropertyInput.from(min);
    const hi = PropertyInput.from(max);
    if (lo.variant === "Value" && hi.variant === "Value") {
      return new Predicate("Between", [property, lo.payload, hi.payload]);
    }
    return new Predicate("BetweenExpr", [property, lo.toExpr(), hi.toExpr()]);
  }
  static hasKey(property: string): Predicate {
    return new Predicate("HasKey", property);
  }
  static isNull(property: string): Predicate {
    return new Predicate("IsNull", property);
  }
  static isNotNull(property: string): Predicate {
    return new Predicate("IsNotNull", property);
  }
  static startsWith(property: string, prefix: string): Predicate {
    return new Predicate("StartsWith", [property, prefix]);
  }
  static endsWith(property: string, suffix: string): Predicate {
    return new Predicate("EndsWith", [property, suffix]);
  }
  static contains(property: string, substring: string): Predicate {
    return new Predicate("Contains", [property, substring]);
  }
  static containsParam(property: string, paramName: string): Predicate {
    return new Predicate("ContainsExpr", [property, Expr.param(paramName)]);
  }
  static isIn(property: string, values: PropertyValueInput): Predicate {
    return new Predicate("IsIn", [property, PropertyValue.from(values)]);
  }
  static isInExpr(property: string, values: Expr | ParamRef): Predicate {
    return new Predicate("IsInExpr", [property, values instanceof ParamRef ? Expr.param(values.name) : values]);
  }
  static isInParam(property: string, paramName: string): Predicate {
    return Predicate.isInExpr(property, Expr.param(paramName));
  }
  static and(predicates: Predicate[]): Predicate {
    return new Predicate("And", predicates);
  }
  static or(predicates: Predicate[]): Predicate {
    return new Predicate("Or", predicates);
  }
  static not(predicate: Predicate): Predicate {
    return new Predicate("Not", predicate);
  }
  static compare(left: Expr, op: CompareOp, right: Expr): Predicate {
    return new Predicate("Compare", { left, op, right });
  }
  static eqParam(property: string, paramName: string): Predicate {
    return new Predicate("EqExpr", [property, Expr.param(paramName)]);
  }
  static neqParam(property: string, paramName: string): Predicate {
    return new Predicate("NeqExpr", [property, Expr.param(paramName)]);
  }
  static gtParam(property: string, paramName: string): Predicate {
    return new Predicate("GtExpr", [property, Expr.param(paramName)]);
  }
  static gteParam(property: string, paramName: string): Predicate {
    return new Predicate("GteExpr", [property, Expr.param(paramName)]);
  }
  static ltParam(property: string, paramName: string): Predicate {
    return new Predicate("LtExpr", [property, Expr.param(paramName)]);
  }
  static lteParam(property: string, paramName: string): Predicate {
    return new Predicate("LteExpr", [property, Expr.param(paramName)]);
  }
  static fromSource(predicate: SourcePredicate): Predicate {
    return predicate;
  }
  toJSON(): JsonValue {
    const propExpr = (property: string) => Expr.prop(property);
    const asExpr = (value: unknown) => (value instanceof Expr ? value : Expr.val(value as PropertyValue));
    const binary = (name: string, payload: unknown[]) => astTag(name, { left: propExpr(payload[0] as string), right: asExpr(payload[1]) });
    switch (this.variant) {
      case "Eq":
      case "EqExpr":
        return binary("Eq", this.payload as unknown[]);
      case "Neq":
      case "NeqExpr":
        return binary("Neq", this.payload as unknown[]);
      case "Gt":
      case "GtExpr":
        return binary("Gt", this.payload as unknown[]);
      case "Gte":
      case "GteExpr":
        return binary("Gte", this.payload as unknown[]);
      case "Lt":
      case "LtExpr":
        return binary("Lt", this.payload as unknown[]);
      case "Lte":
      case "LteExpr":
        return binary("Lte", this.payload as unknown[]);
      case "Between": {
        const [property, min, max] = this.payload as unknown[];
        return astTag("Between", { value: propExpr(property as string), min: asExpr(min), max: asExpr(max) });
      }
      case "BetweenExpr": {
        const [property, min, max] = this.payload as unknown[];
        return astTag("Between", { value: propExpr(property as string), min, max });
      }
      case "HasKey":
      case "IsNull":
      case "IsNotNull":
        return astTag(this.variant, { property: this.payload });
      case "StartsWith": {
        const [property, prefix] = this.payload as [string, string];
        return astTag("StartsWith", { value: propExpr(property), prefix: Expr.val(prefix) });
      }
      case "EndsWith": {
        const [property, suffix] = this.payload as [string, string];
        return astTag("EndsWith", { value: propExpr(property), suffix: Expr.val(suffix) });
      }
      case "Contains": {
        const [property, substring] = this.payload as [string, string];
        return astTag("Contains", { value: propExpr(property), substring: Expr.val(substring) });
      }
      case "ContainsExpr": {
        const [property, substring] = this.payload as [string, Expr];
        return astTag("Contains", { value: propExpr(property), substring });
      }
      case "IsIn": {
        const [property, values] = this.payload as unknown[];
        return astTag("IsIn", { value: propExpr(property as string), values: Expr.val(values as PropertyValue) });
      }
      case "IsInExpr": {
        const [property, values] = this.payload as [string, Expr];
        return astTag("IsIn", { value: propExpr(property), values });
      }
      case "And":
      case "Or":
        return astTag(this.variant, { predicates: this.payload });
      case "Not":
        return astTag("Not", { predicate: this.payload });
      case "Compare":
        return astTag("Compare", this.payload as Record<string, unknown>);
      default:
        throw new Error(`unknown predicate: ${this.variant}`);
    }
  }
}

export type SourcePredicate = Predicate;
export const SourcePredicate = Predicate;

export class PropertyProjection implements Encodable {
  constructor(
    readonly source: string,
    readonly alias: string,
  ) {}
  static new(name: string): PropertyProjection {
    return new PropertyProjection(name, name);
  }
  static renamed(source: string, alias: string): PropertyProjection {
    return new PropertyProjection(source, alias);
  }
  toJSON(): JsonValue {
    return { source: this.source, alias: this.alias };
  }
}

export class ExprProjection implements Encodable {
  constructor(
    readonly alias: string,
    readonly expr: Expr,
  ) {}
  static new(alias: string, expr: Expr): ExprProjection {
    return new ExprProjection(alias, expr);
  }
  toJSON(): JsonValue {
    return { alias: this.alias, expr: encode(this.expr) };
  }
}

export type ProjectionInput = Projection | PropertyProjection | ExprProjection;

export class Projection implements Encodable {
  private constructor(readonly inner: PropertyProjection | ExprProjection) {}
  static property(source: string, alias: string): Projection {
    return new Projection(PropertyProjection.renamed(source, alias));
  }
  static fromEndpoint(source: string, alias: string): Projection {
    return Projection.property(`$from.${source}`, alias);
  }
  static toEndpoint(source: string, alias: string): Projection {
    return Projection.property(`$to.${source}`, alias);
  }
  static expr(alias: string, expr: Expr): Projection {
    return new Projection(ExprProjection.new(alias, expr));
  }
  static from(value: ProjectionInput): Projection {
    return value instanceof Projection ? value : new Projection(value);
  }
  toJSON(): JsonValue {
    return this.inner instanceof ExprProjection ? astNewtype("Expr", this.inner) : astNewtype("Property", this.inner);
  }
}

export type BindingTarget = "current" | { binding: string };

export const BindingTarget = {
  current(): BindingTarget {
    return "current";
  },
  binding(name: string): BindingTarget {
    return { binding: name };
  },
};

export type BindingValueRef = {
  target: BindingTarget;
  source: string;
};

export type BindingProjection =
  | { property: { target: BindingTarget; source: string; alias: string } }
  | { coalesce: { refs: BindingValueRef[]; alias: string } };

export const BindingProjection = {
  property(target: BindingTarget, source: string, alias: string): BindingProjection {
    return { property: { target, source, alias } };
  },
  current(source: string, alias: string): BindingProjection {
    return BindingProjection.property(BindingTarget.current(), source, alias);
  },
  binding(name: string, source: string, alias: string): BindingProjection {
    return BindingProjection.property(BindingTarget.binding(name), source, alias);
  },
  valueRef(target: BindingTarget, source: string): BindingValueRef {
    return { target, source };
  },
  currentRef(source: string): BindingValueRef {
    return BindingProjection.valueRef(BindingTarget.current(), source);
  },
  bindingRef(name: string, source: string): BindingValueRef {
    return BindingProjection.valueRef(BindingTarget.binding(name), source);
  },
  coalesce(refs: BindingValueRef[], alias: string): BindingProjection {
    return { coalesce: { refs, alias } };
  },
};

function validateBindingName(name: string): string {
  if (name.length === 0) throw new TypeError("binding name must not be empty");
  return name;
}

export class RepeatConfig implements Encodable {
  readonly timesValue: number | null;
  readonly untilValue: Predicate | null;
  readonly emitValue: EmitBehavior;
  readonly emitPredicateValue: Predicate | null;
  readonly maxDepthValue: number;

  private constructor(
    readonly traversal: SubTraversal,
    times: number | null,
    until: Predicate | null,
    emit: EmitBehavior,
    emitPredicate: Predicate | null,
    maxDepth: number,
  ) {
    this.timesValue = times;
    this.untilValue = until;
    this.emitValue = emit;
    this.emitPredicateValue = emitPredicate;
    this.maxDepthValue = maxDepth;
  }

  static new(traversal: SubTraversal): RepeatConfig {
    return new RepeatConfig(traversal, null, null, EmitBehavior.None, null, 100);
  }
  times(n: number): RepeatConfig {
    return new RepeatConfig(this.traversal, n, this.untilValue, this.emitValue, this.emitPredicateValue, this.maxDepthValue);
  }
  until(predicate: Predicate): RepeatConfig {
    return new RepeatConfig(this.traversal, this.timesValue, predicate, this.emitValue, this.emitPredicateValue, this.maxDepthValue);
  }
  emitAll(): RepeatConfig {
    return new RepeatConfig(
      this.traversal,
      this.timesValue,
      this.untilValue,
      EmitBehavior.All,
      this.emitPredicateValue,
      this.maxDepthValue,
    );
  }
  emitBefore(): RepeatConfig {
    return new RepeatConfig(
      this.traversal,
      this.timesValue,
      this.untilValue,
      EmitBehavior.Before,
      this.emitPredicateValue,
      this.maxDepthValue,
    );
  }
  emitAfter(): RepeatConfig {
    return new RepeatConfig(
      this.traversal,
      this.timesValue,
      this.untilValue,
      EmitBehavior.After,
      this.emitPredicateValue,
      this.maxDepthValue,
    );
  }
  emitIf(predicate: Predicate): RepeatConfig {
    return new RepeatConfig(this.traversal, this.timesValue, this.untilValue, EmitBehavior.After, predicate, this.maxDepthValue);
  }
  maxDepth(depth: number): RepeatConfig {
    return new RepeatConfig(this.traversal, this.timesValue, this.untilValue, this.emitValue, this.emitPredicateValue, depth);
  }
  toJSON(): JsonValue {
    return encode({
      traversal: this.traversal,
      times: this.timesValue ?? undefined,
      until: this.untilValue ?? undefined,
      emit: this.emitValue,
      emit_predicate: this.emitPredicateValue ?? undefined,
      max_depth: this.maxDepthValue,
    });
  }
}

export class IndexSpec implements Encodable {
  private constructor(
    readonly variant: string,
    readonly fields: Record<string, unknown>,
  ) {}
  static nodeEquality(label: string, property: string): IndexSpec {
    return new IndexSpec("NodeEquality", { label, property, unique: false });
  }
  static nodeUniqueEquality(label: string, property: string): IndexSpec {
    return new IndexSpec("NodeEquality", { label, property, unique: true });
  }
  static nodeRange(label: string, property: string): IndexSpec {
    return IndexSpec.nodeRangeWithDirection(label, property, RangeIndexDirection.Asc);
  }
  static nodeRangeDesc(label: string, property: string): IndexSpec {
    return IndexSpec.nodeRangeWithDirection(label, property, RangeIndexDirection.Desc);
  }
  static nodeRangeWithDirection(label: string, property: string, direction: RangeIndexDirection): IndexSpec {
    return new IndexSpec("NodeRange", rangeIndexFields(label, property, direction));
  }
  static edgeEquality(label: string, property: string): IndexSpec {
    return new IndexSpec("EdgeEquality", { label, property });
  }
  static edgeRange(label: string, property: string): IndexSpec {
    return IndexSpec.edgeRangeWithDirection(label, property, RangeIndexDirection.Asc);
  }
  static edgeRangeDesc(label: string, property: string): IndexSpec {
    return IndexSpec.edgeRangeWithDirection(label, property, RangeIndexDirection.Desc);
  }
  static edgeRangeWithDirection(label: string, property: string, direction: RangeIndexDirection): IndexSpec {
    return new IndexSpec("EdgeRange", rangeIndexFields(label, property, direction));
  }
  static nodeVector(
    label: string,
    property: string,
    dimension: number,
    metric: VectorDistanceMetric,
    tenantProperty?: string | null,
  ): IndexSpec {
    if (!Number.isSafeInteger(dimension) || dimension <= 0)
      throw new TypeError(`vector dimension must be a positive safe integer: ${dimension}`);
    if (!Object.values(VectorDistanceMetric).includes(metric)) throw new TypeError(`unsupported vector distance metric: ${String(metric)}`);
    return new IndexSpec("NodeVector", { label, property, dimension, metric, tenant_property: tenantProperty ?? undefined });
  }
  static nodeText(label: string, property: string, tenantProperty?: string | null): IndexSpec {
    return new IndexSpec("NodeText", { label, property, tenant_property: tenantProperty ?? undefined });
  }
  static edgeVector(
    label: string,
    property: string,
    dimension: number,
    metric: VectorDistanceMetric,
    tenantProperty?: string | null,
  ): IndexSpec {
    if (!Number.isSafeInteger(dimension) || dimension <= 0)
      throw new TypeError(`vector dimension must be a positive safe integer: ${dimension}`);
    if (!Object.values(VectorDistanceMetric).includes(metric)) throw new TypeError(`unsupported vector distance metric: ${String(metric)}`);
    return new IndexSpec("EdgeVector", { label, property, dimension, metric, tenant_property: tenantProperty ?? undefined });
  }
  static edgeText(label: string, property: string, tenantProperty?: string | null): IndexSpec {
    return new IndexSpec("EdgeText", { label, property, tenant_property: tenantProperty ?? undefined });
  }
  toJSON(): JsonValue {
    return astTag(this.variant, this.fields);
  }
}

/** Canonical lowercase non-nil UUID used by index lifecycle controls. */
export type IndexOperationId = string;

/** CREATE/DROP receipt with decimal-string u64 fields. */
export type IndexDdlReceipt =
  | { kind: "accepted"; operation_id: IndexOperationId; index_id: string; generation: string; [key: string]: unknown }
  | { kind: "existing_operation"; operation_id: IndexOperationId; [key: string]: unknown }
  | { kind: "already_active"; index_id: string; generation: string; [key: string]: unknown };

/** Durable operation kind returned by lifecycle status. */
export type IndexOperationKind = "build" | "drop";
/** Physical family lane returned by lifecycle status. */
export type IndexFamily = "secondary" | "vector" | "text";
/** Frozen stage returned by lifecycle status. */
export type IndexOperationStage =
  | "scan"
  | "scan_partitions"
  | "catch_up"
  | "validate"
  | "validate_descriptor"
  | "validate_legacy_physical"
  | "compact"
  | "prepare_manifests"
  | "validate_manifests"
  | "activate"
  | "delete_entries"
  | "retire_cache"
  | "delete_physical"
  | "delete_deltas"
  | "delete_metadata"
  | "finalize"
  | "aborting_delete_entries"
  | "aborting_retire_cache"
  | "aborting_delete_physical"
  | "aborting_delete_deltas"
  | "aborting_delete_metadata"
  | "aborting_finalize";
/** Stable reason a blocked operation requires explicit control. */
export type IndexOperationBlockerCode =
  | "invalid_source_data"
  | "uniqueness_violation"
  | "oversized_entity"
  | "manifest_limit"
  | "object_store_configuration_unavailable"
  | "invariant_violation";

/** Stable machine-readable lifecycle API error. */
export type IndexErrorCode =
  | "index_lifecycle_unavailable"
  | "index_already_exists"
  | "index_definition_conflict"
  | "index_busy"
  | "index_not_found"
  | "index_operation_not_found"
  | "index_operation_not_abortable"
  | "index_id_exhausted"
  | "vector_physical_id_exhausted"
  | "index_generation_exhausted"
  | "index_revision_exhausted"
  | "index_operation_revision_exhausted"
  | "stale_index_generation";

/** Decimal-string bounded-work counters. */
export interface IndexOperationProgress {
  entities: string;
  input_bytes: string;
  output_operations: string;
  output_bytes: string;
  [key: string]: unknown;
}

/** Fields present in every lifecycle status variant. */
export interface IndexOperationStatusCommon {
  operation_id: IndexOperationId;
  index_id: string;
  generation: string;
  operation_kind: IndexOperationKind;
  family: IndexFamily;
  stage: IndexOperationStage;
  attempt: number;
  progress: IndexOperationProgress;
  [key: string]: unknown;
}

/** Status union. Additive response fields remain forward-compatible. */
export type IndexOperationStatus =
  | (IndexOperationStatusCommon & { status: "queued" })
  | (IndexOperationStatusCommon & { status: "running" })
  | (IndexOperationStatusCommon & { status: "blocked"; blocker_code: IndexOperationBlockerCode; message?: string | null })
  | (IndexOperationStatusCommon & { status: "succeeded" })
  | (IndexOperationStatusCommon & { status: "aborted" });

/** Validate the frozen lowercase non-nil UUID control identifier. */
function indexOperationId(value: string): IndexOperationId {
  if (!/^(?!00000000-0000-0000-0000-000000000000$)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value))
    throw new TypeError(`index operation ID must be a canonical lowercase non-nil UUID: ${value}`);
  return value;
}

const INDEX_OPERATION_KINDS = new Set<IndexOperationKind>(["build", "drop"]);
const INDEX_FAMILIES = new Set<IndexFamily>(["secondary", "vector", "text"]);
const INDEX_OPERATION_BLOCKERS = new Set<IndexOperationBlockerCode>([
  "invalid_source_data",
  "uniqueness_violation",
  "oversized_entity",
  "manifest_limit",
  "object_store_configuration_unavailable",
  "invariant_violation",
]);
const INDEX_OPERATION_STAGES = new Set<IndexOperationStage>([
  "scan",
  "scan_partitions",
  "catch_up",
  "validate",
  "validate_descriptor",
  "validate_legacy_physical",
  "compact",
  "prepare_manifests",
  "validate_manifests",
  "activate",
  "delete_entries",
  "retire_cache",
  "delete_physical",
  "delete_deltas",
  "delete_metadata",
  "finalize",
  "aborting_delete_entries",
  "aborting_retire_cache",
  "aborting_delete_physical",
  "aborting_delete_deltas",
  "aborting_delete_metadata",
  "aborting_finalize",
]);

/** Require the object shape shared by tagged lifecycle responses. */
function lifecycleRecord(value: unknown, contract: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new TypeError(`${contract} must be a JSON object`);
  return value as Record<string, unknown>;
}

/** Read one required string field without coercing caller data. */
function lifecycleString(record: Record<string, unknown>, field: string): string {
  const value = record[field];
  if (typeof value !== "string") throw new TypeError(`${field} must be a string`);
  return value;
}

/** Validate a canonical decimal-string u64 and preserve its wire form. */
function lifecycleU64(value: unknown, field: string, allowZero: boolean): string {
  if (typeof value !== "string" || !/^(?:0|[1-9][0-9]*)$/.test(value))
    throw new TypeError(`${field} must be a canonical unsigned decimal string`);
  const parsed = BigInt(value);
  if ((!allowZero && parsed === 0n) || parsed > 18_446_744_073_709_551_615n) throw new TypeError(`${field} is outside the u64 range`);
  return value;
}

/** Decode and validate one CREATE/DROP receipt while ignoring additive fields. */
export function parseIndexDdlReceipt(value: unknown): IndexDdlReceipt {
  const receipt = lifecycleRecord(value, "index DDL receipt");
  const kind = lifecycleString(receipt, "kind");
  if (kind === "accepted") {
    indexOperationId(lifecycleString(receipt, "operation_id"));
    lifecycleU64(receipt.index_id, "index_id", false);
    lifecycleU64(receipt.generation, "generation", false);
  } else if (kind === "existing_operation") {
    indexOperationId(lifecycleString(receipt, "operation_id"));
  } else if (kind === "already_active") {
    lifecycleU64(receipt.index_id, "index_id", false);
    lifecycleU64(receipt.generation, "generation", false);
  } else {
    throw new TypeError(`unknown index DDL receipt kind: ${kind}`);
  }
  return receipt as IndexDdlReceipt;
}

/** Decode and validate one lifecycle status while ignoring additive fields. */
export function parseIndexOperationStatus(value: unknown): IndexOperationStatus {
  const status = lifecycleRecord(value, "index operation status");
  const tag = lifecycleString(status, "status");
  if (!new Set(["queued", "running", "blocked", "succeeded", "aborted"]).has(tag))
    throw new TypeError(`unknown index operation status: ${tag}`);
  indexOperationId(lifecycleString(status, "operation_id"));
  lifecycleU64(status.index_id, "index_id", false);
  lifecycleU64(status.generation, "generation", false);
  const operationKind = lifecycleString(status, "operation_kind") as IndexOperationKind;
  if (!INDEX_OPERATION_KINDS.has(operationKind)) throw new TypeError(`unknown index operation kind: ${operationKind}`);
  const family = lifecycleString(status, "family") as IndexFamily;
  if (!INDEX_FAMILIES.has(family)) throw new TypeError(`unknown index family: ${family}`);
  const stage = lifecycleString(status, "stage") as IndexOperationStage;
  if (!INDEX_OPERATION_STAGES.has(stage)) throw new TypeError(`unknown index operation stage: ${stage}`);
  if (!Number.isInteger(status.attempt) || (status.attempt as number) < 0 || (status.attempt as number) > 4_294_967_295)
    throw new TypeError("attempt must be a u32 JSON number");
  const progress = lifecycleRecord(status.progress, "index operation progress");
  lifecycleU64(progress.entities, "progress.entities", true);
  lifecycleU64(progress.input_bytes, "progress.input_bytes", true);
  lifecycleU64(progress.output_operations, "progress.output_operations", true);
  lifecycleU64(progress.output_bytes, "progress.output_bytes", true);
  if (tag === "blocked") {
    const blocker = lifecycleString(status, "blocker_code") as IndexOperationBlockerCode;
    if (!INDEX_OPERATION_BLOCKERS.has(blocker)) throw new TypeError(`unknown index operation blocker: ${blocker}`);
    if (status.message !== undefined && status.message !== null && typeof status.message !== "string")
      throw new TypeError("message must be a string or null when present");
  }
  if (tag === "aborted" && (operationKind !== "build" || !stage.startsWith("aborting_")))
    throw new TypeError("aborted status must describe build cleanup");
  return status as IndexOperationStatus;
}

type StepStyle = "unit" | "newtype" | "tuple" | "struct";

export class Step implements Encodable {
  private constructor(
    readonly variant: string,
    readonly style: StepStyle,
    readonly payload?: unknown,
  ) {}
  private static unit(name: string): Step {
    return new Step(name, "unit");
  }
  private static newtype(name: string, value: unknown): Step {
    return new Step(name, "newtype", value);
  }
  private static tuple(name: string, values: unknown[]): Step {
    return new Step(name, "tuple", values);
  }
  private static struct(name: string, fields: Record<string, unknown>): Step {
    return new Step(name, "struct", fields);
  }
  static n(nodes: NodeRef): Step {
    return Step.newtype("N", nodes);
  }
  static nWhere(predicate: SourcePredicate): Step {
    return Step.newtype("NWhere", predicate);
  }
  static shortestPath(
    source: NodeRef,
    target: NodeRef,
    maxDepth: number,
    options: { label?: string | null; direction?: ShortestPathDirection } = {},
  ): Step {
    return Step.struct("ShortestPath", {
      source,
      target,
      label: options.label ?? undefined,
      direction: options.direction ?? ShortestPathDirection.Out,
      max_depth: maxDepth,
    });
  }
  static e(edges: EdgeRef): Step {
    return Step.newtype("E", edges);
  }
  static eWhere(predicate: SourcePredicate): Step {
    return Step.newtype("EWhere", predicate);
  }
  static vectorSearchNodes(
    label: string,
    property: string,
    queryVector: PropertyInput,
    k: StreamBound,
    tenantValue?: PropertyInput | null,
  ): Step {
    return Step.struct("VectorSearchNodes", { label, property, tenant_value: tenantValue ?? undefined, query_vector: queryVector, k });
  }
  static textSearchNodes(
    label: string,
    property: string,
    queryText: PropertyInput,
    k: StreamBound,
    tenantValue?: PropertyInput | null,
  ): Step {
    return Step.struct("TextSearchNodes", { label, property, tenant_value: tenantValue ?? undefined, query_text: queryText, k });
  }
  static vectorSearchEdges(
    label: string,
    property: string,
    queryVector: PropertyInput,
    k: StreamBound,
    tenantValue?: PropertyInput | null,
  ): Step {
    return Step.struct("VectorSearchEdges", { label, property, tenant_value: tenantValue ?? undefined, query_vector: queryVector, k });
  }
  static vectorSearchNodesWithin(
    label: string,
    property: string,
    queryVector: PropertyInput,
    k: StreamBound,
    tenantValue?: PropertyInput | null,
  ): Step {
    return Step.struct("VectorSearchNodesWithin", {
      label,
      property,
      tenant_value: tenantValue ?? undefined,
      query_vector: queryVector,
      k,
    });
  }
  static vectorSearchEdgesWithin(
    label: string,
    property: string,
    queryVector: PropertyInput,
    k: StreamBound,
    tenantValue?: PropertyInput | null,
  ): Step {
    return Step.struct("VectorSearchEdgesWithin", {
      label,
      property,
      tenant_value: tenantValue ?? undefined,
      query_vector: queryVector,
      k,
    });
  }
  static textSearchNodesWithin(
    label: string,
    property: string,
    queryText: PropertyInput,
    k: StreamBound,
    tenantValue?: PropertyInput | null,
  ): Step {
    return Step.struct("TextSearchNodesWithin", {
      label,
      property,
      tenant_value: tenantValue ?? undefined,
      query_text: queryText,
      k,
    });
  }
  static textSearchEdgesWithin(
    label: string,
    property: string,
    queryText: PropertyInput,
    k: StreamBound,
    tenantValue?: PropertyInput | null,
  ): Step {
    return Step.struct("TextSearchEdgesWithin", {
      label,
      property,
      tenant_value: tenantValue ?? undefined,
      query_text: queryText,
      k,
    });
  }
  static textSearchEdges(
    label: string,
    property: string,
    queryText: PropertyInput,
    k: StreamBound,
    tenantValue?: PropertyInput | null,
  ): Step {
    return Step.struct("TextSearchEdges", { label, property, tenant_value: tenantValue ?? undefined, query_text: queryText, k });
  }
  static out(label?: string | null): Step {
    return Step.newtype("Out", label ?? null);
  }
  static in(label?: string | null): Step {
    return Step.newtype("In", label ?? null);
  }
  static both(label?: string | null): Step {
    return Step.newtype("Both", label ?? null);
  }
  static outE(label?: string | null): Step {
    return Step.newtype("OutE", label ?? null);
  }
  static inE(label?: string | null): Step {
    return Step.newtype("InE", label ?? null);
  }
  static bothE(label?: string | null): Step {
    return Step.newtype("BothE", label ?? null);
  }
  static outN(): Step {
    return Step.unit("OutN");
  }
  static inN(): Step {
    return Step.unit("InN");
  }
  static otherN(): Step {
    return Step.unit("OtherN");
  }
  static has(property: string, value: PropertyValueInput): Step {
    return Step.tuple("Has", [property, PropertyValue.from(value)]);
  }
  static hasLabel(label: string): Step {
    return Step.newtype("HasLabel", label);
  }
  static hasKey(property: string): Step {
    return Step.newtype("HasKey", property);
  }
  static where(predicate: Predicate): Step {
    return Step.newtype("Where", predicate);
  }
  static dedup(): Step {
    return Step.unit("Dedup");
  }
  static within(name: string): Step {
    return Step.newtype("Within", name);
  }
  static without(name: string): Step {
    return Step.newtype("Without", name);
  }
  static edgeHas(property: string, value: PropertyInput): Step {
    return Step.tuple("EdgeHas", [property, value]);
  }
  static edgeHasLabel(label: string): Step {
    return Step.newtype("EdgeHasLabel", label);
  }
  static limit(bound: StreamBound): Step {
    return bound.variant === "Literal" ? Step.newtype("Limit", bound.payload) : Step.newtype("LimitBy", bound.payload);
  }
  static skip(bound: StreamBound): Step {
    return bound.variant === "Literal" ? Step.newtype("Skip", bound.payload) : Step.newtype("SkipBy", bound.payload);
  }
  static range(start: StreamBound, end: StreamBound): Step {
    return start.variant === "Literal" && end.variant === "Literal"
      ? Step.tuple("Range", [start.payload, end.payload])
      : Step.tuple("RangeBy", [start, end]);
  }
  static as(name: string): Step {
    return Step.newtype("As", name);
  }
  static store(name: string): Step {
    return Step.newtype("Store", name);
  }
  static select(name: string): Step {
    return Step.newtype("Select", name);
  }
  static bind(name: string): Step {
    return Step.newtype("Bind", validateBindingName(name));
  }
  static count(): Step {
    return Step.unit("Count");
  }
  static exists(): Step {
    return Step.unit("Exists");
  }
  static id(): Step {
    return Step.unit("Id");
  }
  static label(): Step {
    return Step.unit("Label");
  }
  static values(properties: string[]): Step {
    return Step.newtype("Values", properties);
  }
  static valueMap(properties?: string[] | null): Step {
    return Step.newtype("ValueMap", properties ?? null);
  }
  static project(projections: ProjectionInput[]): Step {
    return Step.newtype("Project", projections.map(Projection.from));
  }
  static projectBindings(projections: BindingProjection[], distinct = false): Step {
    return Step.struct("ProjectBindings", { projections, distinct });
  }
  static edgeProperties(): Step {
    return Step.unit("EdgeProperties");
  }
  static createIndex(spec: IndexSpec, ifNotExists: boolean): Step {
    return Step.struct("CreateIndex", { spec, if_not_exists: ifNotExists });
  }
  static dropIndex(spec: IndexSpec): Step {
    return Step.struct("DropIndex", { spec });
  }
  /** Build the raw AST terminal for an exact-scope operation lookup. */
  static getIndexOperation(operationId: string): Step {
    return Step.struct("GetIndexOperation", { operation_id: indexOperationId(operationId) });
  }
  /** Build the raw AST terminal for a convergent blocked-operation retry. */
  static retryIndexOperation(operationId: string): Step {
    return Step.struct("RetryIndexOperation", { operation_id: indexOperationId(operationId) });
  }
  /** Build the raw AST terminal for build-abort cleanup. */
  static abortIndexOperation(operationId: string): Step {
    return Step.struct("AbortIndexOperation", { operation_id: indexOperationId(operationId) });
  }
  static createVectorIndexNodes(
    label: string,
    property: string,
    dimension: number,
    metric: VectorDistanceMetric,
    tenantProperty?: string | null,
  ): Step {
    return Step.createIndex(IndexSpec.nodeVector(label, property, dimension, metric, tenantProperty), true);
  }
  static createVectorIndexEdges(
    label: string,
    property: string,
    dimension: number,
    metric: VectorDistanceMetric,
    tenantProperty?: string | null,
  ): Step {
    return Step.createIndex(IndexSpec.edgeVector(label, property, dimension, metric, tenantProperty), true);
  }
  static createTextIndexNodes(label: string, property: string, tenantProperty?: string | null): Step {
    return Step.createIndex(IndexSpec.nodeText(label, property, tenantProperty), true);
  }
  static createTextIndexEdges(label: string, property: string, tenantProperty?: string | null): Step {
    return Step.createIndex(IndexSpec.edgeText(label, property, tenantProperty), true);
  }
  static addN(label: string, properties: [string, PropertyInput][]): Step {
    return Step.struct("AddN", { label, properties });
  }
  static addE(label: string, to: NodeRef, properties: [string, PropertyInput][]): Step {
    return Step.struct("AddE", { label, to, properties });
  }
  static setProperty(name: string, value: PropertyInput): Step {
    return Step.tuple("SetProperty", [name, value]);
  }
  static removeProperty(name: string): Step {
    return Step.newtype("RemoveProperty", name);
  }
  static drop(): Step {
    return Step.unit("Drop");
  }
  static dropEdge(to: NodeRef): Step {
    return Step.newtype("DropEdge", to);
  }
  static dropEdgeLabeled(to: NodeRef, label: string): Step {
    return Step.struct("DropEdgeLabeled", { to, label });
  }
  static dropEdgeById(edges: EdgeRef): Step {
    return Step.newtype("DropEdgeById", edges);
  }
  static orderBy(property: string, order: Order): Step {
    return Step.tuple("OrderBy", [property, order]);
  }
  static orderByMultiple(orderings: [string, Order][]): Step {
    return Step.newtype("OrderByMultiple", orderings);
  }
  static repeat(config: RepeatConfig): Step {
    return Step.newtype("Repeat", config);
  }
  static union(traversals: SubTraversal[]): Step {
    return Step.newtype("Union", traversals);
  }
  static choose(condition: Predicate, thenTraversal: SubTraversal, elseTraversal?: SubTraversal | null): Step {
    return Step.struct("Choose", { condition, then_traversal: thenTraversal, else_traversal: elseTraversal ?? undefined });
  }
  static coalesce(traversals: SubTraversal[]): Step {
    return Step.newtype("Coalesce", traversals);
  }
  static optional(traversal: SubTraversal): Step {
    return Step.newtype("Optional", traversal);
  }
  static group(property: string): Step {
    return Step.newtype("Group", property);
  }
  static groupCount(property: string): Step {
    return Step.newtype("GroupCount", property);
  }
  static aggregateBy(fn: AggregateFunction, property: string): Step {
    return Step.tuple("AggregateBy", [fn, property]);
  }
  static fold(): Step {
    return Step.unit("Fold");
  }
  static unfold(): Step {
    return Step.unit("Unfold");
  }
  static path(): Step {
    return Step.unit("Path");
  }
  static simplePath(): Step {
    return Step.unit("SimplePath");
  }
  static withSack(initial: PropertyValueInput): Step {
    return Step.newtype("WithSack", PropertyValue.from(initial));
  }
  static sackSet(property: string): Step {
    return Step.newtype("SackSet", property);
  }
  static sackAdd(property: string): Step {
    return Step.newtype("SackAdd", property);
  }
  static sackGet(): Step {
    return Step.unit("SackGet");
  }
  static inject(name: string): Step {
    return Step.newtype("Inject", name);
  }
  toAst(input?: JsonValue | null): JsonValue {
    const hasExplicitInput = arguments.length > 0;
    const current = hasExplicitInput ? input : "context";
    const requiredInput = (): JsonValue => {
      if (current == null) throw new TypeError(`step ${this.variant} requires a source AST node`);
      return current;
    };
    const inputField = (): Record<string, unknown> => (current == null ? {} : { input: current });
    const unary = (name: string, fields: Record<string, unknown> = {}) => astTag(name, { input: requiredInput(), ...fields });
    switch (this.variant) {
      case "N":
        return astTag("Nodes", { reference: this.payload });
      case "NWhere":
        return astTag("NodesWhere", { predicate: this.payload });
      case "ShortestPath":
        return astTag("ShortestPath", this.payload as Record<string, unknown>);
      case "E":
        return astTag("Edges", { reference: this.payload });
      case "EWhere":
        return astTag("EdgesWhere", { predicate: this.payload });
      case "VectorSearchNodes":
      case "TextSearchNodes":
      case "VectorSearchEdges":
      case "TextSearchEdges":
        return astTag(this.variant, this.payload as Record<string, unknown>);
      case "VectorSearchNodesWithin":
      case "VectorSearchEdgesWithin":
      case "TextSearchNodesWithin":
      case "TextSearchEdgesWithin":
        return unary(this.variant, this.payload as Record<string, unknown>);
      case "Out":
      case "In":
      case "Both":
      case "OutE":
      case "InE":
      case "BothE":
        return unary(this.variant, { label: this.payload ?? undefined });
      case "OutN":
      case "InN":
      case "OtherN":
      case "Dedup":
      case "Count":
      case "Exists":
      case "Id":
      case "Label":
      case "EdgeProperties":
      case "Fold":
      case "Unfold":
      case "Path":
      case "SimplePath":
      case "SackGet":
        return unary(this.variant);
      case "Has": {
        const [property, value] = this.payload as [string, PropertyValue];
        return unary("Has", { property, value });
      }
      case "HasLabel":
        return unary("HasLabel", { label: this.payload });
      case "HasKey":
        return unary("HasKey", { property: this.payload });
      case "Where":
        return unary("Where", { predicate: this.payload });
      case "Within":
        return unary("Within", { variable: this.payload });
      case "Without":
        return unary("Without", { variable: this.payload });
      case "EdgeHas": {
        const [property, value] = this.payload as [string, PropertyInput];
        return unary("EdgeHas", { property, value });
      }
      case "EdgeHasLabel":
        return unary("EdgeHasLabel", { label: this.payload });
      case "Limit":
        return unary("Limit", { count: astBoundLiteral(this.payload) });
      case "LimitBy":
        return unary("Limit", { count: astBoundExpr(this.payload) });
      case "Skip":
        return unary("Skip", { count: astBoundLiteral(this.payload) });
      case "SkipBy":
        return unary("Skip", { count: astBoundExpr(this.payload) });
      case "Range": {
        const [start, end] = this.payload as unknown[];
        return unary("Range", { start: astBoundLiteral(start), end: astBoundLiteral(end) });
      }
      case "RangeBy": {
        const [start, end] = this.payload as [StreamBound, StreamBound];
        return unary("Range", { start, end });
      }
      case "As":
      case "Store":
      case "Select":
      case "Bind":
        return unary(this.variant, { name: this.payload });
      case "Inject":
        return astTag("Inject", { ...inputField(), variable: this.payload });
      case "Values":
        return unary("Values", { properties: this.payload });
      case "ValueMap":
        return unary("ValueMap", { properties: this.payload ?? undefined });
      case "Project":
        return unary("Project", { projections: this.payload });
      case "ProjectBindings":
        return unary("ProjectBindings", this.payload as Record<string, unknown>);
      case "CreateIndex":
      case "DropIndex":
      case "GetIndexOperation":
      case "RetryIndexOperation":
      case "AbortIndexOperation":
        return astTag(this.variant, this.payload as Record<string, unknown>);
      case "AddN":
        return astTag("AddN", { ...inputField(), ...(this.payload as Record<string, unknown>) });
      case "AddE":
        return unary("AddE", this.payload as Record<string, unknown>);
      case "SetProperty": {
        const [name, value] = this.payload as [string, PropertyInput];
        return unary("SetProperty", { name, value });
      }
      case "RemoveProperty":
        return unary("RemoveProperty", { name: this.payload });
      case "Drop":
        return unary("Drop");
      case "DropEdge":
        return unary("DropEdge", { to: this.payload });
      case "DropEdgeLabeled":
        return unary("DropEdgeLabeled", this.payload as Record<string, unknown>);
      case "DropEdgeById":
        return astTag("DropEdgeById", { ...inputField(), edges: this.payload });
      case "OrderBy": {
        const [property, order] = this.payload as [string, Order];
        return unary("OrderBy", { property, order });
      }
      case "OrderByMultiple":
        return unary("OrderByMultiple", { orderings: this.payload });
      case "Repeat":
        return unary("Repeat", { config: this.payload });
      case "Union":
        return unary("Union", { traversals: this.payload });
      case "Choose":
        return unary("Choose", this.payload as Record<string, unknown>);
      case "Coalesce":
        return unary("Coalesce", { traversals: this.payload });
      case "Optional":
        return unary("Optional", { traversal: this.payload });
      case "Group":
        return unary("Group", { property: this.payload });
      case "GroupCount":
        return unary("GroupCount", { property: this.payload });
      case "AggregateBy": {
        const [fn, property] = this.payload as [AggregateFunction, string];
        return unary("AggregateBy", { function: fn, property });
      }
      case "WithSack":
        return unary("WithSack", { initial: this.payload });
      case "SackSet":
      case "SackAdd":
        return unary(this.variant, { property: this.payload });
      default:
        throw new Error(`unknown step: ${this.variant}`);
    }
  }
  toJSON(): JsonValue {
    return this.toAst();
  }
}

function stepsToAst(steps: Step[], initial: JsonValue | null = null): JsonValue {
  let root = initial;
  for (const step of steps) root = step.toAst(root);
  if (root == null) throw new TypeError("traversal must contain at least one AST node before execution");
  return root;
}

type PropEntries =
  | [string, PropertyValueInput | PropertyInput | Expr | ParamRef][]
  | Record<string, PropertyValueInput | PropertyInput | Expr | ParamRef>;
function propertyEntries(properties: PropEntries = []): [string, PropertyInput][] {
  const entries = Array.isArray(properties) ? properties : Object.entries(properties);
  return entries.map(([key, value]) => [key, PropertyInput.from(value as PropertyValueInput | Expr | ParamRef | PropertyInput)]);
}

export type TraversalState = "empty" | "nodes" | "edges" | "terminal";
export type MutationMode = "read" | "write";

export class Traversal<S extends TraversalState = "nodes", M extends MutationMode = "read"> {
  constructor(
    readonly steps: Step[] = [],
    readonly state: S = "nodes" as S,
    readonly mode: M = "read" as M,
  ) {}
  static new(): Traversal<"empty", "read"> {
    return new Traversal([], "empty", "read");
  }
  static fromSteps<S extends TraversalState, M extends MutationMode>(
    steps: Step[],
    state: S = "nodes" as S,
    mode: M = "read" as M,
  ): Traversal<S, M> {
    return new Traversal(steps, state, mode);
  }
  toJSON(): JsonValue {
    return { root: this.intoAst() };
  }
  intoAst(): JsonValue {
    return stepsToAst(this.steps);
  }
  intoSteps(): Step[] {
    return this.steps;
  }
  hasTerminal(): boolean {
    return this.steps.some((s) =>
      [
        "Count",
        "Exists",
        "Id",
        "Label",
        "Values",
        "ValueMap",
        "Project",
        "ProjectBindings",
        "EdgeProperties",
        "CreateIndex",
        "DropIndex",
        "GetIndexOperation",
        "RetryIndexOperation",
        "AbortIndexOperation",
        "ShortestPath",
      ].includes(s.variant),
    );
  }
  private push<T extends TraversalState>(step: Step, state: T, mode: MutationMode = this.mode): Traversal<T, MutationMode> {
    return new Traversal([...this.steps, step], state, mode);
  }
  n(nodes: NodeRef | NodeId | NodeId[] | string): Traversal<"nodes", M> {
    return this.push(Step.n(NodeRef.from(nodes)), "nodes", this.mode) as Traversal<"nodes", M>;
  }
  nWhere(predicate: SourcePredicate): Traversal<"nodes", M> {
    return this.push(Step.nWhere(predicate), "nodes", this.mode) as Traversal<"nodes", M>;
  }
  nWithLabel(label: string): Traversal<"nodes", M> {
    return this.nWhere(SourcePredicate.eq("$label", label));
  }
  nWithLabelWhere(label: string, predicate: SourcePredicate): Traversal<"nodes", M> {
    return this.nWhere(SourcePredicate.and([SourcePredicate.eq("$label", label), predicate]));
  }
  shortestPath(
    source: NodeRef | NodeId | NodeId[] | string,
    target: NodeRef | NodeId | NodeId[] | string,
    maxDepth: number,
    options: { label?: string | null; direction?: ShortestPathDirection } = {},
  ): Traversal<"terminal", M> {
    return this.push(Step.shortestPath(NodeRef.from(source), NodeRef.from(target), maxDepth, options), "terminal", this.mode) as Traversal<
      "terminal",
      M
    >;
  }
  e(edges: EdgeRef | EdgeId | EdgeId[]): Traversal<"edges", M> {
    return this.push(Step.e(EdgeRef.from(edges)), "edges", this.mode) as Traversal<"edges", M>;
  }
  eWhere(predicate: SourcePredicate): Traversal<"edges", M> {
    return this.push(Step.eWhere(predicate), "edges", this.mode) as Traversal<"edges", M>;
  }
  eWithLabel(label: string): Traversal<"edges", M> {
    return this.eWhere(SourcePredicate.eq("$label", label));
  }
  eWithLabelWhere(label: string, predicate: SourcePredicate): Traversal<"edges", M> {
    return this.eWhere(SourcePredicate.and([SourcePredicate.eq("$label", label), predicate]));
  }
  vectorSearchNodes(
    label: string,
    property: string,
    queryVector: number[],
    k: number,
    tenantValue?: PropertyValueInput | null,
  ): Traversal<"nodes", M> {
    return this.vectorSearchNodesWith(
      label,
      property,
      PropertyInput.value(PropertyValue.f32Array(queryVector)),
      k,
      tenantValue == null ? null : PropertyInput.value(tenantValue),
    );
  }
  vectorSearchNodesWith(
    label: string,
    property: string,
    queryVector: PropertyInput | Expr | ParamRef | PropertyValueInput,
    k: StreamBound | Expr | ParamRef | number | bigint,
    tenantValue?: PropertyInput | Expr | ParamRef | PropertyValueInput | null,
  ): Traversal<"nodes", M> {
    return this.push(
      Step.vectorSearchNodes(
        label,
        property,
        PropertyInput.from(queryVector as never),
        StreamBound.from(k),
        tenantValue == null ? null : PropertyInput.from(tenantValue as never),
      ),
      "nodes",
      this.mode,
    ) as Traversal<"nodes", M>;
  }
  textSearchNodes(
    label: string,
    property: string,
    queryText: string,
    k: number,
    tenantValue?: PropertyValueInput | null,
  ): Traversal<"nodes", M> {
    return this.textSearchNodesWith(label, property, queryText, k, tenantValue == null ? null : PropertyInput.value(tenantValue));
  }
  textSearchNodesWith(
    label: string,
    property: string,
    queryText: PropertyInput | Expr | ParamRef | PropertyValueInput,
    k: StreamBound | Expr | ParamRef | number | bigint,
    tenantValue?: PropertyInput | Expr | ParamRef | PropertyValueInput | null,
  ): Traversal<"nodes", M> {
    return this.push(
      Step.textSearchNodes(
        label,
        property,
        PropertyInput.from(queryText as never),
        StreamBound.from(k),
        tenantValue == null ? null : PropertyInput.from(tenantValue as never),
      ),
      "nodes",
      this.mode,
    ) as Traversal<"nodes", M>;
  }
  vectorSearchEdges(
    label: string,
    property: string,
    queryVector: number[],
    k: number,
    tenantValue?: PropertyValueInput | null,
  ): Traversal<"edges", M> {
    return this.vectorSearchEdgesWith(
      label,
      property,
      PropertyInput.value(PropertyValue.f32Array(queryVector)),
      k,
      tenantValue == null ? null : PropertyInput.value(tenantValue),
    );
  }
  vectorSearchEdgesWith(
    label: string,
    property: string,
    queryVector: PropertyInput | Expr | ParamRef | PropertyValueInput,
    k: StreamBound | Expr | ParamRef | number | bigint,
    tenantValue?: PropertyInput | Expr | ParamRef | PropertyValueInput | null,
  ): Traversal<"edges", M> {
    return this.push(
      Step.vectorSearchEdges(
        label,
        property,
        PropertyInput.from(queryVector as never),
        StreamBound.from(k),
        tenantValue == null ? null : PropertyInput.from(tenantValue as never),
      ),
      "edges",
      this.mode,
    ) as Traversal<"edges", M>;
  }
  /** Rank only the current node/edge stream using its exact IDs as the filter. */
  vectorSearch<T extends "nodes" | "edges">(
    this: Traversal<T, M>,
    label: string,
    property: string,
    queryVector: number[],
    k: number,
    tenantValue?: PropertyValueInput | null,
  ): Traversal<T, M> {
    return this.vectorSearchWith(
      label,
      property,
      PropertyInput.value(PropertyValue.f32Array(queryVector)),
      k,
      tenantValue == null ? null : PropertyInput.value(tenantValue),
    );
  }
  /** Runtime-input form of traversal-scoped vector ranking. */
  vectorSearchWith<T extends "nodes" | "edges">(
    this: Traversal<T, M>,
    label: string,
    property: string,
    queryVector: PropertyInput | Expr | ParamRef | PropertyValueInput,
    k: StreamBound | Expr | ParamRef | number | bigint,
    tenantValue?: PropertyInput | Expr | ParamRef | PropertyValueInput | null,
  ): Traversal<T, M> {
    const step =
      this.state === "nodes"
        ? Step.vectorSearchNodesWithin(
            label,
            property,
            PropertyInput.from(queryVector as never),
            StreamBound.from(k),
            tenantValue == null ? null : PropertyInput.from(tenantValue as never),
          )
        : Step.vectorSearchEdgesWithin(
            label,
            property,
            PropertyInput.from(queryVector as never),
            StreamBound.from(k),
            tenantValue == null ? null : PropertyInput.from(tenantValue as never),
          );
    return this.push(step, this.state, this.mode) as Traversal<T, M>;
  }
  /** Rank only the current node/edge stream by BM25 score. */
  textSearch<T extends "nodes" | "edges">(
    this: Traversal<T, M>,
    label: string,
    property: string,
    queryText: string,
    k: number,
    tenantValue?: PropertyValueInput | null,
  ): Traversal<T, M> {
    return this.textSearchWith(label, property, queryText, k, tenantValue == null ? null : PropertyInput.value(tenantValue));
  }
  /** Runtime-input form of traversal-scoped BM25 ranking. */
  textSearchWith<T extends "nodes" | "edges">(
    this: Traversal<T, M>,
    label: string,
    property: string,
    queryText: PropertyInput | Expr | ParamRef | PropertyValueInput,
    k: StreamBound | Expr | ParamRef | number | bigint,
    tenantValue?: PropertyInput | Expr | ParamRef | PropertyValueInput | null,
  ): Traversal<T, M> {
    const step =
      this.state === "nodes"
        ? Step.textSearchNodesWithin(
            label,
            property,
            PropertyInput.from(queryText as never),
            StreamBound.from(k),
            tenantValue == null ? null : PropertyInput.from(tenantValue as never),
          )
        : Step.textSearchEdgesWithin(
            label,
            property,
            PropertyInput.from(queryText as never),
            StreamBound.from(k),
            tenantValue == null ? null : PropertyInput.from(tenantValue as never),
          );
    return this.push(step, this.state, this.mode) as Traversal<T, M>;
  }
  textSearchEdges(
    label: string,
    property: string,
    queryText: string,
    k: number,
    tenantValue?: PropertyValueInput | null,
  ): Traversal<"edges", M> {
    return this.textSearchEdgesWith(label, property, queryText, k, tenantValue == null ? null : PropertyInput.value(tenantValue));
  }
  textSearchEdgesWith(
    label: string,
    property: string,
    queryText: PropertyInput | Expr | ParamRef | PropertyValueInput,
    k: StreamBound | Expr | ParamRef | number | bigint,
    tenantValue?: PropertyInput | Expr | ParamRef | PropertyValueInput | null,
  ): Traversal<"edges", M> {
    return this.push(
      Step.textSearchEdges(
        label,
        property,
        PropertyInput.from(queryText as never),
        StreamBound.from(k),
        tenantValue == null ? null : PropertyInput.from(tenantValue as never),
      ),
      "edges",
      this.mode,
    ) as Traversal<"edges", M>;
  }
  createIndexIfNotExists(spec: IndexSpec): Traversal<"terminal", "write"> {
    return this.push(Step.createIndex(spec, true), "terminal", "write") as Traversal<"terminal", "write">;
  }
  dropIndex(spec: IndexSpec): Traversal<"terminal", "write"> {
    return this.push(Step.dropIndex(spec), "terminal", "write") as Traversal<"terminal", "write">;
  }
  /** Read one retained operation in the request's storage scope. */
  getIndexOperation(operationId: string): Traversal<"terminal", M> {
    return this.push(Step.getIndexOperation(operationId), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  /** Convergently requeue a blocked operation at its exact checkpoint. */
  retryIndexOperation(operationId: string): Traversal<"terminal", "write"> {
    return this.push(Step.retryIndexOperation(operationId), "terminal", "write") as Traversal<"terminal", "write">;
  }
  /** Convert one constructing build into abort cleanup. */
  abortIndexOperation(operationId: string): Traversal<"terminal", "write"> {
    return this.push(Step.abortIndexOperation(operationId), "terminal", "write") as Traversal<"terminal", "write">;
  }
  createVectorIndexNodes(
    label: string,
    property: string,
    dimension: number,
    metric: VectorDistanceMetric,
    tenantProperty?: string | null,
  ): Traversal<"terminal", "write"> {
    return this.createIndexIfNotExists(IndexSpec.nodeVector(label, property, dimension, metric, tenantProperty));
  }
  createVectorIndexEdges(
    label: string,
    property: string,
    dimension: number,
    metric: VectorDistanceMetric,
    tenantProperty?: string | null,
  ): Traversal<"terminal", "write"> {
    return this.createIndexIfNotExists(IndexSpec.edgeVector(label, property, dimension, metric, tenantProperty));
  }
  createTextIndexNodes(label: string, property: string, tenantProperty?: string | null): Traversal<"terminal", "write"> {
    return this.createIndexIfNotExists(IndexSpec.nodeText(label, property, tenantProperty));
  }
  createTextIndexEdges(label: string, property: string, tenantProperty?: string | null): Traversal<"terminal", "write"> {
    return this.createIndexIfNotExists(IndexSpec.edgeText(label, property, tenantProperty));
  }
  out(label?: string | null): Traversal<"nodes", M> {
    return this.push(Step.out(label), "nodes", this.mode) as Traversal<"nodes", M>;
  }
  in(label?: string | null): Traversal<"nodes", M> {
    return this.push(Step.in(label), "nodes", this.mode) as Traversal<"nodes", M>;
  }
  both(label?: string | null): Traversal<"nodes", M> {
    return this.push(Step.both(label), "nodes", this.mode) as Traversal<"nodes", M>;
  }
  outE(label?: string | null): Traversal<"edges", M> {
    return this.push(Step.outE(label), "edges", this.mode) as Traversal<"edges", M>;
  }
  inE(label?: string | null): Traversal<"edges", M> {
    return this.push(Step.inE(label), "edges", this.mode) as Traversal<"edges", M>;
  }
  bothE(label?: string | null): Traversal<"edges", M> {
    return this.push(Step.bothE(label), "edges", this.mode) as Traversal<"edges", M>;
  }
  outN(): Traversal<"nodes", M> {
    return this.push(Step.outN(), "nodes", this.mode) as Traversal<"nodes", M>;
  }
  inN(): Traversal<"nodes", M> {
    return this.push(Step.inN(), "nodes", this.mode) as Traversal<"nodes", M>;
  }
  otherN(): Traversal<"nodes", M> {
    return this.push(Step.otherN(), "nodes", this.mode) as Traversal<"nodes", M>;
  }
  has(property: string, value: PropertyValueInput): Traversal<S, M> {
    return this.push(Step.has(property, value), this.state, this.mode) as Traversal<S, M>;
  }
  hasLabel(label: string): Traversal<S, M> {
    return this.push(Step.hasLabel(label), this.state, this.mode) as Traversal<S, M>;
  }
  hasKey(property: string): Traversal<S, M> {
    return this.push(Step.hasKey(property), this.state, this.mode) as Traversal<S, M>;
  }
  where(predicate: Predicate): Traversal<S, M> {
    return this.push(Step.where(predicate), this.state, this.mode) as Traversal<S, M>;
  }
  dedup(): Traversal<S, M> {
    return this.push(Step.dedup(), this.state, this.mode) as Traversal<S, M>;
  }
  within(name: string): Traversal<S, M> {
    return this.push(Step.within(name), this.state, this.mode) as Traversal<S, M>;
  }
  without(name: string): Traversal<S, M> {
    return this.push(Step.without(name), this.state, this.mode) as Traversal<S, M>;
  }
  edgeHas(property: string, value: PropertyInput | Expr | ParamRef | PropertyValueInput): Traversal<S, M> {
    return this.push(Step.edgeHas(property, PropertyInput.from(value as never)), this.state, this.mode) as Traversal<S, M>;
  }
  edgeHasLabel(label: string): Traversal<S, M> {
    return this.push(Step.edgeHasLabel(label), this.state, this.mode) as Traversal<S, M>;
  }
  limit(n: StreamBound | Expr | ParamRef | number | bigint): Traversal<S, M> {
    return this.push(Step.limit(StreamBound.from(n)), this.state, this.mode) as Traversal<S, M>;
  }
  skip(n: StreamBound | Expr | ParamRef | number | bigint): Traversal<S, M> {
    return this.push(Step.skip(StreamBound.from(n)), this.state, this.mode) as Traversal<S, M>;
  }
  range(start: StreamBound | Expr | ParamRef | number | bigint, end: StreamBound | Expr | ParamRef | number | bigint): Traversal<S, M> {
    return this.push(Step.range(StreamBound.from(start), StreamBound.from(end)), this.state, this.mode) as Traversal<S, M>;
  }
  as(name: string): Traversal<S, M> {
    return this.push(Step.as(name), this.state, this.mode) as Traversal<S, M>;
  }
  store(name: string): Traversal<S, M> {
    return this.push(Step.store(name), this.state, this.mode) as Traversal<S, M>;
  }
  select(name: string): Traversal<S, M> {
    return this.push(Step.select(name), this.state, this.mode) as Traversal<S, M>;
  }
  bind(name: string): Traversal<S, M> {
    return this.push(Step.bind(name), this.state, this.mode) as Traversal<S, M>;
  }
  inject(name: string): Traversal<"nodes", M> {
    return this.push(Step.inject(name), "nodes", this.mode) as Traversal<"nodes", M>;
  }
  count(): Traversal<"terminal", M> {
    return this.push(Step.count(), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  exists(): Traversal<"terminal", M> {
    return this.push(Step.exists(), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  id(): Traversal<"terminal", M> {
    return this.push(Step.id(), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  label(): Traversal<"terminal", M> {
    return this.push(Step.label(), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  values(properties: string[]): Traversal<"terminal", M> {
    return this.push(Step.values(properties), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  valueMap(properties?: string[] | null): Traversal<"terminal", M> {
    return this.push(Step.valueMap(properties), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  project(projections: ProjectionInput[]): Traversal<"terminal", M> {
    return this.push(Step.project(projections), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  projectBindings(projections: BindingProjection[]): Traversal<"terminal", M> {
    return this.push(Step.projectBindings(projections, false), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  projectDistinctBindings(projections: BindingProjection[]): Traversal<"terminal", M> {
    return this.push(Step.projectBindings(projections, true), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  edgeProperties(): Traversal<"terminal", M> {
    return this.push(Step.edgeProperties(), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  orderBy(property: string, order: Order): Traversal<S, M> {
    return this.push(Step.orderBy(property, order), this.state, this.mode) as Traversal<S, M>;
  }
  orderByMultiple(orderings: [string, Order][]): Traversal<S, M> {
    return this.push(Step.orderByMultiple(orderings), this.state, this.mode) as Traversal<S, M>;
  }
  repeat(config: RepeatConfig): Traversal<S, M> {
    return this.push(Step.repeat(config), this.state, this.mode) as Traversal<S, M>;
  }
  union(traversals: SubTraversal[]): Traversal<S, M> {
    return this.push(Step.union(traversals), this.state, this.mode) as Traversal<S, M>;
  }
  choose(condition: Predicate, thenTraversal: SubTraversal, elseTraversal?: SubTraversal | null): Traversal<S, M> {
    return this.push(Step.choose(condition, thenTraversal, elseTraversal), this.state, this.mode) as Traversal<S, M>;
  }
  coalesce(traversals: SubTraversal[]): Traversal<S, M> {
    return this.push(Step.coalesce(traversals), this.state, this.mode) as Traversal<S, M>;
  }
  optional(traversal: SubTraversal): Traversal<S, M> {
    return this.push(Step.optional(traversal), this.state, this.mode) as Traversal<S, M>;
  }
  group(property: string): Traversal<"terminal", M> {
    return this.push(Step.group(property), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  groupCount(property: string): Traversal<"terminal", M> {
    return this.push(Step.groupCount(property), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  aggregateBy(fn: AggregateFunction, property: string): Traversal<"terminal", M> {
    return this.push(Step.aggregateBy(fn, property), "terminal", this.mode) as Traversal<"terminal", M>;
  }
  fold(): Traversal<S, M> {
    return this.push(Step.fold(), this.state, this.mode) as Traversal<S, M>;
  }
  unfold(): Traversal<S, M> {
    return this.push(Step.unfold(), this.state, this.mode) as Traversal<S, M>;
  }
  path(): Traversal<S, M> {
    return this.push(Step.path(), this.state, this.mode) as Traversal<S, M>;
  }
  simplePath(): Traversal<S, M> {
    return this.push(Step.simplePath(), this.state, this.mode) as Traversal<S, M>;
  }
  withSack(initial: PropertyValueInput): Traversal<S, M> {
    return this.push(Step.withSack(initial), this.state, this.mode) as Traversal<S, M>;
  }
  sackSet(property: string): Traversal<S, M> {
    return this.push(Step.sackSet(property), this.state, this.mode) as Traversal<S, M>;
  }
  sackAdd(property: string): Traversal<S, M> {
    return this.push(Step.sackAdd(property), this.state, this.mode) as Traversal<S, M>;
  }
  sackGet(): Traversal<S, M> {
    return this.push(Step.sackGet(), this.state, this.mode) as Traversal<S, M>;
  }
  addN(label: string, properties: PropEntries = []): Traversal<"nodes", "write"> {
    return this.push(Step.addN(label, propertyEntries(properties)), "nodes", "write") as Traversal<"nodes", "write">;
  }
  addE(label: string, to: NodeRef | NodeId | NodeId[] | string, properties: PropEntries = []): Traversal<"nodes", "write"> {
    return this.push(Step.addE(label, NodeRef.from(to), propertyEntries(properties)), "nodes", "write") as Traversal<"nodes", "write">;
  }
  setProperty(name: string, value: PropertyInput | Expr | ParamRef | PropertyValueInput): Traversal<"nodes", "write"> {
    return this.push(Step.setProperty(name, PropertyInput.from(value as never)), "nodes", "write") as Traversal<"nodes", "write">;
  }
  removeProperty(name: string): Traversal<"nodes", "write"> {
    return this.push(Step.removeProperty(name), "nodes", "write") as Traversal<"nodes", "write">;
  }
  drop(): Traversal<"nodes", "write"> {
    return this.push(Step.drop(), "nodes", "write") as Traversal<"nodes", "write">;
  }
  dropEdge(to: NodeRef | NodeId | NodeId[] | string): Traversal<"nodes", "write"> {
    return this.push(Step.dropEdge(NodeRef.from(to)), "nodes", "write") as Traversal<"nodes", "write">;
  }
  dropEdgeLabeled(to: NodeRef | NodeId | NodeId[] | string, label: string): Traversal<"nodes", "write"> {
    return this.push(Step.dropEdgeLabeled(NodeRef.from(to), label), "nodes", "write") as Traversal<"nodes", "write">;
  }
  dropEdgeById(edges: EdgeRef | EdgeId | EdgeId[]): Traversal<"nodes", "write"> {
    return this.push(Step.dropEdgeById(EdgeRef.from(edges)), "nodes", "write") as Traversal<"nodes", "write">;
  }
}

export function g(): Traversal<"empty", "read"> {
  return Traversal.new();
}

export class SubTraversal implements Encodable {
  constructor(readonly steps: Step[] = []) {}
  static new(): SubTraversal {
    return new SubTraversal();
  }
  private push(step: Step): SubTraversal {
    return new SubTraversal([...this.steps, step]);
  }
  out(label?: string | null): SubTraversal {
    return this.push(Step.out(label));
  }
  in(label?: string | null): SubTraversal {
    return this.push(Step.in(label));
  }
  both(label?: string | null): SubTraversal {
    return this.push(Step.both(label));
  }
  outE(label?: string | null): SubTraversal {
    return this.push(Step.outE(label));
  }
  inE(label?: string | null): SubTraversal {
    return this.push(Step.inE(label));
  }
  bothE(label?: string | null): SubTraversal {
    return this.push(Step.bothE(label));
  }
  outN(): SubTraversal {
    return this.push(Step.outN());
  }
  inN(): SubTraversal {
    return this.push(Step.inN());
  }
  otherN(): SubTraversal {
    return this.push(Step.otherN());
  }
  has(property: string, value: PropertyValueInput): SubTraversal {
    return this.push(Step.has(property, value));
  }
  hasLabel(label: string): SubTraversal {
    return this.push(Step.hasLabel(label));
  }
  hasKey(property: string): SubTraversal {
    return this.push(Step.hasKey(property));
  }
  where(predicate: Predicate): SubTraversal {
    return this.push(Step.where(predicate));
  }
  dedup(): SubTraversal {
    return this.push(Step.dedup());
  }
  within(name: string): SubTraversal {
    return this.push(Step.within(name));
  }
  without(name: string): SubTraversal {
    return this.push(Step.without(name));
  }
  edgeHas(property: string, value: PropertyInput | Expr | ParamRef | PropertyValueInput): SubTraversal {
    return this.push(Step.edgeHas(property, PropertyInput.from(value as never)));
  }
  edgeHasLabel(label: string): SubTraversal {
    return this.push(Step.edgeHasLabel(label));
  }
  limit(n: StreamBound | Expr | ParamRef | number | bigint): SubTraversal {
    return this.push(Step.limit(StreamBound.from(n)));
  }
  skip(n: StreamBound | Expr | ParamRef | number | bigint): SubTraversal {
    return this.push(Step.skip(StreamBound.from(n)));
  }
  range(start: StreamBound | Expr | ParamRef | number | bigint, end: StreamBound | Expr | ParamRef | number | bigint): SubTraversal {
    return this.push(Step.range(StreamBound.from(start), StreamBound.from(end)));
  }
  as(name: string): SubTraversal {
    return this.push(Step.as(name));
  }
  store(name: string): SubTraversal {
    return this.push(Step.store(name));
  }
  select(name: string): SubTraversal {
    return this.push(Step.select(name));
  }
  bind(name: string): SubTraversal {
    return this.push(Step.bind(name));
  }
  orderBy(property: string, order: Order): SubTraversal {
    return this.push(Step.orderBy(property, order));
  }
  orderByMultiple(orderings: [string, Order][]): SubTraversal {
    return this.push(Step.orderByMultiple(orderings));
  }
  path(): SubTraversal {
    return this.push(Step.path());
  }
  simplePath(): SubTraversal {
    return this.push(Step.simplePath());
  }
  toJSON(): JsonValue {
    return { root: stepsToAst(this.steps, "context") };
  }
}

export function sub(): SubTraversal {
  return SubTraversal.new();
}

export class BatchCondition implements Encodable {
  private constructor(
    readonly variant: string,
    readonly payload?: unknown,
  ) {}
  static varNotEmpty(name: string): BatchCondition {
    return new BatchCondition("VarNotEmpty", name);
  }
  static varEmpty(name: string): BatchCondition {
    return new BatchCondition("VarEmpty", name);
  }
  static varMinSize(name: string, size: number): BatchCondition {
    return new BatchCondition("VarMinSize", [name, size]);
  }
  static prevNotEmpty(): BatchCondition {
    return new BatchCondition("PrevNotEmpty");
  }
  toJSON(): JsonValue {
    return this.variant === "PrevNotEmpty"
      ? astUnit("PrevNotEmpty")
      : this.variant === "VarMinSize"
        ? astNewtype("VarMinSize", this.payload as unknown[])
        : astNewtype(this.variant, this.payload);
  }
}

export class NamedQuery implements Encodable {
  constructor(
    readonly name: string | null,
    readonly root: JsonValue,
    readonly condition: BatchCondition | null,
  ) {}
  toJSON(): JsonValue {
    return { name: this.name ?? undefined, root: this.root, condition: this.condition ?? undefined };
  }
}

export class BatchEntry implements Encodable {
  private constructor(
    readonly variant: "Query" | "ForEach",
    readonly payload: unknown,
  ) {}
  static query(query: NamedQuery): BatchEntry {
    return new BatchEntry("Query", query);
  }
  static forEach(paramName: string, body: readonly BatchEntry[]): BatchEntry {
    return new BatchEntry("ForEach", { param: paramName, body });
  }
  toJSON(): JsonValue {
    return this.variant === "Query" ? astNewtype("Query", this.payload) : astTag("ForEach", this.payload as Record<string, unknown>);
  }
}

export class ReadBatch implements Encodable {
  declare private readonly __readBatchBrand: void;
  readonly queries: readonly BatchEntry[];
  readonly returns: readonly string[];
  private constructor(queries: readonly BatchEntry[] = [], returns: readonly string[] = []) {
    this.queries = Object.freeze([...queries]);
    this.returns = Object.freeze([...returns]);
    Object.freeze(this);
  }
  static new(): ReadBatch {
    return new ReadBatch();
  }
  varAs<S extends TraversalState>(name: string, traversal: Traversal<S, "read">): ReadBatch {
    if (traversal.mode !== "read") throw new TypeError("ReadBatch.varAs only accepts read-only traversals");
    return new ReadBatch([...this.queries, BatchEntry.query(new NamedQuery(name, traversal.intoAst(), null))], this.returns);
  }
  varAsIf<S extends TraversalState>(name: string, condition: BatchCondition, traversal: Traversal<S, "read">): ReadBatch {
    if (traversal.mode !== "read") throw new TypeError("ReadBatch.varAsIf only accepts read-only traversals");
    return new ReadBatch([...this.queries, BatchEntry.query(new NamedQuery(name, traversal.intoAst(), condition))], this.returns);
  }
  forEachParam(paramName: string, body: ReadBatch): ReadBatch {
    return new ReadBatch([...this.queries, BatchEntry.forEach(paramName, body.queries)], this.returns);
  }
  returning(vars: Iterable<string>): ReadBatch {
    return new ReadBatch(this.queries, Array.from(vars));
  }
  toJSON(): JsonValue {
    return { entries: this.queries, returns: this.returns };
  }
  toJsonBytes(): Uint8Array {
    return new TextEncoder().encode(this.toJsonString());
  }
  toJsonString(): string {
    return stringifyJson(this);
  }
  toQueryRequest(options?: QueryOptions): QueryRequest;
  toQueryRequest(): QueryRequest;
  toQueryRequest<T extends ParamShape>(params: DefinedParams<T>, values: ParamInputs<T>, options?: QueryOptions): QueryRequest;
  toQueryRequest<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | QueryOptions,
    values?: ParamInputs<T>,
    options?: QueryOptions,
  ): QueryRequest {
    return buildQueryRequest(QueryRequest.read(this), paramsOrOptions, values, options);
  }
  toQueryJson(options?: QueryOptions): string;
  toQueryJson(): string;
  toQueryJson<T extends ParamShape>(params: DefinedParams<T>, values: ParamInputs<T>, options?: QueryOptions): string;
  toQueryJson<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | QueryOptions,
    values?: ParamInputs<T>,
    options?: QueryOptions,
  ): string {
    return this.toQueryRequest(paramsOrOptions as DefinedParams<T>, values as ParamInputs<T>, options).toJsonString();
  }
  toQueryBytes(options?: QueryOptions): Uint8Array;
  toQueryBytes(): Uint8Array;
  toQueryBytes<T extends ParamShape>(params: DefinedParams<T>, values: ParamInputs<T>, options?: QueryOptions): Uint8Array;
  toQueryBytes<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | QueryOptions,
    values?: ParamInputs<T>,
    options?: QueryOptions,
  ): Uint8Array {
    return this.toQueryRequest(paramsOrOptions as DefinedParams<T>, values as ParamInputs<T>, options).toJsonBytes();
  }
}

export class WriteBatch implements Encodable {
  declare private readonly __writeBatchBrand: void;
  readonly queries: readonly BatchEntry[];
  readonly returns: readonly string[];
  private constructor(queries: readonly BatchEntry[] = [], returns: readonly string[] = []) {
    this.queries = Object.freeze([...queries]);
    this.returns = Object.freeze([...returns]);
    Object.freeze(this);
  }
  static new(): WriteBatch {
    return new WriteBatch();
  }
  varAs<S extends TraversalState, M extends MutationMode>(name: string, traversal: Traversal<S, M>): WriteBatch {
    return new WriteBatch([...this.queries, BatchEntry.query(new NamedQuery(name, traversal.intoAst(), null))], this.returns);
  }
  varAsIf<S extends TraversalState, M extends MutationMode>(
    name: string,
    condition: BatchCondition,
    traversal: Traversal<S, M>,
  ): WriteBatch {
    return new WriteBatch([...this.queries, BatchEntry.query(new NamedQuery(name, traversal.intoAst(), condition))], this.returns);
  }
  forEachParam(paramName: string, body: WriteBatch): WriteBatch {
    return new WriteBatch([...this.queries, BatchEntry.forEach(paramName, body.queries)], this.returns);
  }
  returning(vars: Iterable<string>): WriteBatch {
    return new WriteBatch(this.queries, Array.from(vars));
  }
  toJSON(): JsonValue {
    return { entries: this.queries, returns: this.returns };
  }
  toJsonBytes(): Uint8Array {
    return new TextEncoder().encode(this.toJsonString());
  }
  toJsonString(): string {
    return stringifyJson(this);
  }
  toQueryRequest(options?: QueryOptions): QueryRequest;
  toQueryRequest(): QueryRequest;
  toQueryRequest<T extends ParamShape>(params: DefinedParams<T>, values: ParamInputs<T>, options?: QueryOptions): QueryRequest;
  toQueryRequest<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | QueryOptions,
    values?: ParamInputs<T>,
    options?: QueryOptions,
  ): QueryRequest {
    return buildQueryRequest(QueryRequest.write(this), paramsOrOptions, values, options);
  }
  toQueryJson(options?: QueryOptions): string;
  toQueryJson(): string;
  toQueryJson<T extends ParamShape>(params: DefinedParams<T>, values: ParamInputs<T>, options?: QueryOptions): string;
  toQueryJson<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | QueryOptions,
    values?: ParamInputs<T>,
    options?: QueryOptions,
  ): string {
    return this.toQueryRequest(paramsOrOptions as DefinedParams<T>, values as ParamInputs<T>, options).toJsonString();
  }
  toQueryBytes(options?: QueryOptions): Uint8Array;
  toQueryBytes(): Uint8Array;
  toQueryBytes<T extends ParamShape>(params: DefinedParams<T>, values: ParamInputs<T>, options?: QueryOptions): Uint8Array;
  toQueryBytes<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | QueryOptions,
    values?: ParamInputs<T>,
    options?: QueryOptions,
  ): Uint8Array {
    return this.toQueryRequest(paramsOrOptions as DefinedParams<T>, values as ParamInputs<T>, options).toJsonBytes();
  }
}

export function readBatch(): ReadBatch {
  return ReadBatch.new();
}
export function writeBatch(): WriteBatch {
  return WriteBatch.new();
}

type QueryParamTypeState =
  | { readonly variant: "Bool" | "I64" | "F64" | "F32" | "String" | "DateTime" | "Bytes" | "Value" | "Object" }
  | { readonly variant: "Array"; readonly inner: QueryParamType };

export class QueryParamType implements Encodable {
  private constructor(private readonly state: QueryParamTypeState) {
    Object.freeze(state);
    Object.freeze(this);
  }
  get variant(): QueryParamTypeState["variant"] {
    return this.state.variant;
  }
  get inner(): QueryParamType | undefined {
    return this.state.variant === "Array" ? this.state.inner : undefined;
  }
  static bool(): QueryParamType {
    return new QueryParamType({ variant: "Bool" });
  }
  static i64(): QueryParamType {
    return new QueryParamType({ variant: "I64" });
  }
  static f64(): QueryParamType {
    return new QueryParamType({ variant: "F64" });
  }
  static f32(): QueryParamType {
    return new QueryParamType({ variant: "F32" });
  }
  static string(): QueryParamType {
    return new QueryParamType({ variant: "String" });
  }
  static dateTime(): QueryParamType {
    return new QueryParamType({ variant: "DateTime" });
  }
  static bytes(): QueryParamType {
    return new QueryParamType({ variant: "Bytes" });
  }
  static value(): QueryParamType {
    return new QueryParamType({ variant: "Value" });
  }
  static object(): QueryParamType {
    return new QueryParamType({ variant: "Object" });
  }
  static array(inner: QueryParamType): QueryParamType {
    return new QueryParamType({ variant: "Array", inner });
  }
  toJSON(): JsonValue {
    return this.state.variant === "Array" ? astNewtype("Array", this.state.inner) : astUnit(this.state.variant);
  }
}

type ParamKind = "Bool" | "I64" | "F64" | "F32" | "String" | "DateTime" | "Bytes" | "Value" | "Object" | "Array";

export type ParamSchemaInput<T> = T extends ParamSchema<infer Input> ? Input : never;
const PARAMS_METADATA: unique symbol = Symbol("@helixdb/enterprise-ql/params-metadata");

type ParamSchemaState =
  | { readonly kind: "Bool" | "I64" | "F64" | "F32" | "String" | "DateTime" | "Bytes" | "Value" }
  | { readonly kind: "Object"; readonly objectInner: ParamSchema }
  | { readonly kind: "Array"; readonly inner: ParamSchema };

export class ParamSchema<Input = unknown> implements Encodable {
  declare readonly __input?: Input;
  private constructor(private readonly state: ParamSchemaState) {
    Object.freeze(state);
    Object.freeze(this);
  }
  get kind(): ParamKind {
    return this.state.kind;
  }
  get inner(): ParamSchema | undefined {
    return this.state.kind === "Array" ? this.state.inner : undefined;
  }
  get objectInner(): ParamSchema | undefined {
    return this.state.kind === "Object" ? this.state.objectInner : undefined;
  }
  static bool(): ParamSchema<boolean> {
    return new ParamSchema({ kind: "Bool" });
  }
  static i64(): ParamSchema<number | bigint> {
    return new ParamSchema({ kind: "I64" });
  }
  static f64(): ParamSchema<number> {
    return new ParamSchema({ kind: "F64" });
  }
  static f32(): ParamSchema<number> {
    return new ParamSchema({ kind: "F32" });
  }
  static string(): ParamSchema<string> {
    return new ParamSchema({ kind: "String" });
  }
  static dateTime(): ParamSchema<DateTime | string | number | bigint> {
    return new ParamSchema({ kind: "DateTime" });
  }
  static bytes(): ParamSchema<Uint8Array | number[]> {
    return new ParamSchema({ kind: "Bytes" });
  }
  static value(): ParamSchema<PropertyValueInput> {
    return new ParamSchema({ kind: "Value" });
  }
  static object<Inner extends ParamSchema>(inner: Inner): ParamSchema<Record<string, ParamSchemaInput<Inner>>> {
    return new ParamSchema({ kind: "Object", objectInner: inner });
  }
  static array<Inner extends ParamSchema>(inner: Inner): ParamSchema<ParamSchemaInput<Inner>[]> {
    return new ParamSchema({ kind: "Array", inner });
  }
  toParamType(): QueryParamType {
    switch (this.state.kind) {
      case "Bool":
        return QueryParamType.bool();
      case "I64":
        return QueryParamType.i64();
      case "F64":
        return QueryParamType.f64();
      case "F32":
        return QueryParamType.f32();
      case "String":
        return QueryParamType.string();
      case "DateTime":
        return QueryParamType.dateTime();
      case "Bytes":
        return QueryParamType.bytes();
      case "Value":
        return QueryParamType.value();
      case "Object":
        return QueryParamType.object();
      case "Array":
        return QueryParamType.array(this.state.inner.toParamType());
    }
  }
  toJSON(): JsonValue {
    return this.toParamType().toJSON();
  }
}

export const param = {
  bool: ParamSchema.bool,
  i64: ParamSchema.i64,
  f64: ParamSchema.f64,
  f32: ParamSchema.f32,
  string: ParamSchema.string,
  dateTime: ParamSchema.dateTime,
  bytes: ParamSchema.bytes,
  value: ParamSchema.value,
  object: <Inner extends ParamSchema = ParamSchema<PropertyValueInput>>(
    inner: Inner = ParamSchema.value() as Inner,
  ): ParamSchema<Record<string, ParamSchemaInput<Inner>>> => ParamSchema.object(inner),
  array: ParamSchema.array,
};

export class ParamRef<Input = unknown> implements Encodable {
  declare readonly __input?: Input;
  constructor(
    readonly name: string,
    readonly schema: ParamSchema<Input>,
  ) {}
  toExpr(): Expr {
    return Expr.param(this.name);
  }
  toJSON(): JsonValue {
    return Expr.param(this.name).toJSON();
  }
}

export type ParamShape = Record<string, ParamSchema>;
export type ParamRefs<T extends ParamShape> = { readonly [K in keyof T]: ParamRef<ParamSchemaInput<T[K]>> };
export type ParamInputs<T extends ParamShape> = { readonly [K in keyof T]: ParamSchemaInput<T[K]> };
type ParamsMetadata<T extends ParamShape> = { readonly schema: T };
export type DefinedParams<T extends ParamShape> = ParamRefs<T> & { readonly [PARAMS_METADATA]: ParamsMetadata<T> };

export function defineParams<T extends ParamShape>(schema: T): DefinedParams<T> {
  const refs: Record<string, ParamRef> = Object.create(null);
  for (const [name, paramSchema] of Object.entries(schema)) refs[name] = new ParamRef(name, paramSchema);
  Object.defineProperty(refs, PARAMS_METADATA, { value: { schema }, enumerable: false });
  return refs as DefinedParams<T>;
}

function schemaForParams<T extends ParamShape>(params: DefinedParams<T>): T {
  const metadata = params[PARAMS_METADATA];
  if (!metadata) throw new TypeError("invalid parameter definition; use defineParams(...)");
  return metadata.schema;
}

function convertInputFromSchema<T extends ParamShape>(schema: T, input: ParamInputs<T>): Record<string, JsonValue> {
  const out: Record<string, JsonValue> = {};
  for (const [name, paramSchema] of Object.entries(schema)) {
    if (!(name in input)) throw new TypeError(`missing required parameter: ${name}`);
    out[name] = convertParamValue(paramSchema, input[name], name);
  }
  return out;
}

function convertParamValue(schema: ParamSchema, value: unknown, path: string): JsonValue {
  switch (schema.kind) {
    case "Bool":
      if (typeof value !== "boolean") throw new TypeError(`parameter '${path}' must be boolean`);
      return value;
    case "I64":
      return intToJson(value as number | bigint);
    case "F64":
      if (typeof value !== "number") throw new TypeError(`parameter '${path}' must be number`);
      return finiteNumber(value, path);
    case "F32":
      if (typeof value !== "number") throw new TypeError(`parameter '${path}' must be number`);
      return normalizeF32(value, path);
    case "String":
      if (typeof value !== "string") throw new TypeError(`parameter '${path}' must be string`);
      return value;
    case "DateTime": {
      const dt =
        value instanceof DateTime
          ? value
          : typeof value === "string"
            ? DateTime.parseRfc3339(value)
            : DateTime.fromMillis(value as number | bigint);
      return dateTimeToRfc3339(dt, path);
    }
    case "Bytes":
      throw QueryError.unsupportedBytes(path);
    case "Value":
      return queryValueFromPropertyValue(PropertyValue.from(value as PropertyValueInput), path);
    case "Object": {
      if (typeof value !== "object" || value === null || Array.isArray(value)) throw new TypeError(`parameter '${path}' must be object`);
      const out: Record<string, JsonValue> = {};
      for (const [key, entry] of Object.entries(value as Record<string, unknown>))
        out[key] = convertParamValue(schema.objectInner ?? param.value(), entry, `${path}.${key}`);
      return out;
    }
    case "Array": {
      if (!Array.isArray(value)) throw new TypeError(`parameter '${path}' must be array`);
      return value.map((entry, index) => convertParamValue(schema.inner!, entry, `${path}[${index}]`));
    }
  }
}

function finiteNumber(value: number, path: string): number {
  if (!Number.isFinite(value)) throw new TypeError(`parameter '${path}' must be finite`);
  return value;
}

function normalizeF32(value: number, path: string): number {
  const normalized = Math.fround(finiteNumber(value, path));
  if (!Number.isFinite(normalized)) throw new TypeError(`parameter '${path}' is outside the f32 range`);
  return normalized;
}

function validateJsonValue(value: JsonValue, path: string): JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "bigint") return value;
  if (typeof value === "number") return finiteNumber(value, path);
  if (Array.isArray(value)) return value.map((entry, index) => validateJsonValue(entry, `${path}[${index}]`));
  if (typeof value !== "object") throw new TypeError(`parameter '${path}' must be JSON-compatible`);
  const out: Record<string, JsonValue> = {};
  for (const [name, entry] of Object.entries(value as Record<string, JsonValue>)) out[name] = validateJsonValue(entry, `${path}.${name}`);
  return out;
}

function normalizeTypedQueryValue(type: QueryParamType, value: JsonValue, path: string): JsonValue {
  switch (type.variant) {
    case "Bool":
      if (typeof value !== "boolean") throw new TypeError(`parameter '${path}' must be boolean`);
      return value;
    case "I64":
      if (typeof value !== "number" && typeof value !== "bigint") throw new TypeError(`parameter '${path}' must be an integer`);
      return intToJson(value);
    case "F64":
      if (typeof value !== "number") throw new TypeError(`parameter '${path}' must be number`);
      return finiteNumber(value, path);
    case "F32":
      if (typeof value !== "number") throw new TypeError(`parameter '${path}' must be number`);
      return normalizeF32(value, path);
    case "String":
      if (typeof value !== "string") throw new TypeError(`parameter '${path}' must be string`);
      return value;
    case "DateTime":
      if (typeof value !== "string") throw new TypeError(`parameter '${path}' must be an RFC3339 string`);
      return dateTimeToRfc3339(DateTime.parseRfc3339(value), path);
    case "Bytes":
      throw QueryError.unsupportedBytes(path);
    case "Value":
      return validateJsonValue(value, path);
    case "Object": {
      if (typeof value !== "object" || value === null || Array.isArray(value)) throw new TypeError(`parameter '${path}' must be object`);
      return validateJsonValue(value, path);
    }
    case "Array":
      if (!Array.isArray(value)) throw new TypeError(`parameter '${path}' must be array`);
      return value.map((entry, index) => normalizeTypedQueryValue(type.inner!, entry, `${path}[${index}]`));
  }
}

function queryValueFromPropertyValue(value: PropertyValue, path: string): JsonValue {
  switch (value.variant) {
    case "Null":
      return null;
    case "Bool":
      return value.payload as boolean;
    case "I64":
      return value.payload as number | bigint;
    case "DateTime":
      return dateTimeToRfc3339(DateTime.fromMillis(value.payload as number | bigint), path);
    case "F64":
    case "F32":
      return value.payload as number;
    case "String":
      return value.payload as string;
    case "Bytes":
      throw QueryError.unsupportedBytes(path);
    case "I64Array":
    case "F64Array":
    case "F32Array":
    case "StringArray":
      return value.payload as JsonValue;
    case "Array":
      return (value.payload as PropertyValue[]).map((entry, index) => queryValueFromPropertyValue(entry, `${path}[${index}]`));
    case "Object": {
      const out: Record<string, JsonValue> = {};
      for (const [key, entry] of Object.entries(value.payload as Record<string, PropertyValue>))
        out[key] = queryValueFromPropertyValue(entry, `${path}.${key}`);
      return out;
    }
    default:
      throw new TypeError(`unsupported property value variant: ${value.variant}`);
  }
}

export enum QueryRequestType {
  Read = "read",
  Write = "write",
}
export type QueryValue = JsonValue;
export const QueryValue = {
  null: (): JsonValue => null,
  bool: (value: boolean): JsonValue => value,
  i64: (value: number | bigint): JsonValue => intToJson(value),
  f64: (value: number): JsonValue => finiteNumber(value, "value"),
  f32: (value: number): JsonValue => normalizeF32(value, "value"),
  string: (value: string): JsonValue => value,
  array: (values: JsonValue[]): JsonValue => values,
  object: (values: Record<string, JsonValue>): JsonValue => values,
};
export type BatchQuery = ReadBatch | WriteBatch;
export type QueryOptions = { queryName?: string | null };

type QueryRequestState =
  | { readonly requestType: QueryRequestType.Read; readonly query: ReadBatch }
  | { readonly requestType: QueryRequestType.Write; readonly query: WriteBatch };
type QueryParameterState =
  | { readonly mode: "untyped"; readonly values: Record<string, JsonValue> }
  | {
      readonly mode: "typed";
      readonly values: Record<string, JsonValue>;
      readonly types: Record<string, QueryParamType>;
    };

export class QueryRequest implements Encodable {
  private queryName: string | null = null;
  private parameterState?: QueryParameterState;
  private constructor(
    private readonly state: QueryRequestState,
    queryName: string | null = null,
  ) {
    this.queryName = queryName;
  }
  get requestType(): QueryRequestType {
    return this.state.requestType;
  }
  get query(): BatchQuery {
    return this.state.query;
  }
  static read(query: ReadBatch, queryName: string | null = null): QueryRequest {
    return new QueryRequest({ requestType: QueryRequestType.Read, query }, queryName);
  }
  static write(query: WriteBatch, queryName: string | null = null): QueryRequest {
    return new QueryRequest({ requestType: QueryRequestType.Write, query }, queryName);
  }
  insertUntypedParameter(name: string, value: JsonValue): void {
    validateParameterName(name);
    if (this.parameterState?.mode === "typed") throw new TypeError("typed and untyped query parameters cannot be mixed");
    const values = this.parameterState?.values ?? {};
    if (Object.hasOwn(values, name)) throw new TypeError(`duplicate parameter: ${name}`);
    values[name] = validateJsonValue(value, name);
    this.parameterState = { mode: "untyped", values };
  }
  insertTypedParameter(name: string, type: QueryParamType, value: JsonValue): void {
    validateParameterName(name);
    if (this.parameterState?.mode === "untyped") throw new TypeError("typed and untyped query parameters cannot be mixed");
    const values = this.parameterState?.values ?? {};
    const types = this.parameterState?.types ?? {};
    if (Object.hasOwn(values, name)) throw new TypeError(`duplicate parameter: ${name}`);
    const normalized = normalizeTypedQueryValue(type, value, name);
    values[name] = normalized;
    types[name] = type;
    this.parameterState = { mode: "typed", values, types };
  }
  withUntypedParameter(name: string, value: JsonValue): QueryRequest {
    this.insertUntypedParameter(name, value);
    return this;
  }
  withTypedParameter(name: string, type: QueryParamType, value: JsonValue): QueryRequest {
    this.insertTypedParameter(name, type, value);
    return this;
  }
  setQueryName(name: string): void {
    this.queryName = name;
  }
  clearQueryName(): void {
    this.queryName = null;
  }
  withQueryName(name: string): QueryRequest {
    this.setQueryName(name);
    return this;
  }
  toJSON(): JsonValue {
    const parameters = this.parameterState?.values;
    const parameterTypes = this.parameterState?.mode === "typed" ? this.parameterState.types : undefined;
    return {
      request_type: this.state.requestType,
      query_name: this.queryName ?? null,
      query: this.state.requestType === QueryRequestType.Read ? { read: this.state.query } : { write: this.state.query },
      parameters,
      parameter_types: parameterTypes,
    };
  }
  toJsonBytes(): Uint8Array {
    return new TextEncoder().encode(this.toJsonString());
  }
  toJsonString(): string {
    return stringifyJson(this);
  }
}

function validateParameterName(name: string): void {
  if (name.length === 0) throw new TypeError("parameter name must not be empty");
}

function rejectUnknownParameters(input: Record<string, unknown>, expected: string[]): void {
  const allowed = new Set(expected);
  for (const key of Object.keys(input)) {
    if (!allowed.has(key)) throw new TypeError(`unknown parameter: ${key}`);
  }
}

function addQueryParameters<T extends ParamShape>(request: QueryRequest, params?: DefinedParams<T>, values?: ParamInputs<T>): QueryRequest {
  if (!params) return request;
  if (values === undefined) throw new TypeError("query parameter values are required when a parameter schema is provided");

  const schema = schemaForParams(params);
  rejectUnknownParameters(values as Record<string, unknown>, Object.keys(schema));
  const converted = convertInputFromSchema(schema, values);
  for (const [name, paramSchema] of Object.entries(schema)) {
    request.insertTypedParameter(name, paramSchema.toParamType(), converted[name]!);
  }
  return request;
}

function isDefinedParams(value: unknown): value is DefinedParams<ParamShape> {
  return typeof value === "object" && value !== null && PARAMS_METADATA in value;
}

function applyQueryOptions(request: QueryRequest, options?: QueryOptions): QueryRequest {
  if (!options || !("queryName" in options)) return request;
  if (options.queryName === null || options.queryName === undefined) {
    request.clearQueryName();
  } else {
    request.setQueryName(options.queryName);
  }
  return request;
}

function buildQueryRequest<T extends ParamShape>(
  request: QueryRequest,
  paramsOrOptions?: DefinedParams<T> | QueryOptions,
  values?: ParamInputs<T>,
  options?: QueryOptions,
): QueryRequest {
  if (isDefinedParams(paramsOrOptions)) {
    return applyQueryOptions(addQueryParameters(request, paramsOrOptions, values), options);
  }
  if (values !== undefined) throw new TypeError("query parameter values require a parameter schema");
  return applyQueryOptions(request, paramsOrOptions);
}

export const prelude = {
  g,
  sub,
  readBatch,
  writeBatch,
  defineParams,
  param,
  DateTime,
  QueryError,
  QueryRequest,
  QueryRequestType,
  QueryValue,
  PropertyValue,
  PropertyInput,
  NodeRef,
  EdgeRef,
  Expr,
  WhenThen,
  StreamBound,
  CompareOp,
  Predicate,
  SourcePredicate,
  PropertyProjection,
  ExprProjection,
  Projection,
  BindingProjection,
  BindingTarget,
  Order,
  ShortestPathDirection,
  EmitBehavior,
  AggregateFunction,
  RepeatConfig,
  IndexSpec,
  RangeIndexDirection,
  VectorDistanceMetric,
  Traversal,
  SubTraversal,
  ReadBatch,
  WriteBatch,
  BatchCondition,
  NamedQuery,
  BatchEntry,
  QueryParamType,
};
