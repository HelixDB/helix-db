import { readFile, writeFile } from "node:fs/promises";

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

function tuple(name: string, values: unknown[]): JsonValue {
  return { [name]: values.map((value) => encode(value)) };
}

function struct(name: string, fields: Record<string, unknown>): JsonValue {
  const out: Record<string, JsonValue> = {};
  for (const [key, value] of Object.entries(fields)) {
    if (value !== undefined) out[key] = encode(value);
  }
  return { [name]: out };
}

export function stringifyJson(value: unknown, pretty = false): string {
  const encoded = encode(value);
  return stringifyEncoded(encoded, pretty ? 0 : undefined);
}

export function parseJsonStructural(json: string): unknown {
  return JSON.parse(quoteUnsafeIntegerTokens(json));
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

export class DynamicQueryError extends Error {
  readonly kind: "Serialize" | "Utf8" | "UnsupportedBytesParameter" | "InvalidDateTimeParameter";
  readonly path?: string;
  readonly millis?: bigint;

  private constructor(kind: DynamicQueryError["kind"], message: string, path?: string, millis?: bigint) {
    super(message);
    this.name = "DynamicQueryError";
    this.kind = kind;
    this.path = path;
    this.millis = millis;
  }

  static serialize(message: string): DynamicQueryError {
    return new DynamicQueryError("Serialize", `json serialization error: ${message}`);
  }

  static utf8(message: string): DynamicQueryError {
    return new DynamicQueryError("Utf8", `utf8 conversion error: ${message}`);
  }

  static unsupportedBytes(path: string): DynamicQueryError {
    return new DynamicQueryError(
      "UnsupportedBytesParameter",
      `parameter '${path}' uses bytes, which the dynamic query JSON route cannot represent`,
      path,
    );
  }

  static invalidDatetime(path: string, millis: bigint): DynamicQueryError {
    return new DynamicQueryError(
      "InvalidDateTimeParameter",
      `parameter '${path}' uses datetime millis '${millis}', which cannot be rendered as RFC3339`,
      path,
      millis,
    );
  }
}

export class GenerateError extends Error {
  readonly kind: "DuplicateQueryName" | "Io" | "Json" | "UnsupportedVersion";
  readonly found?: number;
  readonly expected?: number;

  private constructor(kind: GenerateError["kind"], message: string, found?: number, expected?: number) {
    super(message);
    this.name = "GenerateError";
    this.kind = kind;
    this.found = found;
    this.expected = expected;
  }

  static duplicateQueryName(name: string): GenerateError {
    return new GenerateError("DuplicateQueryName", `duplicate generated query name: ${name}`);
  }

  static io(message: string): GenerateError {
    return new GenerateError("Io", `io error: ${message}`);
  }

  static json(message: string): GenerateError {
    return new GenerateError("Json", `json error: ${message}`);
  }

  static unsupportedVersion(found: number, expected: number): GenerateError {
    return new GenerateError("UnsupportedVersion", `unsupported query bundle version ${found} (expected ${expected})`, found, expected);
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
  if (!Number.isSafeInteger(asNumber)) throw DynamicQueryError.invalidDatetime(path, millis);
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
    if (this.variant === "Null") return unit("Null");
    return newtype(this.variant, this.payload);
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
    return newtype(this.variant, this.payload);
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
    return this.variant === "All" ? unit("All") : newtype(this.variant, this.payload);
  }
}

export class EdgeRef implements Encodable {
  private constructor(
    readonly variant: "Ids" | "Var" | "Param",
    readonly payload: unknown,
  ) {}
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
    return newtype(this.variant, this.payload);
  }
}

export enum CompareOp {
  Eq = "Eq",
  Neq = "Neq",
  Gt = "Gt",
  Gte = "Gte",
  Lt = "Lt",
  Lte = "Lte",
}
export enum Order {
  Asc = "Asc",
  Desc = "Desc",
}

export enum RangeIndexDirection {
  Asc = "Asc",
  Desc = "Desc",
}

function rangeIndexFields(label: string, property: string, direction: RangeIndexDirection): Record<string, unknown> {
  const fields: Record<string, unknown> = { label, property };
  if (direction !== RangeIndexDirection.Asc) fields.direction = direction;
  return fields;
}
export enum EmitBehavior {
  None = "None",
  Before = "Before",
  After = "After",
  All = "All",
}
export enum AggregateFunction {
  Count = "Count",
  Sum = "Sum",
  Min = "Min",
  Max = "Max",
  Mean = "Mean",
}

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
  static case(whenThen: [Predicate, Expr][], elseExpr?: Expr | null): Expr {
    return new Expr("Case", { when_then: whenThen, else_expr: elseExpr ?? null });
  }
  toJSON(): JsonValue {
    if (["Id", "Timestamp", "DateTimeNow"].includes(this.variant)) return unit(this.variant);
    if (["Add", "Sub", "Mul", "Div", "Mod"].includes(this.variant)) return tuple(this.variant, this.payload as unknown[]);
    if (this.variant === "Neg") return newtype("Neg", this.payload);
    if (this.variant === "Case") return struct("Case", this.payload as Record<string, unknown>);
    return newtype(this.variant, this.payload);
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
    return newtype(this.variant, this.payload);
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
    return predicate.toPredicate();
  }
  toJSON(): JsonValue {
    if (this.variant === "Compare") return struct("Compare", this.payload as Record<string, unknown>);
    if (this.variant === "Not") return newtype("Not", this.payload);
    if (["And", "Or"].includes(this.variant)) return newtype(this.variant, this.payload);
    if (["HasKey", "IsNull", "IsNotNull"].includes(this.variant)) return newtype(this.variant, this.payload);
    return tuple(this.variant, this.payload as unknown[]);
  }
}

export class SourcePredicate implements Encodable {
  private constructor(
    readonly variant: string,
    readonly payload?: unknown,
  ) {}
  // Comparison constructors accept a literal value or an Expr/parameter. Literals keep the existing
  // variant (e.g. `Eq`); expressions route to the `*Expr` variant (mirrors the Rust DSL).
  static eq(property: string, value: PropertyValueInput | Expr | ParamRef): SourcePredicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value"
      ? new SourcePredicate("Eq", [property, input.payload])
      : new SourcePredicate("EqExpr", [property, input.payload]);
  }
  static neq(property: string, value: PropertyValueInput | Expr | ParamRef): SourcePredicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value"
      ? new SourcePredicate("Neq", [property, input.payload])
      : new SourcePredicate("NeqExpr", [property, input.payload]);
  }
  static gt(property: string, value: PropertyValueInput | Expr | ParamRef): SourcePredicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value"
      ? new SourcePredicate("Gt", [property, input.payload])
      : new SourcePredicate("GtExpr", [property, input.payload]);
  }
  static gte(property: string, value: PropertyValueInput | Expr | ParamRef): SourcePredicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value"
      ? new SourcePredicate("Gte", [property, input.payload])
      : new SourcePredicate("GteExpr", [property, input.payload]);
  }
  static lt(property: string, value: PropertyValueInput | Expr | ParamRef): SourcePredicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value"
      ? new SourcePredicate("Lt", [property, input.payload])
      : new SourcePredicate("LtExpr", [property, input.payload]);
  }
  static lte(property: string, value: PropertyValueInput | Expr | ParamRef): SourcePredicate {
    const input = PropertyInput.from(value);
    return input.variant === "Value"
      ? new SourcePredicate("Lte", [property, input.payload])
      : new SourcePredicate("LteExpr", [property, input.payload]);
  }
  static between(property: string, min: PropertyValueInput | Expr | ParamRef, max: PropertyValueInput | Expr | ParamRef): SourcePredicate {
    const lo = PropertyInput.from(min);
    const hi = PropertyInput.from(max);
    if (lo.variant === "Value" && hi.variant === "Value") {
      return new SourcePredicate("Between", [property, lo.payload, hi.payload]);
    }
    return new SourcePredicate("BetweenExpr", [property, lo.toExpr(), hi.toExpr()]);
  }
  static hasKey(property: string): SourcePredicate {
    return new SourcePredicate("HasKey", property);
  }
  static startsWith(property: string, prefix: string): SourcePredicate {
    return new SourcePredicate("StartsWith", [property, prefix]);
  }
  static and(predicates: SourcePredicate[]): SourcePredicate {
    return new SourcePredicate("And", predicates);
  }
  static or(predicates: SourcePredicate[]): SourcePredicate {
    return new SourcePredicate("Or", predicates);
  }
  toPredicate(): Predicate {
    const p = this.payload as unknown[];
    switch (this.variant) {
      case "Eq":
        return Predicate.eq(p[0] as string, p[1] as PropertyValue);
      case "Neq":
        return Predicate.neq(p[0] as string, p[1] as PropertyValue);
      case "Gt":
        return Predicate.gt(p[0] as string, p[1] as PropertyValue);
      case "Gte":
        return Predicate.gte(p[0] as string, p[1] as PropertyValue);
      case "Lt":
        return Predicate.lt(p[0] as string, p[1] as PropertyValue);
      case "Lte":
        return Predicate.lte(p[0] as string, p[1] as PropertyValue);
      case "Between":
        return Predicate.between(p[0] as string, p[1] as PropertyValue, p[2] as PropertyValue);
      case "HasKey":
        return Predicate.hasKey(this.payload as string);
      case "StartsWith":
        return Predicate.startsWith(p[0] as string, p[1] as string);
      case "And":
        return Predicate.and((this.payload as SourcePredicate[]).map((entry) => entry.toPredicate()));
      case "Or":
        return Predicate.or((this.payload as SourcePredicate[]).map((entry) => entry.toPredicate()));
      case "EqExpr":
        return Predicate.eq(p[0] as string, p[1] as Expr);
      case "NeqExpr":
        return Predicate.neq(p[0] as string, p[1] as Expr);
      case "GtExpr":
        return Predicate.gt(p[0] as string, p[1] as Expr);
      case "GteExpr":
        return Predicate.gte(p[0] as string, p[1] as Expr);
      case "LtExpr":
        return Predicate.lt(p[0] as string, p[1] as Expr);
      case "LteExpr":
        return Predicate.lte(p[0] as string, p[1] as Expr);
      case "BetweenExpr":
        return Predicate.between(p[0] as string, p[1] as Expr, p[2] as Expr);
      default:
        throw new Error(`unknown source predicate: ${this.variant}`);
    }
  }
  toJSON(): JsonValue {
    if (["And", "Or", "HasKey"].includes(this.variant)) return newtype(this.variant, this.payload);
    return tuple(this.variant, this.payload as unknown[]);
  }
}

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
  static expr(alias: string, expr: Expr): Projection {
    return new Projection(ExprProjection.new(alias, expr));
  }
  static from(value: ProjectionInput): Projection {
    return value instanceof Projection ? value : new Projection(value);
  }
  toJSON(): JsonValue {
    return this.inner.toJSON();
  }
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
    return {
      traversal: this.traversal,
      times: this.timesValue,
      until: this.untilValue,
      emit: this.emitValue,
      emit_predicate: this.emitPredicateValue,
      max_depth: this.maxDepthValue,
    };
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
  static nodeVector(label: string, property: string, tenantProperty?: string | null): IndexSpec {
    return new IndexSpec("NodeVector", { label, property, tenant_property: tenantProperty ?? undefined });
  }
  static nodeText(label: string, property: string, tenantProperty?: string | null): IndexSpec {
    return new IndexSpec("NodeText", { label, property, tenant_property: tenantProperty ?? undefined });
  }
  static edgeVector(label: string, property: string, tenantProperty?: string | null): IndexSpec {
    return new IndexSpec("EdgeVector", { label, property, tenant_property: tenantProperty ?? undefined });
  }
  static edgeText(label: string, property: string, tenantProperty?: string | null): IndexSpec {
    return new IndexSpec("EdgeText", { label, property, tenant_property: tenantProperty ?? undefined });
  }
  toJSON(): JsonValue {
    return struct(this.variant, this.fields);
  }
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
  static edgeProperties(): Step {
    return Step.unit("EdgeProperties");
  }
  static createIndex(spec: IndexSpec, ifNotExists: boolean): Step {
    return Step.struct("CreateIndex", { spec, if_not_exists: ifNotExists });
  }
  static dropIndex(spec: IndexSpec): Step {
    return Step.struct("DropIndex", { spec });
  }
  static createVectorIndexNodes(label: string, property: string, tenantProperty?: string | null): Step {
    return Step.struct("CreateVectorIndexNodes", { label, property, tenant_property: tenantProperty ?? undefined });
  }
  static createVectorIndexEdges(label: string, property: string, tenantProperty?: string | null): Step {
    return Step.struct("CreateVectorIndexEdges", { label, property, tenant_property: tenantProperty ?? undefined });
  }
  static createTextIndexNodes(label: string, property: string, tenantProperty?: string | null): Step {
    return Step.struct("CreateTextIndexNodes", { label, property, tenant_property: tenantProperty ?? undefined });
  }
  static createTextIndexEdges(label: string, property: string, tenantProperty?: string | null): Step {
    return Step.struct("CreateTextIndexEdges", { label, property, tenant_property: tenantProperty ?? undefined });
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
    return Step.struct("Choose", { condition, then_traversal: thenTraversal, else_traversal: elseTraversal ?? null });
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
  toJSON(): JsonValue {
    if (this.style === "unit") return unit(this.variant);
    if (this.style === "newtype") return newtype(this.variant, this.payload);
    if (this.style === "tuple") return tuple(this.variant, this.payload as unknown[]);
    return struct(this.variant, this.payload as Record<string, unknown>);
  }
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
    return { steps: this.steps };
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
        "EdgeProperties",
        "CreateIndex",
        "DropIndex",
        "CreateVectorIndexNodes",
        "CreateVectorIndexEdges",
        "CreateTextIndexNodes",
        "CreateTextIndexEdges",
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
  createVectorIndexNodes(label: string, property: string, tenantProperty?: string | null): Traversal<"terminal", "write"> {
    return this.createIndexIfNotExists(IndexSpec.nodeVector(label, property, tenantProperty));
  }
  createVectorIndexEdges(label: string, property: string, tenantProperty?: string | null): Traversal<"terminal", "write"> {
    return this.createIndexIfNotExists(IndexSpec.edgeVector(label, property, tenantProperty));
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
    return { steps: this.steps };
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
      ? unit("PrevNotEmpty")
      : this.variant === "VarMinSize"
        ? tuple("VarMinSize", this.payload as unknown[])
        : newtype(this.variant, this.payload);
  }
}

export class NamedQuery implements Encodable {
  constructor(
    readonly name: string | null,
    readonly steps: Step[],
    readonly condition: BatchCondition | null,
  ) {}
  toJSON(): JsonValue {
    return { name: this.name, steps: this.steps, condition: this.condition };
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
  static forEach(paramName: string, body: BatchEntry[]): BatchEntry {
    return new BatchEntry("ForEach", { param: paramName, body });
  }
  toJSON(): JsonValue {
    return this.variant === "Query" ? newtype("Query", this.payload) : struct("ForEach", this.payload as Record<string, unknown>);
  }
}

export class ReadBatch implements Encodable {
  constructor(
    readonly queries: BatchEntry[] = [],
    readonly returns: string[] = [],
  ) {}
  static new(): ReadBatch {
    return new ReadBatch();
  }
  varAs<S extends TraversalState>(name: string, traversal: Traversal<S, "read">): ReadBatch {
    if (traversal.mode !== "read") throw new TypeError("ReadBatch.varAs only accepts read-only traversals");
    return new ReadBatch([...this.queries, BatchEntry.query(new NamedQuery(name, traversal.intoSteps(), null))], this.returns);
  }
  varAsIf<S extends TraversalState>(name: string, condition: BatchCondition, traversal: Traversal<S, "read">): ReadBatch {
    if (traversal.mode !== "read") throw new TypeError("ReadBatch.varAsIf only accepts read-only traversals");
    return new ReadBatch([...this.queries, BatchEntry.query(new NamedQuery(name, traversal.intoSteps(), condition))], this.returns);
  }
  forEachParam(paramName: string, body: ReadBatch): ReadBatch {
    return new ReadBatch([...this.queries, BatchEntry.forEach(paramName, body.queries)], this.returns);
  }
  returning(vars: Iterable<string>): ReadBatch {
    return new ReadBatch(this.queries, Array.from(vars));
  }
  toJSON(): JsonValue {
    return { queries: this.queries, returns: this.returns };
  }
  toJsonBytes(): Uint8Array {
    return new TextEncoder().encode(this.toJsonString());
  }
  toJsonString(): string {
    return stringifyJson(this);
  }
  toDynamicRequest(options?: DynamicQueryOptions): DynamicQueryRequest;
  toDynamicRequest(): DynamicQueryRequest;
  toDynamicRequest<T extends ParamShape>(
    params: DefinedParams<T>,
    values: ParamInputs<T>,
    options?: DynamicQueryOptions,
  ): DynamicQueryRequest;
  toDynamicRequest<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | DynamicQueryOptions,
    values?: ParamInputs<T>,
    options?: DynamicQueryOptions,
  ): DynamicQueryRequest {
    return buildDynamicRequest(DynamicQueryRequest.read(this), paramsOrOptions, values, options);
  }
  toDynamicJson(options?: DynamicQueryOptions): string;
  toDynamicJson(): string;
  toDynamicJson<T extends ParamShape>(params: DefinedParams<T>, values: ParamInputs<T>, options?: DynamicQueryOptions): string;
  toDynamicJson<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | DynamicQueryOptions,
    values?: ParamInputs<T>,
    options?: DynamicQueryOptions,
  ): string {
    return this.toDynamicRequest(paramsOrOptions as DefinedParams<T>, values as ParamInputs<T>, options).toJsonString();
  }
  toDynamicBytes(options?: DynamicQueryOptions): Uint8Array;
  toDynamicBytes(): Uint8Array;
  toDynamicBytes<T extends ParamShape>(params: DefinedParams<T>, values: ParamInputs<T>, options?: DynamicQueryOptions): Uint8Array;
  toDynamicBytes<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | DynamicQueryOptions,
    values?: ParamInputs<T>,
    options?: DynamicQueryOptions,
  ): Uint8Array {
    return this.toDynamicRequest(paramsOrOptions as DefinedParams<T>, values as ParamInputs<T>, options).toJsonBytes();
  }
}

export class WriteBatch implements Encodable {
  constructor(
    readonly queries: BatchEntry[] = [],
    readonly returns: string[] = [],
  ) {}
  static new(): WriteBatch {
    return new WriteBatch();
  }
  varAs<S extends TraversalState, M extends MutationMode>(name: string, traversal: Traversal<S, M>): WriteBatch {
    return new WriteBatch([...this.queries, BatchEntry.query(new NamedQuery(name, traversal.intoSteps(), null))], this.returns);
  }
  varAsIf<S extends TraversalState, M extends MutationMode>(
    name: string,
    condition: BatchCondition,
    traversal: Traversal<S, M>,
  ): WriteBatch {
    return new WriteBatch([...this.queries, BatchEntry.query(new NamedQuery(name, traversal.intoSteps(), condition))], this.returns);
  }
  forEachParam(paramName: string, body: WriteBatch): WriteBatch {
    return new WriteBatch([...this.queries, BatchEntry.forEach(paramName, body.queries)], this.returns);
  }
  returning(vars: Iterable<string>): WriteBatch {
    return new WriteBatch(this.queries, Array.from(vars));
  }
  toJSON(): JsonValue {
    return { queries: this.queries, returns: this.returns };
  }
  toJsonBytes(): Uint8Array {
    return new TextEncoder().encode(this.toJsonString());
  }
  toJsonString(): string {
    return stringifyJson(this);
  }
  toDynamicRequest(options?: DynamicQueryOptions): DynamicQueryRequest;
  toDynamicRequest(): DynamicQueryRequest;
  toDynamicRequest<T extends ParamShape>(
    params: DefinedParams<T>,
    values: ParamInputs<T>,
    options?: DynamicQueryOptions,
  ): DynamicQueryRequest;
  toDynamicRequest<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | DynamicQueryOptions,
    values?: ParamInputs<T>,
    options?: DynamicQueryOptions,
  ): DynamicQueryRequest {
    return buildDynamicRequest(DynamicQueryRequest.write(this), paramsOrOptions, values, options);
  }
  toDynamicJson(options?: DynamicQueryOptions): string;
  toDynamicJson(): string;
  toDynamicJson<T extends ParamShape>(params: DefinedParams<T>, values: ParamInputs<T>, options?: DynamicQueryOptions): string;
  toDynamicJson<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | DynamicQueryOptions,
    values?: ParamInputs<T>,
    options?: DynamicQueryOptions,
  ): string {
    return this.toDynamicRequest(paramsOrOptions as DefinedParams<T>, values as ParamInputs<T>, options).toJsonString();
  }
  toDynamicBytes(options?: DynamicQueryOptions): Uint8Array;
  toDynamicBytes(): Uint8Array;
  toDynamicBytes<T extends ParamShape>(params: DefinedParams<T>, values: ParamInputs<T>, options?: DynamicQueryOptions): Uint8Array;
  toDynamicBytes<T extends ParamShape>(
    paramsOrOptions?: DefinedParams<T> | DynamicQueryOptions,
    values?: ParamInputs<T>,
    options?: DynamicQueryOptions,
  ): Uint8Array {
    return this.toDynamicRequest(paramsOrOptions as DefinedParams<T>, values as ParamInputs<T>, options).toJsonBytes();
  }
}

export function readBatch(): ReadBatch {
  return ReadBatch.new();
}
export function writeBatch(): WriteBatch {
  return WriteBatch.new();
}

export class QueryParamType implements Encodable {
  private constructor(
    readonly variant: string,
    readonly inner?: QueryParamType,
  ) {}
  static bool(): QueryParamType {
    return new QueryParamType("Bool");
  }
  static i64(): QueryParamType {
    return new QueryParamType("I64");
  }
  static f64(): QueryParamType {
    return new QueryParamType("F64");
  }
  static f32(): QueryParamType {
    return new QueryParamType("F32");
  }
  static string(): QueryParamType {
    return new QueryParamType("String");
  }
  static dateTime(): QueryParamType {
    return new QueryParamType("DateTime");
  }
  static bytes(): QueryParamType {
    return new QueryParamType("Bytes");
  }
  static value(): QueryParamType {
    return new QueryParamType("Value");
  }
  static object(): QueryParamType {
    return new QueryParamType("Object");
  }
  static array(inner: QueryParamType): QueryParamType {
    return new QueryParamType("Array", inner);
  }
  toJSON(): JsonValue {
    return this.variant === "Array" ? newtype("Array", this.inner) : unit(this.variant);
  }
}

export interface QueryParameter extends Encodable {
  name: string;
  ty: QueryParamType;
}

class QueryParameterImpl implements QueryParameter {
  constructor(
    readonly name: string,
    readonly ty: QueryParamType,
  ) {}
  toJSON(): JsonValue {
    return { name: this.name, ty: this.ty };
  }
}

type ParamKind = "Bool" | "I64" | "F64" | "F32" | "String" | "DateTime" | "Bytes" | "Value" | "Object" | "Array";

export type ParamSchemaInput<T> = T extends ParamSchema<infer Input> ? Input : never;
const PARAMS_METADATA: unique symbol = Symbol("@helixdb/enterprise-ql/params-metadata");

export class ParamSchema<Input = unknown> implements Encodable {
  declare readonly __input?: Input;
  constructor(
    readonly kind: ParamKind,
    readonly inner?: ParamSchema,
    readonly objectInner?: ParamSchema,
  ) {}
  toParamType(): QueryParamType {
    switch (this.kind) {
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
        return QueryParamType.array(this.inner!.toParamType());
    }
  }
  toJSON(): JsonValue {
    return this.toParamType().toJSON();
  }
}

export const param = {
  bool: (): ParamSchema<boolean> => new ParamSchema("Bool"),
  i64: (): ParamSchema<number | bigint> => new ParamSchema("I64"),
  f64: (): ParamSchema<number> => new ParamSchema("F64"),
  f32: (): ParamSchema<number> => new ParamSchema("F32"),
  string: (): ParamSchema<string> => new ParamSchema("String"),
  dateTime: (): ParamSchema<DateTime | string | number | bigint> => new ParamSchema("DateTime"),
  bytes: (): ParamSchema<Uint8Array | number[]> => new ParamSchema("Bytes"),
  value: (): ParamSchema<PropertyValueInput> => new ParamSchema("Value"),
  object: <Inner extends ParamSchema = ParamSchema<PropertyValueInput>>(
    inner: Inner = new ParamSchema("Value") as Inner,
  ): ParamSchema<Record<string, ParamSchemaInput<Inner>>> => new ParamSchema("Object", undefined, inner),
  array: <Inner extends ParamSchema>(inner: Inner): ParamSchema<ParamSchemaInput<Inner>[]> => new ParamSchema("Array", inner),
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

function parametersForParams<T extends ParamShape>(params: DefinedParams<T>): QueryParameter[] {
  return Object.entries(schemaForParams(params)).map(([name, schema]) => new QueryParameterImpl(name, schema.toParamType()));
}

function convertInputFromSchema<T extends ParamShape>(schema: T, input: ParamInputs<T>): Record<string, JsonValue> {
  const out: Record<string, JsonValue> = {};
  for (const [name, paramSchema] of Object.entries(schema)) {
    if (!(name in input)) throw new TypeError(`missing required parameter: ${name}`);
    out[name] = convertParamValue(paramSchema, input[name], name);
  }
  return out;
}

function convertInputForParams<T extends ParamShape>(params: DefinedParams<T>, input: ParamInputs<T>): Record<string, JsonValue> {
  return convertInputFromSchema(schemaForParams(params), input);
}

function convertParamValue(schema: ParamSchema, value: unknown, path: string): JsonValue {
  switch (schema.kind) {
    case "Bool":
      if (typeof value !== "boolean") throw new TypeError(`parameter '${path}' must be boolean`);
      return value;
    case "I64":
      return intToJson(value as number | bigint);
    case "F64":
    case "F32":
      if (typeof value !== "number") throw new TypeError(`parameter '${path}' must be number`);
      return value;
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
      throw DynamicQueryError.unsupportedBytes(path);
    case "Value":
      return dynamicFromPropertyValue(PropertyValue.from(value as PropertyValueInput), path);
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

function dynamicFromPropertyValue(value: PropertyValue, path: string): JsonValue {
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
      throw DynamicQueryError.unsupportedBytes(path);
    case "I64Array":
    case "F64Array":
    case "F32Array":
    case "StringArray":
      return value.payload as JsonValue;
    case "Array":
      return (value.payload as PropertyValue[]).map((entry, index) => dynamicFromPropertyValue(entry, `${path}[${index}]`));
    case "Object": {
      const out: Record<string, JsonValue> = {};
      for (const [key, entry] of Object.entries(value.payload as Record<string, PropertyValue>))
        out[key] = dynamicFromPropertyValue(entry, `${path}.${key}`);
      return out;
    }
    default:
      throw new TypeError(`unsupported property value variant: ${value.variant}`);
  }
}

export enum DynamicQueryRequestType {
  Read = "read",
  Write = "write",
}
export type DynamicQueryValue = JsonValue;
export const DynamicQueryValue = {
  null: (): JsonValue => null,
  bool: (value: boolean): JsonValue => value,
  i64: (value: number | bigint): JsonValue => intToJson(value),
  f64: (value: number): JsonValue => value,
  f32: (value: number): JsonValue => value,
  string: (value: string): JsonValue => value,
  array: (values: JsonValue[]): JsonValue => values,
  object: (values: Record<string, JsonValue>): JsonValue => values,
};
export type BatchQuery = ReadBatch | WriteBatch;
export type DynamicQueryOptions = { queryName?: string | null };

export class DynamicQueryRequest implements Encodable {
  queryName: string | null = null;
  parameters?: Record<string, JsonValue>;
  parameterTypes?: Record<string, QueryParamType>;
  private constructor(
    readonly requestType: DynamicQueryRequestType,
    readonly query: BatchQuery,
    queryName: string | null = null,
  ) {
    this.queryName = queryName;
  }
  static read(query: ReadBatch, queryName: string | null = null): DynamicQueryRequest {
    return new DynamicQueryRequest(DynamicQueryRequestType.Read, query, queryName);
  }
  static write(query: WriteBatch, queryName: string | null = null): DynamicQueryRequest {
    return new DynamicQueryRequest(DynamicQueryRequestType.Write, query, queryName);
  }
  insertParameterValue(name: string, value: JsonValue): void {
    this.parameters ??= {};
    this.parameters[name] = value;
  }
  insertParameterType(name: string, ty: QueryParamType): void {
    this.parameterTypes ??= {};
    this.parameterTypes[name] = ty;
  }
  withParameterValue(name: string, value: JsonValue): DynamicQueryRequest {
    this.insertParameterValue(name, value);
    return this;
  }
  withParameterType(name: string, ty: QueryParamType): DynamicQueryRequest {
    this.insertParameterType(name, ty);
    return this;
  }
  setQueryName(name: string): void {
    this.queryName = name;
  }
  clearQueryName(): void {
    this.queryName = null;
  }
  withQueryName(name: string): DynamicQueryRequest {
    this.setQueryName(name);
    return this;
  }
  toJSON(): JsonValue {
    return {
      request_type: this.requestType,
      query_name: this.queryName ?? null,
      query: this.query,
      parameters: this.parameters,
      parameter_types: this.parameterTypes,
    };
  }
  toJsonBytes(): Uint8Array {
    return new TextEncoder().encode(this.toJsonString());
  }
  toJsonString(): string {
    return stringifyJson(this);
  }
}

function addDynamicParameters<T extends ParamShape>(
  request: DynamicQueryRequest,
  params?: DefinedParams<T>,
  values?: ParamInputs<T>,
): DynamicQueryRequest {
  if (!params) return request;
  if (values === undefined) throw new TypeError("dynamic parameter values are required when a parameter schema is provided");

  const parameters = parametersForParams(params);
  rejectUnknownParameters(
    values as Record<string, unknown>,
    parameters.map((parameter) => parameter.name),
  );
  const converted = convertInputForParams(params, values);
  for (const parameter of parameters) request.insertParameterType(parameter.name, parameter.ty);
  for (const [name, value] of Object.entries(converted)) request.insertParameterValue(name, value);
  return request;
}

function isDefinedParams(value: unknown): value is DefinedParams<ParamShape> {
  return typeof value === "object" && value !== null && PARAMS_METADATA in value;
}

function applyDynamicQueryOptions(request: DynamicQueryRequest, options?: DynamicQueryOptions): DynamicQueryRequest {
  if (!options || !("queryName" in options)) return request;
  if (options.queryName === null || options.queryName === undefined) {
    request.clearQueryName();
  } else {
    request.setQueryName(options.queryName);
  }
  return request;
}

function buildDynamicRequest<T extends ParamShape>(
  request: DynamicQueryRequest,
  paramsOrOptions?: DefinedParams<T> | DynamicQueryOptions,
  values?: ParamInputs<T>,
  options?: DynamicQueryOptions,
): DynamicQueryRequest {
  if (isDefinedParams(paramsOrOptions)) {
    return applyDynamicQueryOptions(addDynamicParameters(request, paramsOrOptions, values), options);
  }
  if (values !== undefined) throw new TypeError("dynamic parameter values require a parameter schema");
  return applyDynamicQueryOptions(request, paramsOrOptions);
}

export const QUERY_BUNDLE_VERSION = 4;

export interface QueryBundle extends Encodable {
  version: number;
  readRoutes: Record<string, ReadBatch>;
  writeRoutes: Record<string, WriteBatch>;
  readParameters: Record<string, QueryParameter[]>;
  writeParameters: Record<string, QueryParameter[]>;
}

class QueryBundleImpl implements QueryBundle {
  constructor(
    readonly version: number,
    readonly readRoutes: Record<string, ReadBatch>,
    readonly writeRoutes: Record<string, WriteBatch>,
    readonly readParameters: Record<string, QueryParameter[]>,
    readonly writeParameters: Record<string, QueryParameter[]>,
  ) {}
  toJSON(): JsonValue {
    return {
      version: this.version,
      read_routes: sortedObject(this.readRoutes),
      write_routes: sortedObject(this.writeRoutes),
      read_parameters: sortedObject(this.readParameters),
      write_parameters: sortedObject(this.writeParameters),
    };
  }
}

function sortedObject<T>(input: Record<string, T>): Record<string, T> {
  return Object.fromEntries(Object.entries(input).sort(([a], [b]) => a.localeCompare(b)));
}

export type RegisteredReadQuery<Input extends Record<string, unknown> = Record<string, unknown>> = {
  kind: "read";
  build: () => ReadBatch;
  parameters: () => QueryParameter[];
  convertInput?: (input: Input) => Record<string, JsonValue>;
};
export type RegisteredWriteQuery<Input extends Record<string, unknown> = Record<string, unknown>> = {
  kind: "write";
  build: () => WriteBatch;
  parameters: () => QueryParameter[];
  convertInput?: (input: Input) => Record<string, JsonValue>;
};

type ReadBuilder<T extends ParamShape> = (params: DefinedParams<T>) => ReadBatch;
type WriteBuilder<T extends ParamShape> = (params: DefinedParams<T>) => WriteBatch;

export function registerRead<T extends ParamShape>(builder: ReadBuilder<T>, params: DefinedParams<T>): RegisteredReadQuery<ParamInputs<T>> {
  return {
    kind: "read",
    build: () => builder(params),
    parameters: () => parametersForParams(params),
    convertInput: (input) => convertInputForParams(params, input),
  };
}

export function registerWrite<T extends ParamShape>(
  builder: WriteBuilder<T>,
  params: DefinedParams<T>,
): RegisteredWriteQuery<ParamInputs<T>> {
  return {
    kind: "write",
    build: () => builder(params),
    parameters: () => parametersForParams(params),
    convertInput: (input) => convertInputForParams(params, input),
  };
}

type QueryDefinitions = { read?: Record<string, RegisteredReadQuery<any>>; write?: Record<string, RegisteredWriteQuery<any>> };
type RouteInput<T> = T extends RegisteredReadQuery<infer Input> ? Input : T extends RegisteredWriteQuery<infer Input> ? Input : never;
type CallArgs<Input extends Record<string, unknown>> = keyof Input extends never ? [input?: Input] : [input: Input];
type QueryCallMap<T extends QueryDefinitions> = {
  readonly [K in keyof NonNullable<T["read"]>]: (...args: CallArgs<RouteInput<NonNullable<T["read"]>[K]>>) => DynamicQueryRequest;
} & {
  readonly [K in keyof NonNullable<T["write"]>]: (...args: CallArgs<RouteInput<NonNullable<T["write"]>[K]>>) => DynamicQueryRequest;
};

export class DefinedQueries<T extends QueryDefinitions> {
  readonly call: QueryCallMap<T>;
  constructor(readonly definitions: T) {
    assertUniqueRouteNames(definitions);
    const call: Record<string, (input?: Record<string, unknown>) => DynamicQueryRequest> = {};
    for (const [name, route] of Object.entries(definitions.read ?? {})) call[name] = (input = {}) => buildRequest(name, route, input);
    for (const [name, route] of Object.entries(definitions.write ?? {})) call[name] = (input = {}) => buildRequest(name, route, input);
    this.call = call as QueryCallMap<T>;
  }
  buildQueryBundle(): QueryBundle {
    return buildQueryBundle(this.definitions);
  }
  async generate(path = "queries.json"): Promise<string> {
    return generateToPath(this.definitions, path);
  }
}

function buildRequest(
  name: string,
  route: RegisteredReadQuery | RegisteredWriteQuery,
  input: Record<string, unknown>,
): DynamicQueryRequest {
  const request = route.kind === "read" ? DynamicQueryRequest.read(route.build()) : DynamicQueryRequest.write(route.build());
  request.setQueryName(name);
  const parameters = route.parameters();
  rejectUnknownParameters(
    input,
    parameters.map((parameter) => parameter.name),
  );
  const values = route.convertInput ? route.convertInput(input) : convertInputFromSchema(parametersToSchemas(parameters), input);
  for (const parameter of parameters) request.insertParameterType(parameter.name, parameter.ty);
  for (const [paramName, value] of Object.entries(values)) request.insertParameterValue(paramName, value);
  return request;
}

function rejectUnknownParameters(input: Record<string, unknown>, expected: string[]): void {
  const allowed = new Set(expected);
  for (const key of Object.keys(input)) {
    if (!allowed.has(key)) throw new TypeError(`unknown parameter: ${key}`);
  }
}

function assertUniqueRouteNames(definitions: QueryDefinitions): void {
  const names = new Set<string>();
  for (const name of Object.keys(definitions.read ?? {})) {
    if (names.has(name)) throw GenerateError.duplicateQueryName(name);
    names.add(name);
  }
  for (const name of Object.keys(definitions.write ?? {})) {
    if (names.has(name)) throw GenerateError.duplicateQueryName(name);
    names.add(name);
  }
}

function parametersToSchemas(parameters: QueryParameter[]): Record<string, ParamSchema> {
  const out: Record<string, ParamSchema> = {};
  for (const parameter of parameters) out[parameter.name] = schemaFromParamType(parameter.ty);
  return out;
}

function schemaFromParamType(type: QueryParamType): ParamSchema {
  switch (type.variant) {
    case "Bool":
      return param.bool();
    case "I64":
      return param.i64();
    case "F64":
      return param.f64();
    case "F32":
      return param.f32();
    case "String":
      return param.string();
    case "DateTime":
      return param.dateTime();
    case "Bytes":
      return param.bytes();
    case "Value":
      return param.value();
    case "Object":
      return param.object();
    case "Array":
      return param.array(schemaFromParamType(type.inner!));
    default:
      throw new Error(`unknown parameter type: ${type.variant}`);
  }
}

export function defineQueries<T extends QueryDefinitions>(definitions: T): DefinedQueries<T> {
  return new DefinedQueries(definitions);
}

export function buildQueryBundle(definitions: QueryDefinitions): QueryBundle {
  assertUniqueRouteNames(definitions);
  const readRoutes: Record<string, ReadBatch> = {};
  const writeRoutes: Record<string, WriteBatch> = {};
  const readParameters: Record<string, QueryParameter[]> = {};
  const writeParameters: Record<string, QueryParameter[]> = {};
  for (const [name, route] of Object.entries(definitions.read ?? {})) {
    if (name in readRoutes || name in writeRoutes) throw GenerateError.duplicateQueryName(name);
    readRoutes[name] = route.build();
    readParameters[name] = route.parameters();
  }
  for (const [name, route] of Object.entries(definitions.write ?? {})) {
    if (name in readRoutes || name in writeRoutes) throw GenerateError.duplicateQueryName(name);
    writeRoutes[name] = route.build();
    writeParameters[name] = route.parameters();
  }
  return new QueryBundleImpl(QUERY_BUNDLE_VERSION, readRoutes, writeRoutes, readParameters, writeParameters);
}

export function serializeQueryBundle(bundle: QueryBundle): string {
  return stringifyJson(bundle, true);
}

export function deserializeQueryBundle(json: string | Uint8Array): unknown {
  const text = typeof json === "string" ? json : new TextDecoder().decode(json);
  const parsed = JSON.parse(text) as { version?: number };
  if (parsed.version !== QUERY_BUNDLE_VERSION) throw GenerateError.unsupportedVersion(parsed.version ?? -1, QUERY_BUNDLE_VERSION);
  return parsed;
}

export async function writeQueryBundleToPath(bundle: QueryBundle, path: string): Promise<void> {
  await writeFile(path, serializeQueryBundle(bundle));
}

export async function readQueryBundleFromPath(path: string): Promise<unknown> {
  return deserializeQueryBundle(await readFile(path));
}

export async function generateToPath(definitions: QueryDefinitions, path: string): Promise<string> {
  await writeQueryBundleToPath(buildQueryBundle(definitions), path);
  return path;
}

export async function generate(definitions: QueryDefinitions): Promise<string> {
  return generateToPath(definitions, "queries.json");
}

export const prelude = {
  g,
  sub,
  readBatch,
  writeBatch,
  defineParams,
  defineQueries,
  registerRead,
  registerWrite,
  param,
  DateTime,
  DynamicQueryRequest,
  DynamicQueryRequestType,
  DynamicQueryValue,
  PropertyValue,
  PropertyInput,
  NodeRef,
  EdgeRef,
  Expr,
  StreamBound,
  CompareOp,
  Predicate,
  SourcePredicate,
  PropertyProjection,
  ExprProjection,
  Projection,
  Order,
  EmitBehavior,
  AggregateFunction,
  RepeatConfig,
  IndexSpec,
  RangeIndexDirection,
  Traversal,
  SubTraversal,
  ReadBatch,
  WriteBatch,
  BatchCondition,
  BatchEntry,
  QueryParamType,
};
