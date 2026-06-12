"""HelixDB dynamic query DSL.

The module mirrors the dynamic query AST emitted by the Rust, TypeScript, and Go
SDKs while keeping the Python surface idiomatic: methods are snake_case and
builders are immutable.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import math
from pathlib import Path
from typing import Any, TypeAlias

JsonValue: TypeAlias = Any
NodeId: TypeAlias = int
EdgeId: TypeAlias = int

QUERY_BUNDLE_VERSION = 4


class _Omit:
    pass


_OMIT = _Omit()
_UNSET = object()


def _encode(value: Any) -> JsonValue:
    if value is _OMIT:
        return _OMIT
    if hasattr(value, "to_json") and callable(value.to_json):
        return _encode(value.to_json())
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, bytes):
        return list(value)
    if isinstance(value, bytearray):
        return list(value)
    if isinstance(value, (list, tuple)):
        return [_encode(entry) for entry in value]
    if isinstance(value, dict):
        out: dict[str, JsonValue] = {}
        for key, entry in value.items():
            encoded = _encode(entry)
            if encoded is not _OMIT:
                out[str(key)] = encoded
        return out
    if isinstance(value, float) and not math.isfinite(value):
        raise TypeError("non-finite numbers cannot be serialized as JSON")
    return value


def _unit(name: str) -> JsonValue:
    return name


def _newtype(name: str, value: Any) -> JsonValue:
    return {name: _encode(value)}


def _tuple(name: str, values: Sequence[Any]) -> JsonValue:
    return {name: [_encode(value) for value in values]}


def _struct(name: str, fields: Mapping[str, Any]) -> JsonValue:
    return {name: _encode(dict(fields))}


def stringify_json(value: Any, pretty: bool = False) -> str:
    """Serialize SDK values to Helix dynamic-query JSON."""

    return json.dumps(
        _encode(value),
        allow_nan=False,
        ensure_ascii=False,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
    )


def parse_json_structural(data: str | bytes) -> JsonValue:
    return json.loads(data)


def canonicalize_json(value: Any) -> Any:
    if isinstance(value, list):
        return [canonicalize_json(entry) for entry in value]
    if isinstance(value, dict):
        return {key: canonicalize_json(value[key]) for key in sorted(value)}
    return value


def structural_json_equal(left: str | bytes, right: str | bytes) -> bool:
    return canonicalize_json(parse_json_structural(left)) == canonicalize_json(
        parse_json_structural(right)
    )


class DynamicQueryError(ValueError):
    """Error raised while converting dynamic query parameters."""

    def __init__(
        self,
        kind: str,
        message: str,
        *,
        path: str | None = None,
        millis: int | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.path = path
        self.millis = millis

    @classmethod
    def serialize(cls, message: str) -> "DynamicQueryError":
        return cls("Serialize", f"json serialization error: {message}")

    @classmethod
    def utf8(cls, message: str) -> "DynamicQueryError":
        return cls("Utf8", f"utf8 conversion error: {message}")

    @classmethod
    def unsupported_bytes(cls, path: str) -> "DynamicQueryError":
        return cls(
            "UnsupportedBytesParameter",
            f"parameter '{path}' uses bytes, which the dynamic query JSON route cannot represent",
            path=path,
        )

    @classmethod
    def invalid_datetime(cls, path: str, millis: int) -> "DynamicQueryError":
        return cls(
            "InvalidDateTimeParameter",
            f"parameter '{path}' uses datetime millis '{millis}', which cannot be rendered as RFC3339",
            path=path,
            millis=millis,
        )


class GenerateError(ValueError):
    """Error raised while generating or decoding query bundles."""

    def __init__(
        self,
        kind: str,
        message: str,
        *,
        found: int | None = None,
        expected: int | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.found = found
        self.expected = expected

    @classmethod
    def duplicate_query_name(cls, name: str) -> "GenerateError":
        return cls("DuplicateQueryName", f"duplicate generated query name: {name}")

    @classmethod
    def unsupported_version(cls, found: int, expected: int) -> "GenerateError":
        return cls(
            "UnsupportedVersion",
            f"unsupported query bundle version {found} (expected {expected})",
            found=found,
            expected=expected,
        )


def _int_to_json(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected integer, got {value!r}")
    return value


def _finite_float(value: float, *, name: str = "float") -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"expected {name}, got {value!r}")
    out = float(value)
    if not math.isfinite(out):
        raise TypeError("non-finite floats cannot be serialized as JSON")
    return out


@dataclass(frozen=True)
class DateTime:
    """Millisecond timestamp rendered as RFC3339 UTC for dynamic parameters."""

    _millis: int

    @classmethod
    def from_millis(cls, millis: int) -> "DateTime":
        return cls(_int_to_json(millis))

    @classmethod
    def from_datetime(cls, value: datetime) -> "DateTime":
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return cls.from_millis(int(value.astimezone(timezone.utc).timestamp() * 1000))

    @classmethod
    def parse_rfc3339(cls, value: str) -> "DateTime":
        text = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return cls.from_datetime(datetime.fromisoformat(text))
        except ValueError as exc:
            raise TypeError(f"invalid RFC3339 datetime: {value}") from exc

    def millis(self) -> int:
        return self._millis

    def to_rfc3339(self) -> str:
        return _datetime_to_rfc3339(self, "datetime")


def _datetime_to_rfc3339(value: DateTime, path: str) -> str:
    millis = value.millis()
    try:
        dt = datetime.fromtimestamp(millis / 1000, timezone.utc)
    except (OverflowError, OSError) as exc:
        raise DynamicQueryError.invalid_datetime(path, millis) from exc
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class I64Literal:
    value: int


@dataclass(frozen=True)
class F32Literal:
    value: float


@dataclass(frozen=True)
class F64Literal:
    value: float


@dataclass(frozen=True)
class BytesLiteral:
    value: bytes | bytearray | Sequence[int]


@dataclass(frozen=True)
class DateTimeLiteral:
    value: DateTime


def i64(value: int) -> I64Literal:
    return I64Literal(_int_to_json(value))


def f32(value: float) -> F32Literal:
    return F32Literal(_finite_float(value, name="f32"))


def f64(value: float) -> F64Literal:
    return F64Literal(_finite_float(value, name="f64"))


def bytes_(value: bytes | bytearray | Sequence[int]) -> BytesLiteral:
    return BytesLiteral(value)


def date_time(value: DateTime) -> DateTimeLiteral:
    return DateTimeLiteral(value)


PropertyValueInput: TypeAlias = Any
ParamObject: TypeAlias = Mapping[str, PropertyValueInput]
PropertyMap: TypeAlias = Mapping[str, PropertyValueInput]


@dataclass(frozen=True)
class PropertyValue:
    variant: str
    payload: Any = None

    @classmethod
    def null(cls) -> "PropertyValue":
        return cls("Null")

    @classmethod
    def bool(cls, value: bool) -> "PropertyValue":
        if not isinstance(value, bool):
            raise TypeError(f"expected bool, got {value!r}")
        return cls("Bool", value)

    @classmethod
    def i64(cls, value: int) -> "PropertyValue":
        return cls("I64", _int_to_json(value))

    @classmethod
    def date_time(cls, value: DateTime | int) -> "PropertyValue":
        millis = value.millis() if isinstance(value, DateTime) else _int_to_json(value)
        return cls("DateTime", millis)

    datetime = date_time

    @classmethod
    def datetime_millis(cls, millis: int) -> "PropertyValue":
        return cls.date_time(millis)

    @classmethod
    def f64(cls, value: float) -> "PropertyValue":
        return cls("F64", _finite_float(value, name="f64"))

    @classmethod
    def f32(cls, value: float) -> "PropertyValue":
        return cls("F32", _finite_float(value, name="f32"))

    @classmethod
    def string(cls, value: str) -> "PropertyValue":
        if not isinstance(value, str):
            raise TypeError(f"expected string, got {value!r}")
        return cls("String", value)

    @classmethod
    def bytes(cls, value: bytes | bytearray | Sequence[int]) -> "PropertyValue":
        return cls("Bytes", [int(byte) for byte in value])

    @classmethod
    def i64_array(cls, values: Iterable[int]) -> "PropertyValue":
        return cls("I64Array", [_int_to_json(value) for value in values])

    @classmethod
    def f64_array(cls, values: Iterable[float]) -> "PropertyValue":
        return cls("F64Array", [_finite_float(value, name="f64") for value in values])

    @classmethod
    def f32_array(cls, values: Iterable[float]) -> "PropertyValue":
        return cls("F32Array", [_finite_float(value, name="f32") for value in values])

    @classmethod
    def string_array(cls, values: Iterable[str]) -> "PropertyValue":
        return cls("StringArray", [str(value) for value in values])

    @classmethod
    def array(cls, values: Iterable[PropertyValueInput]) -> "PropertyValue":
        return cls("Array", [cls.from_value(value) for value in values])

    @classmethod
    def object(cls, values: Mapping[str, PropertyValueInput]) -> "PropertyValue":
        return cls("Object", {key: cls.from_value(value) for key, value in values.items()})

    @classmethod
    def from_value(cls, value: PropertyValueInput) -> "PropertyValue":
        if isinstance(value, PropertyValue):
            return value
        if isinstance(value, I64Literal):
            return cls.i64(value.value)
        if isinstance(value, F32Literal):
            return cls.f32(value.value)
        if isinstance(value, F64Literal):
            return cls.f64(value.value)
        if isinstance(value, BytesLiteral):
            return cls.bytes(value.value)
        if isinstance(value, DateTimeLiteral):
            return cls.date_time(value.value)
        if isinstance(value, DateTime):
            return cls.date_time(value)
        if value is None:
            return cls.null()
        if isinstance(value, bool):
            return cls.bool(value)
        if isinstance(value, str):
            return cls.string(value)
        if isinstance(value, int):
            return cls.i64(value)
        if isinstance(value, float):
            return cls.f64(value)
        if isinstance(value, (bytes, bytearray)):
            return cls.bytes(value)
        if isinstance(value, Mapping):
            return cls.object(value)
        if isinstance(value, (list, tuple)):
            if all(isinstance(entry, str) for entry in value):
                return cls.string_array(value)
            if all(isinstance(entry, int) and not isinstance(entry, bool) for entry in value):
                return cls.i64_array(value)
            if all(
                isinstance(entry, (int, float)) and not isinstance(entry, bool)
                for entry in value
            ):
                return cls.f64_array(value)
            return cls.array(value)
        raise TypeError(f"unsupported property value {type(value).__name__}")

    def as_str(self) -> str | None:
        return self.payload if self.variant == "String" else None

    def as_i64(self) -> int | None:
        return self.payload if self.variant == "I64" else None

    def as_datetime_millis(self) -> int | None:
        return self.payload if self.variant == "DateTime" else None

    def as_f64(self) -> float | None:
        return self.payload if self.variant in {"F64", "F32"} else None

    def as_bool(self) -> bool | None:
        return self.payload if self.variant == "Bool" else None

    def as_array(self) -> list["PropertyValue"] | None:
        return self.payload if self.variant == "Array" else None

    def as_object(self) -> dict[str, "PropertyValue"] | None:
        return self.payload if self.variant == "Object" else None

    def to_json(self) -> JsonValue:
        if self.variant == "Null":
            return _unit("Null")
        return _newtype(self.variant, self.payload)


ParamValue = PropertyValue


@dataclass(frozen=True)
class PropertyInput:
    variant: str
    payload: PropertyValue | "Expr"

    @classmethod
    def value(cls, value: PropertyValueInput) -> "PropertyInput":
        return cls("Value", PropertyValue.from_value(value))

    @classmethod
    def expr(cls, expr: "Expr | ParamRef") -> "PropertyInput":
        return cls("Expr", expr.to_expr() if isinstance(expr, ParamRef) else expr)

    @classmethod
    def param(cls, name: str) -> "PropertyInput":
        return cls.expr(Expr.param(name))

    @classmethod
    def from_value(
        cls, value: PropertyValueInput | "Expr" | "ParamRef" | "PropertyInput"
    ) -> "PropertyInput":
        if isinstance(value, PropertyInput):
            return value
        if isinstance(value, (Expr, ParamRef)):
            return cls.expr(value)
        return cls.value(value)

    def to_expr(self) -> "Expr":
        if self.variant == "Expr":
            return self.payload  # type: ignore[return-value]
        return Expr.val(self.payload)

    def to_json(self) -> JsonValue:
        return _newtype(self.variant, self.payload)


@dataclass(frozen=True)
class NodeRef:
    variant: str
    payload: Any = None

    @classmethod
    def all(cls) -> "NodeRef":
        return cls("All")

    @classmethod
    def id(cls, node_id: NodeId) -> "NodeRef":
        return cls("Ids", [_int_to_json(node_id)])

    @classmethod
    def ids(cls, node_ids: Iterable[NodeId]) -> "NodeRef":
        return cls("Ids", [_int_to_json(node_id) for node_id in node_ids])

    @classmethod
    def var(cls, name: str) -> "NodeRef":
        return cls("Var", name)

    @classmethod
    def param(cls, name: str) -> "NodeRef":
        return cls("Param", name)

    @classmethod
    def from_value(cls, value: "NodeRef | NodeId | Iterable[NodeId] | str") -> "NodeRef":
        if isinstance(value, NodeRef):
            return value
        if isinstance(value, str):
            return cls.var(value)
        if isinstance(value, Iterable):
            return cls.ids(value)  # type: ignore[arg-type]
        return cls.id(value)  # type: ignore[arg-type]

    def to_json(self) -> JsonValue:
        return _unit("All") if self.variant == "All" else _newtype(self.variant, self.payload)


@dataclass(frozen=True)
class EdgeRef:
    variant: str
    payload: Any

    @classmethod
    def id(cls, edge_id: EdgeId) -> "EdgeRef":
        return cls("Ids", [_int_to_json(edge_id)])

    @classmethod
    def ids(cls, edge_ids: Iterable[EdgeId]) -> "EdgeRef":
        return cls("Ids", [_int_to_json(edge_id) for edge_id in edge_ids])

    @classmethod
    def var(cls, name: str) -> "EdgeRef":
        return cls("Var", name)

    @classmethod
    def param(cls, name: str) -> "EdgeRef":
        return cls("Param", name)

    @classmethod
    def from_value(cls, value: "EdgeRef | EdgeId | Iterable[EdgeId]") -> "EdgeRef":
        if isinstance(value, EdgeRef):
            return value
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
            return cls.ids(value)  # type: ignore[arg-type]
        return cls.id(value)  # type: ignore[arg-type]

    def to_json(self) -> JsonValue:
        return _newtype(self.variant, self.payload)


class CompareOp(str, Enum):
    EQ = "Eq"
    NEQ = "Neq"
    GT = "Gt"
    GTE = "Gte"
    LT = "Lt"
    LTE = "Lte"


CompareOp.Eq = CompareOp.EQ  # type: ignore[attr-defined]
CompareOp.Neq = CompareOp.NEQ  # type: ignore[attr-defined]
CompareOp.Gt = CompareOp.GT  # type: ignore[attr-defined]
CompareOp.Gte = CompareOp.GTE  # type: ignore[attr-defined]
CompareOp.Lt = CompareOp.LT  # type: ignore[attr-defined]
CompareOp.Lte = CompareOp.LTE  # type: ignore[attr-defined]


class Order(str, Enum):
    ASC = "Asc"
    DESC = "Desc"


Order.Asc = Order.ASC  # type: ignore[attr-defined]
Order.Desc = Order.DESC  # type: ignore[attr-defined]


class RangeIndexDirection(str, Enum):
    ASC = "Asc"
    DESC = "Desc"


RangeIndexDirection.Asc = RangeIndexDirection.ASC  # type: ignore[attr-defined]
RangeIndexDirection.Desc = RangeIndexDirection.DESC  # type: ignore[attr-defined]


class EmitBehavior(str, Enum):
    NONE = "None"
    BEFORE = "Before"
    AFTER = "After"
    ALL = "All"


EmitBehavior.None_ = EmitBehavior.NONE  # type: ignore[attr-defined]
EmitBehavior.Before = EmitBehavior.BEFORE  # type: ignore[attr-defined]
EmitBehavior.After = EmitBehavior.AFTER  # type: ignore[attr-defined]
EmitBehavior.All = EmitBehavior.ALL  # type: ignore[attr-defined]


class AggregateFunction(str, Enum):
    COUNT = "Count"
    SUM = "Sum"
    MIN = "Min"
    MAX = "Max"
    MEAN = "Mean"


AggregateFunction.Count = AggregateFunction.COUNT  # type: ignore[attr-defined]
AggregateFunction.Sum = AggregateFunction.SUM  # type: ignore[attr-defined]
AggregateFunction.Min = AggregateFunction.MIN  # type: ignore[attr-defined]
AggregateFunction.Max = AggregateFunction.MAX  # type: ignore[attr-defined]
AggregateFunction.Mean = AggregateFunction.MEAN  # type: ignore[attr-defined]


@dataclass(frozen=True)
class Expr:
    variant: str
    payload: Any = None

    @classmethod
    def prop(cls, name: str) -> "Expr":
        return cls("Property", name)

    @classmethod
    def val(cls, value: PropertyValueInput) -> "Expr":
        return cls("Constant", PropertyValue.from_value(value))

    @classmethod
    def id(cls) -> "Expr":
        return cls("Id")

    @classmethod
    def timestamp(cls) -> "Expr":
        return cls("Timestamp")

    @classmethod
    def date_time_now(cls) -> "Expr":
        return cls("DateTimeNow")

    datetime = date_time_now

    @classmethod
    def param(cls, name: str) -> "Expr":
        return cls("Param", name)

    def add(self, other: "Expr") -> "Expr":
        return Expr("Add", [self, other])

    def sub(self, other: "Expr") -> "Expr":
        return Expr("Sub", [self, other])

    def mul(self, other: "Expr") -> "Expr":
        return Expr("Mul", [self, other])

    def div(self, other: "Expr") -> "Expr":
        return Expr("Div", [self, other])

    def modulo(self, other: "Expr") -> "Expr":
        return Expr("Mod", [self, other])

    mod = modulo

    def neg(self) -> "Expr":
        return Expr("Neg", self)

    def __add__(self, other: "Expr") -> "Expr":
        return self.add(other)

    def __sub__(self, other: "Expr") -> "Expr":
        return self.sub(other)

    def __mul__(self, other: "Expr") -> "Expr":
        return self.mul(other)

    def __truediv__(self, other: "Expr") -> "Expr":
        return self.div(other)

    def __mod__(self, other: "Expr") -> "Expr":
        return self.modulo(other)

    def __neg__(self) -> "Expr":
        return self.neg()

    @classmethod
    def case(
        cls,
        when_then: Iterable[tuple["Predicate", "Expr"]],
        else_expr: "Expr | None" = None,
    ) -> "Expr":
        return cls("Case", {"when_then": list(when_then), "else_expr": else_expr})

    def to_json(self) -> JsonValue:
        if self.variant in {"Id", "Timestamp", "DateTimeNow"}:
            return _unit(self.variant)
        if self.variant in {"Add", "Sub", "Mul", "Div", "Mod"}:
            return _tuple(self.variant, self.payload)
        if self.variant == "Neg":
            return _newtype("Neg", self.payload)
        if self.variant == "Case":
            return _struct("Case", self.payload)
        return _newtype(self.variant, self.payload)


@dataclass(frozen=True)
class StreamBound:
    variant: str
    payload: Any

    @classmethod
    def literal(cls, value: int) -> "StreamBound":
        return cls("Literal", _int_to_json(value))

    @classmethod
    def expr(cls, expr: "Expr | ParamRef") -> "StreamBound":
        return cls("Expr", expr.to_expr() if isinstance(expr, ParamRef) else expr)

    @classmethod
    def from_value(cls, value: "StreamBound | int | Expr | ParamRef") -> "StreamBound":
        if isinstance(value, StreamBound):
            return value
        if isinstance(value, (Expr, ParamRef)):
            return cls.expr(value)
        if isinstance(value, int) and not isinstance(value, bool) and value < 0:
            return cls.expr(Expr.val(value))
        return cls.literal(value)  # type: ignore[arg-type]

    def to_json(self) -> JsonValue:
        return _newtype(self.variant, self.payload)


@dataclass(frozen=True)
class Predicate:
    variant: str
    payload: Any = None

    @staticmethod
    def _comparison(variant: str, property: str, value: PropertyValueInput | Expr | "ParamRef") -> "Predicate":
        input_value = PropertyInput.from_value(value)
        if input_value.variant == "Value":
            return Predicate(variant, [property, input_value.payload])
        return Predicate(f"{variant}Expr", [property, input_value.payload])

    @classmethod
    def eq(cls, property: str, value: PropertyValueInput | Expr | "ParamRef") -> "Predicate":
        return cls._comparison("Eq", property, value)

    @classmethod
    def neq(cls, property: str, value: PropertyValueInput | Expr | "ParamRef") -> "Predicate":
        return cls._comparison("Neq", property, value)

    @classmethod
    def gt(cls, property: str, value: PropertyValueInput | Expr | "ParamRef") -> "Predicate":
        return cls._comparison("Gt", property, value)

    @classmethod
    def gte(cls, property: str, value: PropertyValueInput | Expr | "ParamRef") -> "Predicate":
        return cls._comparison("Gte", property, value)

    @classmethod
    def lt(cls, property: str, value: PropertyValueInput | Expr | "ParamRef") -> "Predicate":
        return cls._comparison("Lt", property, value)

    @classmethod
    def lte(cls, property: str, value: PropertyValueInput | Expr | "ParamRef") -> "Predicate":
        return cls._comparison("Lte", property, value)

    @classmethod
    def between(
        cls,
        property: str,
        min_value: PropertyValueInput | Expr | "ParamRef",
        max_value: PropertyValueInput | Expr | "ParamRef",
    ) -> "Predicate":
        lo = PropertyInput.from_value(min_value)
        hi = PropertyInput.from_value(max_value)
        if lo.variant == "Value" and hi.variant == "Value":
            return cls("Between", [property, lo.payload, hi.payload])
        return cls("BetweenExpr", [property, lo.to_expr(), hi.to_expr()])

    @classmethod
    def has_key(cls, property: str) -> "Predicate":
        return cls("HasKey", property)

    @classmethod
    def is_null(cls, property: str) -> "Predicate":
        return cls("IsNull", property)

    @classmethod
    def is_not_null(cls, property: str) -> "Predicate":
        return cls("IsNotNull", property)

    @classmethod
    def starts_with(cls, property: str, prefix: str) -> "Predicate":
        return cls("StartsWith", [property, prefix])

    @classmethod
    def ends_with(cls, property: str, suffix: str) -> "Predicate":
        return cls("EndsWith", [property, suffix])

    @classmethod
    def contains(cls, property: str, substring: str) -> "Predicate":
        return cls("Contains", [property, substring])

    @classmethod
    def contains_expr(cls, property: str, expr: Expr | "ParamRef") -> "Predicate":
        return cls("ContainsExpr", [property, expr.to_expr() if isinstance(expr, ParamRef) else expr])

    @classmethod
    def contains_param(cls, property: str, param_name: str) -> "Predicate":
        return cls.contains_expr(property, Expr.param(param_name))

    @classmethod
    def is_in(cls, property: str, values: PropertyValueInput) -> "Predicate":
        return cls("IsIn", [property, PropertyValue.from_value(values)])

    @classmethod
    def is_in_expr(cls, property: str, values: Expr | "ParamRef") -> "Predicate":
        return cls("IsInExpr", [property, values.to_expr() if isinstance(values, ParamRef) else values])

    @classmethod
    def is_in_param(cls, property: str, param_name: str) -> "Predicate":
        return cls.is_in_expr(property, Expr.param(param_name))

    @classmethod
    def and_(cls, predicates: Iterable["Predicate"]) -> "Predicate":
        return cls("And", list(predicates))

    @classmethod
    def or_(cls, predicates: Iterable["Predicate"]) -> "Predicate":
        return cls("Or", list(predicates))

    @classmethod
    def not_(cls, predicate: "Predicate") -> "Predicate":
        return cls("Not", predicate)

    @classmethod
    def compare(cls, left: Expr, op: CompareOp, right: Expr) -> "Predicate":
        return cls("Compare", {"left": left, "op": op, "right": right})

    @classmethod
    def eq_param(cls, property: str, param_name: str) -> "Predicate":
        return cls("EqExpr", [property, Expr.param(param_name)])

    @classmethod
    def neq_param(cls, property: str, param_name: str) -> "Predicate":
        return cls("NeqExpr", [property, Expr.param(param_name)])

    @classmethod
    def gt_param(cls, property: str, param_name: str) -> "Predicate":
        return cls("GtExpr", [property, Expr.param(param_name)])

    @classmethod
    def gte_param(cls, property: str, param_name: str) -> "Predicate":
        return cls("GteExpr", [property, Expr.param(param_name)])

    @classmethod
    def lt_param(cls, property: str, param_name: str) -> "Predicate":
        return cls("LtExpr", [property, Expr.param(param_name)])

    @classmethod
    def lte_param(cls, property: str, param_name: str) -> "Predicate":
        return cls("LteExpr", [property, Expr.param(param_name)])

    @classmethod
    def from_source(cls, predicate: "SourcePredicate") -> "Predicate":
        return predicate.to_predicate()

    def to_json(self) -> JsonValue:
        if self.variant == "Compare":
            return _struct("Compare", self.payload)
        if self.variant == "Not":
            return _newtype("Not", self.payload)
        if self.variant in {"And", "Or", "HasKey", "IsNull", "IsNotNull"}:
            return _newtype(self.variant, self.payload)
        return _tuple(self.variant, self.payload)


@dataclass(frozen=True)
class SourcePredicate:
    variant: str
    payload: Any = None

    @staticmethod
    def _comparison(
        variant: str, property: str, value: PropertyValueInput | Expr | ParamRef
    ) -> "SourcePredicate":
        input_value = PropertyInput.from_value(value)
        if input_value.variant == "Value":
            return SourcePredicate(variant, [property, input_value.payload])
        return SourcePredicate(f"{variant}Expr", [property, input_value.payload])

    @classmethod
    def eq(cls, property: str, value: PropertyValueInput | Expr | ParamRef) -> "SourcePredicate":
        return cls._comparison("Eq", property, value)

    @classmethod
    def neq(cls, property: str, value: PropertyValueInput | Expr | ParamRef) -> "SourcePredicate":
        return cls._comparison("Neq", property, value)

    @classmethod
    def gt(cls, property: str, value: PropertyValueInput | Expr | ParamRef) -> "SourcePredicate":
        return cls._comparison("Gt", property, value)

    @classmethod
    def gte(cls, property: str, value: PropertyValueInput | Expr | ParamRef) -> "SourcePredicate":
        return cls._comparison("Gte", property, value)

    @classmethod
    def lt(cls, property: str, value: PropertyValueInput | Expr | ParamRef) -> "SourcePredicate":
        return cls._comparison("Lt", property, value)

    @classmethod
    def lte(cls, property: str, value: PropertyValueInput | Expr | ParamRef) -> "SourcePredicate":
        return cls._comparison("Lte", property, value)

    @classmethod
    def between(
        cls,
        property: str,
        min_value: PropertyValueInput | Expr | ParamRef,
        max_value: PropertyValueInput | Expr | ParamRef,
    ) -> "SourcePredicate":
        lo = PropertyInput.from_value(min_value)
        hi = PropertyInput.from_value(max_value)
        if lo.variant == "Value" and hi.variant == "Value":
            return cls("Between", [property, lo.payload, hi.payload])
        return cls("BetweenExpr", [property, lo.to_expr(), hi.to_expr()])

    @classmethod
    def has_key(cls, property: str) -> "SourcePredicate":
        return cls("HasKey", property)

    @classmethod
    def starts_with(cls, property: str, prefix: str) -> "SourcePredicate":
        return cls("StartsWith", [property, prefix])

    @classmethod
    def and_(cls, predicates: Iterable["SourcePredicate"]) -> "SourcePredicate":
        return cls("And", list(predicates))

    @classmethod
    def or_(cls, predicates: Iterable["SourcePredicate"]) -> "SourcePredicate":
        return cls("Or", list(predicates))

    def to_predicate(self) -> Predicate:
        payload = self.payload
        if self.variant == "Eq":
            return Predicate.eq(payload[0], payload[1])
        if self.variant == "Neq":
            return Predicate.neq(payload[0], payload[1])
        if self.variant == "Gt":
            return Predicate.gt(payload[0], payload[1])
        if self.variant == "Gte":
            return Predicate.gte(payload[0], payload[1])
        if self.variant == "Lt":
            return Predicate.lt(payload[0], payload[1])
        if self.variant == "Lte":
            return Predicate.lte(payload[0], payload[1])
        if self.variant == "Between":
            return Predicate.between(payload[0], payload[1], payload[2])
        if self.variant == "HasKey":
            return Predicate.has_key(payload)
        if self.variant == "StartsWith":
            return Predicate.starts_with(payload[0], payload[1])
        if self.variant == "And":
            return Predicate.and_(entry.to_predicate() for entry in payload)
        if self.variant == "Or":
            return Predicate.or_(entry.to_predicate() for entry in payload)
        if self.variant == "EqExpr":
            return Predicate.eq(payload[0], payload[1])
        if self.variant == "NeqExpr":
            return Predicate.neq(payload[0], payload[1])
        if self.variant == "GtExpr":
            return Predicate.gt(payload[0], payload[1])
        if self.variant == "GteExpr":
            return Predicate.gte(payload[0], payload[1])
        if self.variant == "LtExpr":
            return Predicate.lt(payload[0], payload[1])
        if self.variant == "LteExpr":
            return Predicate.lte(payload[0], payload[1])
        if self.variant == "BetweenExpr":
            return Predicate.between(payload[0], payload[1], payload[2])
        raise ValueError(f"unknown source predicate: {self.variant}")

    def to_json(self) -> JsonValue:
        if self.variant in {"And", "Or", "HasKey"}:
            return _newtype(self.variant, self.payload)
        return _tuple(self.variant, self.payload)


@dataclass(frozen=True)
class PropertyProjection:
    source: str
    alias: str

    @classmethod
    def new(cls, name: str) -> "PropertyProjection":
        return cls(name, name)

    @classmethod
    def renamed(cls, source: str, alias: str) -> "PropertyProjection":
        return cls(source, alias)

    def to_json(self) -> JsonValue:
        return {"source": self.source, "alias": self.alias}


@dataclass(frozen=True)
class ExprProjection:
    alias: str
    expr: Expr

    @classmethod
    def new(cls, alias: str, expr: Expr) -> "ExprProjection":
        return cls(alias, expr)

    def to_json(self) -> JsonValue:
        return {"alias": self.alias, "expr": _encode(self.expr)}


ProjectionInput: TypeAlias = "Projection | PropertyProjection | ExprProjection"


@dataclass(frozen=True)
class Projection:
    inner: PropertyProjection | ExprProjection

    @classmethod
    def property(cls, source: str, alias: str | None = None) -> "Projection":
        return cls(PropertyProjection.renamed(source, alias or source))

    @classmethod
    def expr(cls, alias: str, expr: Expr) -> "Projection":
        return cls(ExprProjection(alias, expr))

    @classmethod
    def from_value(cls, value: ProjectionInput) -> "Projection":
        return value if isinstance(value, Projection) else cls(value)

    def to_json(self) -> JsonValue:
        return self.inner.to_json()


@dataclass(frozen=True)
class RepeatConfig:
    traversal: "SubTraversal"
    times_value: int | None = None
    until_value: Predicate | None = None
    emit_value: EmitBehavior = EmitBehavior.NONE
    emit_predicate_value: Predicate | None = None
    max_depth_value: int = 100

    @classmethod
    def new(cls, traversal: "SubTraversal") -> "RepeatConfig":
        return cls(traversal)

    def times(self, n: int) -> "RepeatConfig":
        return RepeatConfig(
            self.traversal,
            _int_to_json(n),
            self.until_value,
            self.emit_value,
            self.emit_predicate_value,
            self.max_depth_value,
        )

    def until(self, predicate: Predicate) -> "RepeatConfig":
        return RepeatConfig(
            self.traversal,
            self.times_value,
            predicate,
            self.emit_value,
            self.emit_predicate_value,
            self.max_depth_value,
        )

    def emit_all(self) -> "RepeatConfig":
        return self._emit(EmitBehavior.ALL)

    def emit_before(self) -> "RepeatConfig":
        return self._emit(EmitBehavior.BEFORE)

    def emit_after(self) -> "RepeatConfig":
        return self._emit(EmitBehavior.AFTER)

    def emit_if(self, predicate: Predicate) -> "RepeatConfig":
        return RepeatConfig(
            self.traversal,
            self.times_value,
            self.until_value,
            EmitBehavior.AFTER,
            predicate,
            self.max_depth_value,
        )

    def max_depth(self, depth: int) -> "RepeatConfig":
        return RepeatConfig(
            self.traversal,
            self.times_value,
            self.until_value,
            self.emit_value,
            self.emit_predicate_value,
            _int_to_json(depth),
        )

    def _emit(self, behavior: EmitBehavior) -> "RepeatConfig":
        return RepeatConfig(
            self.traversal,
            self.times_value,
            self.until_value,
            behavior,
            self.emit_predicate_value,
            self.max_depth_value,
        )

    def to_json(self) -> JsonValue:
        return {
            "traversal": self.traversal,
            "times": self.times_value,
            "until": self.until_value,
            "emit": self.emit_value,
            "emit_predicate": self.emit_predicate_value,
            "max_depth": self.max_depth_value,
        }


@dataclass(frozen=True)
class IndexSpec:
    variant: str
    fields: Mapping[str, Any]

    @staticmethod
    def _range_fields(label: str, property: str, direction: RangeIndexDirection) -> Mapping[str, Any]:
        fields: dict[str, Any] = {"label": label, "property": property}
        if direction != RangeIndexDirection.ASC:
            fields["direction"] = direction.value
        return fields

    @classmethod
    def node_equality(cls, label: str, property: str) -> "IndexSpec":
        return cls("NodeEquality", {"label": label, "property": property, "unique": False})

    @classmethod
    def node_unique_equality(cls, label: str, property: str) -> "IndexSpec":
        return cls("NodeEquality", {"label": label, "property": property, "unique": True})

    @classmethod
    def node_range(cls, label: str, property: str) -> "IndexSpec":
        return cls.node_range_with_direction(label, property, RangeIndexDirection.ASC)

    @classmethod
    def node_range_desc(cls, label: str, property: str) -> "IndexSpec":
        return cls.node_range_with_direction(label, property, RangeIndexDirection.DESC)

    @classmethod
    def node_range_with_direction(
        cls, label: str, property: str, direction: RangeIndexDirection = RangeIndexDirection.ASC
    ) -> "IndexSpec":
        return cls("NodeRange", cls._range_fields(label, property, direction))

    @classmethod
    def edge_equality(cls, label: str, property: str) -> "IndexSpec":
        return cls("EdgeEquality", {"label": label, "property": property})

    @classmethod
    def edge_range(cls, label: str, property: str) -> "IndexSpec":
        return cls.edge_range_with_direction(label, property, RangeIndexDirection.ASC)

    @classmethod
    def edge_range_desc(cls, label: str, property: str) -> "IndexSpec":
        return cls.edge_range_with_direction(label, property, RangeIndexDirection.DESC)

    @classmethod
    def edge_range_with_direction(
        cls, label: str, property: str, direction: RangeIndexDirection = RangeIndexDirection.ASC
    ) -> "IndexSpec":
        return cls("EdgeRange", cls._range_fields(label, property, direction))

    @classmethod
    def node_vector(cls, label: str, property: str, tenant_property: str | None = None) -> "IndexSpec":
        return cls(
            "NodeVector",
            {"label": label, "property": property, "tenant_property": tenant_property if tenant_property is not None else _OMIT},
        )

    @classmethod
    def node_text(cls, label: str, property: str, tenant_property: str | None = None) -> "IndexSpec":
        return cls(
            "NodeText",
            {"label": label, "property": property, "tenant_property": tenant_property if tenant_property is not None else _OMIT},
        )

    @classmethod
    def edge_vector(cls, label: str, property: str, tenant_property: str | None = None) -> "IndexSpec":
        return cls(
            "EdgeVector",
            {"label": label, "property": property, "tenant_property": tenant_property if tenant_property is not None else _OMIT},
        )

    @classmethod
    def edge_text(cls, label: str, property: str, tenant_property: str | None = None) -> "IndexSpec":
        return cls(
            "EdgeText",
            {"label": label, "property": property, "tenant_property": tenant_property if tenant_property is not None else _OMIT},
        )

    def to_json(self) -> JsonValue:
        return _struct(self.variant, self.fields)


@dataclass(frozen=True)
class Step:
    variant: str
    style: str
    payload: Any = None

    @classmethod
    def unit(cls, name: str) -> "Step":
        return cls(name, "unit")

    @classmethod
    def newtype(cls, name: str, value: Any) -> "Step":
        return cls(name, "newtype", value)

    @classmethod
    def tuple(cls, name: str, values: Sequence[Any]) -> "Step":
        return cls(name, "tuple", list(values))

    @classmethod
    def struct(cls, name: str, fields: Mapping[str, Any]) -> "Step":
        return cls(name, "struct", dict(fields))

    @classmethod
    def n(cls, nodes: NodeRef) -> "Step":
        return cls.newtype("N", nodes)

    @classmethod
    def n_where(cls, predicate: SourcePredicate) -> "Step":
        return cls.newtype("NWhere", predicate)

    @classmethod
    def e(cls, edges: EdgeRef) -> "Step":
        return cls.newtype("E", edges)

    @classmethod
    def e_where(cls, predicate: SourcePredicate) -> "Step":
        return cls.newtype("EWhere", predicate)

    @classmethod
    def vector_search_nodes(
        cls,
        label: str,
        property: str,
        query_vector: PropertyInput,
        k: StreamBound,
        tenant_value: PropertyInput | None = None,
    ) -> "Step":
        return cls.struct(
            "VectorSearchNodes",
            {
                "label": label,
                "property": property,
                "tenant_value": tenant_value if tenant_value is not None else _OMIT,
                "query_vector": query_vector,
                "k": k,
            },
        )

    @classmethod
    def text_search_nodes(
        cls,
        label: str,
        property: str,
        query_text: PropertyInput,
        k: StreamBound,
        tenant_value: PropertyInput | None = None,
    ) -> "Step":
        return cls.struct(
            "TextSearchNodes",
            {
                "label": label,
                "property": property,
                "tenant_value": tenant_value if tenant_value is not None else _OMIT,
                "query_text": query_text,
                "k": k,
            },
        )

    @classmethod
    def vector_search_edges(
        cls,
        label: str,
        property: str,
        query_vector: PropertyInput,
        k: StreamBound,
        tenant_value: PropertyInput | None = None,
    ) -> "Step":
        return cls.struct(
            "VectorSearchEdges",
            {
                "label": label,
                "property": property,
                "tenant_value": tenant_value if tenant_value is not None else _OMIT,
                "query_vector": query_vector,
                "k": k,
            },
        )

    @classmethod
    def text_search_edges(
        cls,
        label: str,
        property: str,
        query_text: PropertyInput,
        k: StreamBound,
        tenant_value: PropertyInput | None = None,
    ) -> "Step":
        return cls.struct(
            "TextSearchEdges",
            {
                "label": label,
                "property": property,
                "tenant_value": tenant_value if tenant_value is not None else _OMIT,
                "query_text": query_text,
                "k": k,
            },
        )

    @classmethod
    def out(cls, label: str | None = None) -> "Step":
        return cls.newtype("Out", label)

    @classmethod
    def in_(cls, label: str | None = None) -> "Step":
        return cls.newtype("In", label)

    @classmethod
    def both(cls, label: str | None = None) -> "Step":
        return cls.newtype("Both", label)

    @classmethod
    def out_e(cls, label: str | None = None) -> "Step":
        return cls.newtype("OutE", label)

    @classmethod
    def in_e(cls, label: str | None = None) -> "Step":
        return cls.newtype("InE", label)

    @classmethod
    def both_e(cls, label: str | None = None) -> "Step":
        return cls.newtype("BothE", label)

    @classmethod
    def out_n(cls) -> "Step":
        return cls.unit("OutN")

    @classmethod
    def in_n(cls) -> "Step":
        return cls.unit("InN")

    @classmethod
    def other_n(cls) -> "Step":
        return cls.unit("OtherN")

    @classmethod
    def has(cls, property: str, value: PropertyValueInput) -> "Step":
        return cls.tuple("Has", [property, PropertyValue.from_value(value)])

    @classmethod
    def has_label(cls, label: str) -> "Step":
        return cls.newtype("HasLabel", label)

    @classmethod
    def has_key(cls, property: str) -> "Step":
        return cls.newtype("HasKey", property)

    @classmethod
    def where(cls, predicate: Predicate) -> "Step":
        return cls.newtype("Where", predicate)

    @classmethod
    def dedup(cls) -> "Step":
        return cls.unit("Dedup")

    @classmethod
    def within(cls, name: str) -> "Step":
        return cls.newtype("Within", name)

    @classmethod
    def without(cls, name: str) -> "Step":
        return cls.newtype("Without", name)

    @classmethod
    def edge_has(cls, property: str, value: PropertyInput) -> "Step":
        return cls.tuple("EdgeHas", [property, value])

    @classmethod
    def edge_has_label(cls, label: str) -> "Step":
        return cls.newtype("EdgeHasLabel", label)

    @classmethod
    def limit(cls, bound: StreamBound) -> "Step":
        return cls.newtype("Limit", bound.payload) if bound.variant == "Literal" else cls.newtype("LimitBy", bound.payload)

    @classmethod
    def skip(cls, bound: StreamBound) -> "Step":
        return cls.newtype("Skip", bound.payload) if bound.variant == "Literal" else cls.newtype("SkipBy", bound.payload)

    @classmethod
    def range(cls, start: StreamBound, end: StreamBound) -> "Step":
        if start.variant == "Literal" and end.variant == "Literal":
            return cls.tuple("Range", [start.payload, end.payload])
        return cls.tuple("RangeBy", [start, end])

    @classmethod
    def as_(cls, name: str) -> "Step":
        return cls.newtype("As", name)

    @classmethod
    def store(cls, name: str) -> "Step":
        return cls.newtype("Store", name)

    @classmethod
    def select(cls, name: str) -> "Step":
        return cls.newtype("Select", name)

    @classmethod
    def inject(cls, name: str) -> "Step":
        return cls.newtype("Inject", name)

    @classmethod
    def count(cls) -> "Step":
        return cls.unit("Count")

    @classmethod
    def exists(cls) -> "Step":
        return cls.unit("Exists")

    @classmethod
    def id(cls) -> "Step":
        return cls.unit("Id")

    @classmethod
    def label(cls) -> "Step":
        return cls.unit("Label")

    @classmethod
    def values(cls, properties: Iterable[str]) -> "Step":
        return cls.newtype("Values", list(properties))

    @classmethod
    def value_map(cls, properties: Iterable[str] | None = None) -> "Step":
        return cls.newtype("ValueMap", None if properties is None else list(properties))

    @classmethod
    def project(cls, projections: Iterable[ProjectionInput]) -> "Step":
        return cls.newtype("Project", [Projection.from_value(projection) for projection in projections])

    @classmethod
    def edge_properties(cls) -> "Step":
        return cls.unit("EdgeProperties")

    @classmethod
    def create_index(cls, spec: IndexSpec, if_not_exists: bool) -> "Step":
        return cls.struct("CreateIndex", {"spec": spec, "if_not_exists": bool(if_not_exists)})

    @classmethod
    def drop_index(cls, spec: IndexSpec) -> "Step":
        return cls.struct("DropIndex", {"spec": spec})

    @classmethod
    def create_vector_index_nodes(cls, label: str, property: str, tenant_property: str | None = None) -> "Step":
        return cls.struct(
            "CreateVectorIndexNodes",
            {"label": label, "property": property, "tenant_property": tenant_property if tenant_property is not None else _OMIT},
        )

    @classmethod
    def create_vector_index_edges(cls, label: str, property: str, tenant_property: str | None = None) -> "Step":
        return cls.struct(
            "CreateVectorIndexEdges",
            {"label": label, "property": property, "tenant_property": tenant_property if tenant_property is not None else _OMIT},
        )

    @classmethod
    def create_text_index_nodes(cls, label: str, property: str, tenant_property: str | None = None) -> "Step":
        return cls.struct(
            "CreateTextIndexNodes",
            {"label": label, "property": property, "tenant_property": tenant_property if tenant_property is not None else _OMIT},
        )

    @classmethod
    def create_text_index_edges(cls, label: str, property: str, tenant_property: str | None = None) -> "Step":
        return cls.struct(
            "CreateTextIndexEdges",
            {"label": label, "property": property, "tenant_property": tenant_property if tenant_property is not None else _OMIT},
        )

    @classmethod
    def add_n(cls, label: str, properties: Iterable[tuple[str, PropertyInput]]) -> "Step":
        return cls.struct("AddN", {"label": label, "properties": list(properties)})

    @classmethod
    def add_e(cls, label: str, to: NodeRef, properties: Iterable[tuple[str, PropertyInput]]) -> "Step":
        return cls.struct("AddE", {"label": label, "to": to, "properties": list(properties)})

    @classmethod
    def set_property(cls, name: str, value: PropertyInput) -> "Step":
        return cls.tuple("SetProperty", [name, value])

    @classmethod
    def remove_property(cls, name: str) -> "Step":
        return cls.newtype("RemoveProperty", name)

    @classmethod
    def drop(cls) -> "Step":
        return cls.unit("Drop")

    @classmethod
    def drop_edge(cls, to: NodeRef) -> "Step":
        return cls.newtype("DropEdge", to)

    @classmethod
    def drop_edge_labeled(cls, to: NodeRef, label: str) -> "Step":
        return cls.struct("DropEdgeLabeled", {"to": to, "label": label})

    @classmethod
    def drop_edge_by_id(cls, edges: EdgeRef) -> "Step":
        return cls.newtype("DropEdgeById", edges)

    @classmethod
    def order_by(cls, property: str, order: Order) -> "Step":
        return cls.tuple("OrderBy", [property, order])

    @classmethod
    def order_by_multiple(cls, orderings: Iterable[tuple[str, Order]]) -> "Step":
        return cls.newtype("OrderByMultiple", list(orderings))

    @classmethod
    def repeat(cls, config: RepeatConfig) -> "Step":
        return cls.newtype("Repeat", config)

    @classmethod
    def union(cls, traversals: Iterable["SubTraversal"]) -> "Step":
        return cls.newtype("Union", list(traversals))

    @classmethod
    def choose(
        cls,
        condition: Predicate,
        then_traversal: "SubTraversal",
        else_traversal: "SubTraversal | None" = None,
    ) -> "Step":
        return cls.struct(
            "Choose",
            {
                "condition": condition,
                "then_traversal": then_traversal,
                "else_traversal": else_traversal,
            },
        )

    @classmethod
    def coalesce(cls, traversals: Iterable["SubTraversal"]) -> "Step":
        return cls.newtype("Coalesce", list(traversals))

    @classmethod
    def optional(cls, traversal: "SubTraversal") -> "Step":
        return cls.newtype("Optional", traversal)

    @classmethod
    def group(cls, property: str) -> "Step":
        return cls.newtype("Group", property)

    @classmethod
    def group_count(cls, property: str) -> "Step":
        return cls.newtype("GroupCount", property)

    @classmethod
    def aggregate_by(cls, fn: AggregateFunction, property: str) -> "Step":
        return cls.tuple("AggregateBy", [fn, property])

    @classmethod
    def fold(cls) -> "Step":
        return cls.unit("Fold")

    @classmethod
    def unfold(cls) -> "Step":
        return cls.unit("Unfold")

    @classmethod
    def path(cls) -> "Step":
        return cls.unit("Path")

    @classmethod
    def simple_path(cls) -> "Step":
        return cls.unit("SimplePath")

    @classmethod
    def with_sack(cls, initial: PropertyValueInput) -> "Step":
        return cls.newtype("WithSack", PropertyValue.from_value(initial))

    @classmethod
    def sack_set(cls, property: str) -> "Step":
        return cls.newtype("SackSet", property)

    @classmethod
    def sack_add(cls, property: str) -> "Step":
        return cls.newtype("SackAdd", property)

    @classmethod
    def sack_get(cls) -> "Step":
        return cls.unit("SackGet")

    def to_json(self) -> JsonValue:
        if self.style == "unit":
            return _unit(self.variant)
        if self.style == "newtype":
            return _newtype(self.variant, self.payload)
        if self.style == "tuple":
            return _tuple(self.variant, self.payload)
        return _struct(self.variant, self.payload)


PropEntries: TypeAlias = Mapping[str, Any] | Iterable[tuple[str, Any]]


def _property_entries(properties: PropEntries | None = None) -> list[tuple[str, PropertyInput]]:
    if properties is None:
        return []
    entries = properties.items() if isinstance(properties, Mapping) else properties
    return [(key, PropertyInput.from_value(value)) for key, value in entries]


TraversalState: TypeAlias = str
MutationMode: TypeAlias = str


@dataclass(frozen=True)
class Traversal:
    steps: tuple[Step, ...] = ()
    state: TraversalState = "nodes"
    mode: MutationMode = "read"

    @classmethod
    def new(cls) -> "Traversal":
        return cls((), "empty", "read")

    @classmethod
    def from_steps(
        cls,
        steps: Iterable[Step],
        state: TraversalState = "nodes",
        mode: MutationMode = "read",
    ) -> "Traversal":
        return cls(tuple(steps), state, mode)

    def to_json(self) -> JsonValue:
        return {"steps": list(self.steps)}

    def into_steps(self) -> list[Step]:
        return list(self.steps)

    def has_terminal(self) -> bool:
        terminal = {
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
        }
        return any(step.variant in terminal for step in self.steps)

    def _push(
        self, step: Step, state: TraversalState | None = None, mode: MutationMode | None = None
    ) -> "Traversal":
        return Traversal(
            (*self.steps, step),
            self.state if state is None else state,
            self.mode if mode is None else mode,
        )

    def n(self, nodes: NodeRef | NodeId | Iterable[NodeId] | str) -> "Traversal":
        return self._push(Step.n(NodeRef.from_value(nodes)), "nodes")

    def n_where(self, predicate: SourcePredicate) -> "Traversal":
        return self._push(Step.n_where(predicate), "nodes")

    def n_with_label(self, label: str) -> "Traversal":
        return self.n_where(SourcePredicate.eq("$label", label))

    def n_with_label_where(self, label: str, predicate: SourcePredicate) -> "Traversal":
        return self.n_where(SourcePredicate.and_([SourcePredicate.eq("$label", label), predicate]))

    def e(self, edges: EdgeRef | EdgeId | Iterable[EdgeId]) -> "Traversal":
        return self._push(Step.e(EdgeRef.from_value(edges)), "edges")

    def e_where(self, predicate: SourcePredicate) -> "Traversal":
        return self._push(Step.e_where(predicate), "edges")

    def e_with_label(self, label: str) -> "Traversal":
        return self.e_where(SourcePredicate.eq("$label", label))

    def e_with_label_where(self, label: str, predicate: SourcePredicate) -> "Traversal":
        return self.e_where(SourcePredicate.and_([SourcePredicate.eq("$label", label), predicate]))

    def vector_search_nodes(
        self,
        label: str,
        property: str,
        query_vector: Sequence[float],
        k: int,
        tenant_value: PropertyValueInput | None = None,
    ) -> "Traversal":
        return self.vector_search_nodes_with(
            label,
            property,
            PropertyInput.value(PropertyValue.f32_array(query_vector)),
            k,
            None if tenant_value is None else PropertyInput.value(tenant_value),
        )

    def vector_search_nodes_with(
        self,
        label: str,
        property: str,
        query_vector: PropertyInput | Expr | ParamRef | PropertyValueInput,
        k: StreamBound | Expr | ParamRef | int,
        tenant_value: PropertyInput | Expr | ParamRef | PropertyValueInput | None = None,
    ) -> "Traversal":
        return self._push(
            Step.vector_search_nodes(
                label,
                property,
                PropertyInput.from_value(query_vector),
                StreamBound.from_value(k),
                None if tenant_value is None else PropertyInput.from_value(tenant_value),
            ),
            "nodes",
        )

    def text_search_nodes(
        self,
        label: str,
        property: str,
        query_text: str,
        k: int,
        tenant_value: PropertyValueInput | None = None,
    ) -> "Traversal":
        return self.text_search_nodes_with(label, property, query_text, k, tenant_value)

    def text_search_nodes_with(
        self,
        label: str,
        property: str,
        query_text: PropertyInput | Expr | ParamRef | PropertyValueInput,
        k: StreamBound | Expr | ParamRef | int,
        tenant_value: PropertyInput | Expr | ParamRef | PropertyValueInput | None = None,
    ) -> "Traversal":
        return self._push(
            Step.text_search_nodes(
                label,
                property,
                PropertyInput.from_value(query_text),
                StreamBound.from_value(k),
                None if tenant_value is None else PropertyInput.from_value(tenant_value),
            ),
            "nodes",
        )

    def vector_search_edges(
        self,
        label: str,
        property: str,
        query_vector: Sequence[float],
        k: int,
        tenant_value: PropertyValueInput | None = None,
    ) -> "Traversal":
        return self.vector_search_edges_with(
            label,
            property,
            PropertyInput.value(PropertyValue.f32_array(query_vector)),
            k,
            None if tenant_value is None else PropertyInput.value(tenant_value),
        )

    def vector_search_edges_with(
        self,
        label: str,
        property: str,
        query_vector: PropertyInput | Expr | ParamRef | PropertyValueInput,
        k: StreamBound | Expr | ParamRef | int,
        tenant_value: PropertyInput | Expr | ParamRef | PropertyValueInput | None = None,
    ) -> "Traversal":
        return self._push(
            Step.vector_search_edges(
                label,
                property,
                PropertyInput.from_value(query_vector),
                StreamBound.from_value(k),
                None if tenant_value is None else PropertyInput.from_value(tenant_value),
            ),
            "edges",
        )

    def text_search_edges(
        self,
        label: str,
        property: str,
        query_text: str,
        k: int,
        tenant_value: PropertyValueInput | None = None,
    ) -> "Traversal":
        return self.text_search_edges_with(label, property, query_text, k, tenant_value)

    def text_search_edges_with(
        self,
        label: str,
        property: str,
        query_text: PropertyInput | Expr | ParamRef | PropertyValueInput,
        k: StreamBound | Expr | ParamRef | int,
        tenant_value: PropertyInput | Expr | ParamRef | PropertyValueInput | None = None,
    ) -> "Traversal":
        return self._push(
            Step.text_search_edges(
                label,
                property,
                PropertyInput.from_value(query_text),
                StreamBound.from_value(k),
                None if tenant_value is None else PropertyInput.from_value(tenant_value),
            ),
            "edges",
        )

    def create_index_if_not_exists(self, spec: IndexSpec) -> "Traversal":
        return self._push(Step.create_index(spec, True), "terminal", "write")

    def drop_index(self, spec: IndexSpec) -> "Traversal":
        return self._push(Step.drop_index(spec), "terminal", "write")

    def create_vector_index_nodes(self, label: str, property: str, tenant_property: str | None = None) -> "Traversal":
        return self.create_index_if_not_exists(IndexSpec.node_vector(label, property, tenant_property))

    def create_vector_index_edges(self, label: str, property: str, tenant_property: str | None = None) -> "Traversal":
        return self.create_index_if_not_exists(IndexSpec.edge_vector(label, property, tenant_property))

    def create_text_index_nodes(self, label: str, property: str, tenant_property: str | None = None) -> "Traversal":
        return self.create_index_if_not_exists(IndexSpec.node_text(label, property, tenant_property))

    def create_text_index_edges(self, label: str, property: str, tenant_property: str | None = None) -> "Traversal":
        return self.create_index_if_not_exists(IndexSpec.edge_text(label, property, tenant_property))

    def out(self, label: str | None = None) -> "Traversal":
        return self._push(Step.out(label), "nodes")

    def in_(self, label: str | None = None) -> "Traversal":
        return self._push(Step.in_(label), "nodes")

    def both(self, label: str | None = None) -> "Traversal":
        return self._push(Step.both(label), "nodes")

    def out_e(self, label: str | None = None) -> "Traversal":
        return self._push(Step.out_e(label), "edges")

    def in_e(self, label: str | None = None) -> "Traversal":
        return self._push(Step.in_e(label), "edges")

    def both_e(self, label: str | None = None) -> "Traversal":
        return self._push(Step.both_e(label), "edges")

    def out_n(self) -> "Traversal":
        return self._push(Step.out_n(), "nodes")

    def in_n(self) -> "Traversal":
        return self._push(Step.in_n(), "nodes")

    def other_n(self) -> "Traversal":
        return self._push(Step.other_n(), "nodes")

    def has(self, property: str, value: PropertyValueInput) -> "Traversal":
        return self._push(Step.has(property, value))

    def has_label(self, label: str) -> "Traversal":
        return self._push(Step.has_label(label))

    def has_key(self, property: str) -> "Traversal":
        return self._push(Step.has_key(property))

    def where(self, predicate: Predicate) -> "Traversal":
        return self._push(Step.where(predicate))

    where_ = where

    def dedup(self) -> "Traversal":
        return self._push(Step.dedup())

    def within(self, name: str) -> "Traversal":
        return self._push(Step.within(name))

    def without(self, name: str) -> "Traversal":
        return self._push(Step.without(name))

    def edge_has(self, property: str, value: PropertyInput | Expr | ParamRef | PropertyValueInput) -> "Traversal":
        return self._push(Step.edge_has(property, PropertyInput.from_value(value)))

    def edge_has_label(self, label: str) -> "Traversal":
        return self._push(Step.edge_has_label(label))

    def limit(self, n: StreamBound | Expr | ParamRef | int) -> "Traversal":
        return self._push(Step.limit(StreamBound.from_value(n)))

    def skip(self, n: StreamBound | Expr | ParamRef | int) -> "Traversal":
        return self._push(Step.skip(StreamBound.from_value(n)))

    def range(self, start: StreamBound | Expr | ParamRef | int, end: StreamBound | Expr | ParamRef | int) -> "Traversal":
        return self._push(Step.range(StreamBound.from_value(start), StreamBound.from_value(end)))

    def as_(self, name: str) -> "Traversal":
        return self._push(Step.as_(name))

    def store(self, name: str) -> "Traversal":
        return self._push(Step.store(name))

    def select(self, name: str) -> "Traversal":
        return self._push(Step.select(name))

    def inject(self, name: str) -> "Traversal":
        return self._push(Step.inject(name), "nodes")

    def count(self) -> "Traversal":
        return self._push(Step.count(), "terminal")

    def exists(self) -> "Traversal":
        return self._push(Step.exists(), "terminal")

    def id(self) -> "Traversal":
        return self._push(Step.id(), "terminal")

    def label(self) -> "Traversal":
        return self._push(Step.label(), "terminal")

    def values(self, properties: Iterable[str]) -> "Traversal":
        return self._push(Step.values(properties), "terminal")

    def value_map(self, properties: Iterable[str] | None = None) -> "Traversal":
        return self._push(Step.value_map(properties), "terminal")

    def project(self, projections: Iterable[ProjectionInput]) -> "Traversal":
        return self._push(Step.project(projections), "terminal")

    def edge_properties(self) -> "Traversal":
        return self._push(Step.edge_properties(), "terminal")

    def order_by(self, property: str, order: Order) -> "Traversal":
        return self._push(Step.order_by(property, order))

    def order_by_multiple(self, orderings: Iterable[tuple[str, Order]]) -> "Traversal":
        return self._push(Step.order_by_multiple(orderings))

    def repeat(self, config: RepeatConfig) -> "Traversal":
        return self._push(Step.repeat(config))

    def union(self, traversals: Iterable["SubTraversal"]) -> "Traversal":
        return self._push(Step.union(traversals))

    def choose(
        self,
        condition: Predicate,
        then_traversal: "SubTraversal",
        else_traversal: "SubTraversal | None" = None,
    ) -> "Traversal":
        return self._push(Step.choose(condition, then_traversal, else_traversal))

    def coalesce(self, traversals: Iterable["SubTraversal"]) -> "Traversal":
        return self._push(Step.coalesce(traversals))

    def optional(self, traversal: "SubTraversal") -> "Traversal":
        return self._push(Step.optional(traversal))

    def group(self, property: str) -> "Traversal":
        return self._push(Step.group(property), "terminal")

    def group_count(self, property: str) -> "Traversal":
        return self._push(Step.group_count(property), "terminal")

    def aggregate_by(self, fn: AggregateFunction, property: str) -> "Traversal":
        return self._push(Step.aggregate_by(fn, property), "terminal")

    def fold(self) -> "Traversal":
        return self._push(Step.fold())

    def unfold(self) -> "Traversal":
        return self._push(Step.unfold())

    def path(self) -> "Traversal":
        return self._push(Step.path())

    def simple_path(self) -> "Traversal":
        return self._push(Step.simple_path())

    def with_sack(self, initial: PropertyValueInput) -> "Traversal":
        return self._push(Step.with_sack(initial))

    def sack_set(self, property: str) -> "Traversal":
        return self._push(Step.sack_set(property))

    def sack_add(self, property: str) -> "Traversal":
        return self._push(Step.sack_add(property))

    def sack_get(self) -> "Traversal":
        return self._push(Step.sack_get())

    def add_n(self, label: str, properties: PropEntries | None = None) -> "Traversal":
        return self._push(Step.add_n(label, _property_entries(properties)), "nodes", "write")

    def add_e(self, label: str, to: NodeRef | NodeId | Iterable[NodeId] | str, properties: PropEntries | None = None) -> "Traversal":
        return self._push(Step.add_e(label, NodeRef.from_value(to), _property_entries(properties)), "nodes", "write")

    def set_property(self, name: str, value: PropertyInput | Expr | ParamRef | PropertyValueInput) -> "Traversal":
        return self._push(Step.set_property(name, PropertyInput.from_value(value)), "nodes", "write")

    def remove_property(self, name: str) -> "Traversal":
        return self._push(Step.remove_property(name), "nodes", "write")

    def drop(self) -> "Traversal":
        return self._push(Step.drop(), "nodes", "write")

    def drop_edge(self, to: NodeRef | NodeId | Iterable[NodeId] | str) -> "Traversal":
        return self._push(Step.drop_edge(NodeRef.from_value(to)), "nodes", "write")

    def drop_edge_labeled(self, to: NodeRef | NodeId | Iterable[NodeId] | str, label: str) -> "Traversal":
        return self._push(Step.drop_edge_labeled(NodeRef.from_value(to), label), "nodes", "write")

    def drop_edge_by_id(self, edges: EdgeRef | EdgeId | Iterable[EdgeId]) -> "Traversal":
        return self._push(Step.drop_edge_by_id(EdgeRef.from_value(edges)), "nodes", "write")


def g() -> Traversal:
    return Traversal.new()


@dataclass(frozen=True)
class SubTraversal:
    steps: tuple[Step, ...] = ()

    @classmethod
    def new(cls) -> "SubTraversal":
        return cls()

    @classmethod
    def from_steps(cls, steps: Iterable[Step]) -> "SubTraversal":
        return cls(tuple(steps))

    def _push(self, step: Step) -> "SubTraversal":
        return SubTraversal((*self.steps, step))

    def out(self, label: str | None = None) -> "SubTraversal":
        return self._push(Step.out(label))

    def in_(self, label: str | None = None) -> "SubTraversal":
        return self._push(Step.in_(label))

    def both(self, label: str | None = None) -> "SubTraversal":
        return self._push(Step.both(label))

    def out_e(self, label: str | None = None) -> "SubTraversal":
        return self._push(Step.out_e(label))

    def in_e(self, label: str | None = None) -> "SubTraversal":
        return self._push(Step.in_e(label))

    def both_e(self, label: str | None = None) -> "SubTraversal":
        return self._push(Step.both_e(label))

    def out_n(self) -> "SubTraversal":
        return self._push(Step.out_n())

    def in_n(self) -> "SubTraversal":
        return self._push(Step.in_n())

    def other_n(self) -> "SubTraversal":
        return self._push(Step.other_n())

    def has(self, property: str, value: PropertyValueInput) -> "SubTraversal":
        return self._push(Step.has(property, value))

    def has_label(self, label: str) -> "SubTraversal":
        return self._push(Step.has_label(label))

    def has_key(self, property: str) -> "SubTraversal":
        return self._push(Step.has_key(property))

    def where(self, predicate: Predicate) -> "SubTraversal":
        return self._push(Step.where(predicate))

    where_ = where

    def dedup(self) -> "SubTraversal":
        return self._push(Step.dedup())

    def within(self, name: str) -> "SubTraversal":
        return self._push(Step.within(name))

    def without(self, name: str) -> "SubTraversal":
        return self._push(Step.without(name))

    def edge_has(self, property: str, value: PropertyInput | Expr | ParamRef | PropertyValueInput) -> "SubTraversal":
        return self._push(Step.edge_has(property, PropertyInput.from_value(value)))

    def edge_has_label(self, label: str) -> "SubTraversal":
        return self._push(Step.edge_has_label(label))

    def limit(self, n: StreamBound | Expr | ParamRef | int) -> "SubTraversal":
        return self._push(Step.limit(StreamBound.from_value(n)))

    def skip(self, n: StreamBound | Expr | ParamRef | int) -> "SubTraversal":
        return self._push(Step.skip(StreamBound.from_value(n)))

    def range(self, start: StreamBound | Expr | ParamRef | int, end: StreamBound | Expr | ParamRef | int) -> "SubTraversal":
        return self._push(Step.range(StreamBound.from_value(start), StreamBound.from_value(end)))

    def as_(self, name: str) -> "SubTraversal":
        return self._push(Step.as_(name))

    def store(self, name: str) -> "SubTraversal":
        return self._push(Step.store(name))

    def select(self, name: str) -> "SubTraversal":
        return self._push(Step.select(name))

    def order_by(self, property: str, order: Order) -> "SubTraversal":
        return self._push(Step.order_by(property, order))

    def order_by_multiple(self, orderings: Iterable[tuple[str, Order]]) -> "SubTraversal":
        return self._push(Step.order_by_multiple(orderings))

    def path(self) -> "SubTraversal":
        return self._push(Step.path())

    def simple_path(self) -> "SubTraversal":
        return self._push(Step.simple_path())

    def to_json(self) -> JsonValue:
        return {"steps": list(self.steps)}


def sub() -> SubTraversal:
    return SubTraversal.new()


@dataclass(frozen=True)
class BatchCondition:
    variant: str
    payload: Any = None

    @classmethod
    def var_not_empty(cls, name: str) -> "BatchCondition":
        return cls("VarNotEmpty", name)

    @classmethod
    def var_empty(cls, name: str) -> "BatchCondition":
        return cls("VarEmpty", name)

    @classmethod
    def var_min_size(cls, name: str, size: int) -> "BatchCondition":
        return cls("VarMinSize", [name, _int_to_json(size)])

    @classmethod
    def prev_not_empty(cls) -> "BatchCondition":
        return cls("PrevNotEmpty")

    def to_json(self) -> JsonValue:
        if self.variant == "PrevNotEmpty":
            return _unit("PrevNotEmpty")
        if self.variant == "VarMinSize":
            return _tuple("VarMinSize", self.payload)
        return _newtype(self.variant, self.payload)


@dataclass(frozen=True)
class NamedQuery:
    name: str | None
    steps: list[Step]
    condition: BatchCondition | None = None

    def to_json(self) -> JsonValue:
        return {"name": self.name, "steps": self.steps, "condition": self.condition}


@dataclass(frozen=True)
class BatchEntry:
    variant: str
    payload: Any

    @classmethod
    def query(cls, query: NamedQuery) -> "BatchEntry":
        return cls("Query", query)

    @classmethod
    def for_each(cls, param_name: str, body: Iterable["BatchEntry"]) -> "BatchEntry":
        return cls("ForEach", {"param": param_name, "body": list(body)})

    def to_json(self) -> JsonValue:
        if self.variant == "Query":
            return _newtype("Query", self.payload)
        return _struct("ForEach", self.payload)


@dataclass(frozen=True)
class ReadBatch:
    queries: tuple[BatchEntry, ...] = ()
    returns: tuple[str, ...] = ()

    @classmethod
    def new(cls) -> "ReadBatch":
        return cls()

    def var_as(self, name: str, traversal: Traversal) -> "ReadBatch":
        if traversal.mode != "read":
            raise TypeError("ReadBatch.var_as only accepts read-only traversals")
        return ReadBatch(
            (*self.queries, BatchEntry.query(NamedQuery(name, traversal.into_steps(), None))),
            self.returns,
        )

    def var_as_if(self, name: str, condition: BatchCondition, traversal: Traversal) -> "ReadBatch":
        if traversal.mode != "read":
            raise TypeError("ReadBatch.var_as_if only accepts read-only traversals")
        return ReadBatch(
            (*self.queries, BatchEntry.query(NamedQuery(name, traversal.into_steps(), condition))),
            self.returns,
        )

    def for_each_param(self, param_name: str, body: "ReadBatch") -> "ReadBatch":
        return ReadBatch((*self.queries, BatchEntry.for_each(param_name, body.queries)), self.returns)

    def returning(self, vars: Iterable[str]) -> "ReadBatch":
        return ReadBatch(self.queries, tuple(vars))

    def to_json(self) -> JsonValue:
        return {"queries": list(self.queries), "returns": list(self.returns)}

    def to_json_string(self) -> str:
        return stringify_json(self)

    def to_json_bytes(self) -> bytes:
        return self.to_json_string().encode("utf-8")

    def to_dynamic_request(
        self,
        params: "DefinedParams | None" = None,
        values: Mapping[str, Any] | None = None,
        *,
        query_name: str | None | object = _UNSET,
    ) -> "DynamicQueryRequest":
        request = DynamicQueryRequest.read(self)
        return _build_dynamic_request(request, params, values, query_name=query_name)

    def to_dynamic_json(
        self,
        params: "DefinedParams | None" = None,
        values: Mapping[str, Any] | None = None,
        *,
        query_name: str | None | object = _UNSET,
    ) -> str:
        return self.to_dynamic_request(params, values, query_name=query_name).to_json_string()

    def to_dynamic_bytes(
        self,
        params: "DefinedParams | None" = None,
        values: Mapping[str, Any] | None = None,
        *,
        query_name: str | None | object = _UNSET,
    ) -> bytes:
        return self.to_dynamic_request(params, values, query_name=query_name).to_json_bytes()


@dataclass(frozen=True)
class WriteBatch:
    queries: tuple[BatchEntry, ...] = ()
    returns: tuple[str, ...] = ()

    @classmethod
    def new(cls) -> "WriteBatch":
        return cls()

    def var_as(self, name: str, traversal: Traversal) -> "WriteBatch":
        return WriteBatch(
            (*self.queries, BatchEntry.query(NamedQuery(name, traversal.into_steps(), None))),
            self.returns,
        )

    def var_as_if(self, name: str, condition: BatchCondition, traversal: Traversal) -> "WriteBatch":
        return WriteBatch(
            (*self.queries, BatchEntry.query(NamedQuery(name, traversal.into_steps(), condition))),
            self.returns,
        )

    def for_each_param(self, param_name: str, body: "WriteBatch") -> "WriteBatch":
        return WriteBatch((*self.queries, BatchEntry.for_each(param_name, body.queries)), self.returns)

    def returning(self, vars: Iterable[str]) -> "WriteBatch":
        return WriteBatch(self.queries, tuple(vars))

    def to_json(self) -> JsonValue:
        return {"queries": list(self.queries), "returns": list(self.returns)}

    def to_json_string(self) -> str:
        return stringify_json(self)

    def to_json_bytes(self) -> bytes:
        return self.to_json_string().encode("utf-8")

    def to_dynamic_request(
        self,
        params: "DefinedParams | None" = None,
        values: Mapping[str, Any] | None = None,
        *,
        query_name: str | None | object = _UNSET,
    ) -> "DynamicQueryRequest":
        request = DynamicQueryRequest.write(self)
        return _build_dynamic_request(request, params, values, query_name=query_name)

    def to_dynamic_json(
        self,
        params: "DefinedParams | None" = None,
        values: Mapping[str, Any] | None = None,
        *,
        query_name: str | None | object = _UNSET,
    ) -> str:
        return self.to_dynamic_request(params, values, query_name=query_name).to_json_string()

    def to_dynamic_bytes(
        self,
        params: "DefinedParams | None" = None,
        values: Mapping[str, Any] | None = None,
        *,
        query_name: str | None | object = _UNSET,
    ) -> bytes:
        return self.to_dynamic_request(params, values, query_name=query_name).to_json_bytes()


def read_batch() -> ReadBatch:
    return ReadBatch.new()


def write_batch() -> WriteBatch:
    return WriteBatch.new()


@dataclass(frozen=True)
class QueryParamType:
    variant: str
    inner: "QueryParamType | None" = None

    @classmethod
    def bool(cls) -> "QueryParamType":
        return cls("Bool")

    @classmethod
    def i64(cls) -> "QueryParamType":
        return cls("I64")

    @classmethod
    def f64(cls) -> "QueryParamType":
        return cls("F64")

    @classmethod
    def f32(cls) -> "QueryParamType":
        return cls("F32")

    @classmethod
    def string(cls) -> "QueryParamType":
        return cls("String")

    @classmethod
    def date_time(cls) -> "QueryParamType":
        return cls("DateTime")

    datetime = date_time

    @classmethod
    def bytes(cls) -> "QueryParamType":
        return cls("Bytes")

    @classmethod
    def value(cls) -> "QueryParamType":
        return cls("Value")

    @classmethod
    def object(cls) -> "QueryParamType":
        return cls("Object")

    @classmethod
    def array(cls, inner: "QueryParamType") -> "QueryParamType":
        return cls("Array", inner)

    def to_json(self) -> JsonValue:
        return _newtype("Array", self.inner) if self.variant == "Array" else _unit(self.variant)


@dataclass(frozen=True)
class QueryParameter:
    name: str
    ty: QueryParamType

    def to_json(self) -> JsonValue:
        return {"name": self.name, "ty": self.ty}


@dataclass(frozen=True)
class ParamSchema:
    kind: str
    inner: "ParamSchema | None" = None
    object_inner: "ParamSchema | None" = None

    def to_param_type(self) -> QueryParamType:
        if self.kind == "Bool":
            return QueryParamType.bool()
        if self.kind == "I64":
            return QueryParamType.i64()
        if self.kind == "F64":
            return QueryParamType.f64()
        if self.kind == "F32":
            return QueryParamType.f32()
        if self.kind == "String":
            return QueryParamType.string()
        if self.kind == "DateTime":
            return QueryParamType.date_time()
        if self.kind == "Bytes":
            return QueryParamType.bytes()
        if self.kind == "Value":
            return QueryParamType.value()
        if self.kind == "Object":
            return QueryParamType.object()
        if self.kind == "Array":
            if self.inner is None:
                raise TypeError("array parameter schema requires an inner schema")
            return QueryParamType.array(self.inner.to_param_type())
        raise TypeError(f"unknown parameter schema: {self.kind}")

    def to_json(self) -> JsonValue:
        return self.to_param_type().to_json()


class _ParamNamespace:
    def bool(self) -> ParamSchema:
        return ParamSchema("Bool")

    def i64(self) -> ParamSchema:
        return ParamSchema("I64")

    def f64(self) -> ParamSchema:
        return ParamSchema("F64")

    def f32(self) -> ParamSchema:
        return ParamSchema("F32")

    def string(self) -> ParamSchema:
        return ParamSchema("String")

    def date_time(self) -> ParamSchema:
        return ParamSchema("DateTime")

    datetime = date_time

    def bytes(self) -> ParamSchema:
        return ParamSchema("Bytes")

    def value(self) -> ParamSchema:
        return ParamSchema("Value")

    def object(self, inner: ParamSchema | None = None) -> ParamSchema:
        return ParamSchema("Object", object_inner=inner or self.value())

    def array(self, inner: ParamSchema) -> ParamSchema:
        return ParamSchema("Array", inner=inner)


param = _ParamNamespace()


@dataclass(frozen=True)
class ParamRef:
    name: str
    schema: ParamSchema

    def to_expr(self) -> Expr:
        return Expr.param(self.name)

    def input(self) -> PropertyInput:
        return PropertyInput.param(self.name)

    def bound(self) -> StreamBound:
        return StreamBound.expr(self)

    def to_json(self) -> JsonValue:
        return self.to_expr().to_json()


class DefinedParams:
    def __init__(self, schema: Mapping[str, ParamSchema]) -> None:
        self.schema = dict(schema)
        self._refs = {name: ParamRef(name, param_schema) for name, param_schema in self.schema.items()}

    def __getattr__(self, name: str) -> ParamRef:
        try:
            return self._refs[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __getitem__(self, name: str) -> ParamRef:
        return self._refs[name]

    def refs(self) -> Mapping[str, ParamRef]:
        return dict(self._refs)


def define_params(schema: Mapping[str, ParamSchema]) -> DefinedParams:
    return DefinedParams(schema)


def _parameters_for_params(params: DefinedParams) -> list[QueryParameter]:
    return [QueryParameter(name, schema.to_param_type()) for name, schema in params.schema.items()]


def _reject_unknown_parameters(input_values: Mapping[str, Any], expected: Iterable[str]) -> None:
    allowed = set(expected)
    for key in input_values:
        if key not in allowed:
            raise TypeError(f"unknown parameter: {key}")


def _convert_input_for_params(params: DefinedParams, input_values: Mapping[str, Any]) -> dict[str, JsonValue]:
    return _convert_input_from_schema(params.schema, input_values)


def _convert_input_from_schema(
    schema: Mapping[str, ParamSchema], input_values: Mapping[str, Any]
) -> dict[str, JsonValue]:
    out: dict[str, JsonValue] = {}
    for name, param_schema in schema.items():
        if name not in input_values:
            raise TypeError(f"missing required parameter: {name}")
        out[name] = _convert_param_value(param_schema, input_values[name], name)
    return out


def _convert_param_value(schema: ParamSchema, value: Any, path: str) -> JsonValue:
    if schema.kind == "Bool":
        if not isinstance(value, bool):
            raise TypeError(f"parameter '{path}' must be boolean")
        return value
    if schema.kind == "I64":
        return _int_to_json(value)
    if schema.kind in {"F64", "F32"}:
        return _finite_float(value)
    if schema.kind == "String":
        if not isinstance(value, str):
            raise TypeError(f"parameter '{path}' must be string")
        return value
    if schema.kind == "DateTime":
        if isinstance(value, DateTime):
            dt = value
        elif isinstance(value, datetime):
            dt = DateTime.from_datetime(value)
        elif isinstance(value, str):
            dt = DateTime.parse_rfc3339(value)
        else:
            dt = DateTime.from_millis(value)
        return _datetime_to_rfc3339(dt, path)
    if schema.kind == "Bytes":
        raise DynamicQueryError.unsupported_bytes(path)
    if schema.kind == "Value":
        return _dynamic_from_property_value(PropertyValue.from_value(value), path)
    if schema.kind == "Object":
        if not isinstance(value, Mapping):
            raise TypeError(f"parameter '{path}' must be object")
        inner = schema.object_inner or param.value()
        return {
            key: _convert_param_value(inner, entry, f"{path}.{key}")
            for key, entry in value.items()
        }
    if schema.kind == "Array":
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            raise TypeError(f"parameter '{path}' must be array")
        if schema.inner is None:
            raise TypeError(f"parameter '{path}' array schema requires an inner schema")
        return [
            _convert_param_value(schema.inner, entry, f"{path}[{index}]")
            for index, entry in enumerate(value)
        ]
    raise TypeError(f"unknown parameter schema: {schema.kind}")


def _dynamic_from_property_value(value: PropertyValue, path: str) -> JsonValue:
    if value.variant == "Null":
        return None
    if value.variant in {"Bool", "I64", "F64", "F32", "String"}:
        return value.payload
    if value.variant == "DateTime":
        return _datetime_to_rfc3339(DateTime.from_millis(value.payload), path)
    if value.variant == "Bytes":
        raise DynamicQueryError.unsupported_bytes(path)
    if value.variant in {"I64Array", "F64Array", "F32Array", "StringArray"}:
        return value.payload
    if value.variant == "Array":
        return [
            _dynamic_from_property_value(entry, f"{path}[{index}]")
            for index, entry in enumerate(value.payload)
        ]
    if value.variant == "Object":
        return {
            key: _dynamic_from_property_value(entry, f"{path}.{key}")
            for key, entry in value.payload.items()
        }
    raise TypeError(f"unsupported property value variant: {value.variant}")


class DynamicQueryRequestType(str, Enum):
    READ = "read"
    WRITE = "write"


DynamicQueryRequestType.Read = DynamicQueryRequestType.READ  # type: ignore[attr-defined]
DynamicQueryRequestType.Write = DynamicQueryRequestType.WRITE  # type: ignore[attr-defined]


class _DynamicQueryValueNamespace:
    def null(self) -> JsonValue:
        return None

    def bool(self, value: bool) -> JsonValue:
        return bool(value)

    def i64(self, value: int) -> JsonValue:
        return _int_to_json(value)

    def f64(self, value: float) -> JsonValue:
        return _finite_float(value)

    def f32(self, value: float) -> JsonValue:
        return _finite_float(value)

    def string(self, value: str) -> JsonValue:
        return str(value)

    def array(self, values: Iterable[JsonValue]) -> JsonValue:
        return list(values)

    def object(self, values: Mapping[str, JsonValue]) -> JsonValue:
        return dict(values)


DynamicQueryValue = _DynamicQueryValueNamespace()
BatchQuery: TypeAlias = ReadBatch | WriteBatch


@dataclass
class DynamicQueryRequest:
    request_type: DynamicQueryRequestType
    query: BatchQuery
    query_name: str | None = None
    parameters: dict[str, JsonValue] | None = None
    parameter_types: dict[str, QueryParamType] | None = None

    @classmethod
    def read(cls, query: ReadBatch, query_name: str | None = None) -> "DynamicQueryRequest":
        return cls(DynamicQueryRequestType.READ, query, query_name)

    @classmethod
    def write(cls, query: WriteBatch, query_name: str | None = None) -> "DynamicQueryRequest":
        return cls(DynamicQueryRequestType.WRITE, query, query_name)

    def insert_parameter_value(self, name: str, value: JsonValue) -> None:
        if self.parameters is None:
            self.parameters = {}
        self.parameters[name] = value

    def insert_parameter_type(self, name: str, ty: QueryParamType) -> None:
        if self.parameter_types is None:
            self.parameter_types = {}
        self.parameter_types[name] = ty

    def with_parameter_value(self, name: str, value: JsonValue) -> "DynamicQueryRequest":
        self.insert_parameter_value(name, value)
        return self

    def with_parameter_type(self, name: str, ty: QueryParamType) -> "DynamicQueryRequest":
        self.insert_parameter_type(name, ty)
        return self

    def set_query_name(self, name: str) -> None:
        self.query_name = name

    def clear_query_name(self) -> None:
        self.query_name = None

    def with_query_name(self, name: str) -> "DynamicQueryRequest":
        self.set_query_name(name)
        return self

    def to_json(self) -> JsonValue:
        return {
            "request_type": self.request_type,
            "query_name": self.query_name,
            "query": self.query,
            "parameters": self.parameters if self.parameters is not None else _OMIT,
            "parameter_types": self.parameter_types if self.parameter_types is not None else _OMIT,
        }

    def to_json_string(self) -> str:
        return stringify_json(self)

    def to_json_bytes(self) -> bytes:
        return self.to_json_string().encode("utf-8")


def _add_dynamic_parameters(
    request: DynamicQueryRequest,
    params: DefinedParams | None,
    values: Mapping[str, Any] | None,
) -> DynamicQueryRequest:
    if params is None:
        return request
    if values is None:
        raise TypeError("dynamic parameter values are required when a parameter schema is provided")
    parameters = _parameters_for_params(params)
    _reject_unknown_parameters(values, [parameter.name for parameter in parameters])
    converted = _convert_input_for_params(params, values)
    for parameter in parameters:
        request.insert_parameter_type(parameter.name, parameter.ty)
    for name, value in converted.items():
        request.insert_parameter_value(name, value)
    return request


def _apply_query_name(
    request: DynamicQueryRequest, query_name: str | None | object = _UNSET
) -> DynamicQueryRequest:
    if query_name is _UNSET:
        return request
    if query_name is None:
        request.clear_query_name()
    else:
        request.set_query_name(query_name)  # type: ignore[arg-type]
    return request


def _build_dynamic_request(
    request: DynamicQueryRequest,
    params: DefinedParams | None = None,
    values: Mapping[str, Any] | None = None,
    *,
    query_name: str | None | object = _UNSET,
) -> DynamicQueryRequest:
    if params is None and values is not None:
        raise TypeError("dynamic parameter values require a parameter schema")
    return _apply_query_name(_add_dynamic_parameters(request, params, values), query_name)


@dataclass(frozen=True)
class RegisteredQuery:
    kind: str
    build: Callable[[], BatchQuery]
    parameters: Callable[[], list[QueryParameter]]
    convert_input: Callable[[Mapping[str, Any]], dict[str, JsonValue]] | None = None


def register_read(builder: Callable[[DefinedParams], ReadBatch], params: DefinedParams) -> RegisteredQuery:
    return RegisteredQuery(
        "read",
        lambda: builder(params),
        lambda: _parameters_for_params(params),
        lambda input_values: _convert_input_for_params(params, input_values),
    )


def register_write(builder: Callable[[DefinedParams], WriteBatch], params: DefinedParams) -> RegisteredQuery:
    return RegisteredQuery(
        "write",
        lambda: builder(params),
        lambda: _parameters_for_params(params),
        lambda input_values: _convert_input_for_params(params, input_values),
    )


QueryDefinitions: TypeAlias = Mapping[str, Mapping[str, RegisteredQuery]]


class DefinedQueries:
    def __init__(self, definitions: QueryDefinitions) -> None:
        self.definitions = {
            "read": dict(definitions.get("read", {})),
            "write": dict(definitions.get("write", {})),
        }
        _assert_unique_route_names(self.definitions)
        self.call = _QueryCallMap(self.definitions)

    def build_query_bundle(self) -> "QueryBundle":
        return build_query_bundle(self.definitions)

    def generate(self, path: str | Path = "queries.json") -> str:
        return generate_to_path(self.definitions, path)


class _QueryCallMap:
    def __init__(self, definitions: dict[str, dict[str, RegisteredQuery]]) -> None:
        self._definitions = definitions

    def __getattr__(self, name: str) -> Callable[[Mapping[str, Any] | None], DynamicQueryRequest]:
        route = self._definitions["read"].get(name) or self._definitions["write"].get(name)
        if route is None:
            raise AttributeError(name)
        return lambda input_values=None: _build_registered_request(name, route, input_values or {})

    def __getitem__(self, name: str) -> Callable[[Mapping[str, Any] | None], DynamicQueryRequest]:
        return getattr(self, name)


@dataclass(frozen=True)
class QueryBundle:
    version: int
    read_routes: Mapping[str, ReadBatch]
    write_routes: Mapping[str, WriteBatch]
    read_parameters: Mapping[str, list[QueryParameter]]
    write_parameters: Mapping[str, list[QueryParameter]]

    def to_json(self) -> JsonValue:
        return {
            "version": self.version,
            "read_routes": _sorted_object(self.read_routes),
            "write_routes": _sorted_object(self.write_routes),
            "read_parameters": _sorted_object(self.read_parameters),
            "write_parameters": _sorted_object(self.write_parameters),
        }


def _sorted_object(values: Mapping[str, Any]) -> dict[str, Any]:
    return {key: values[key] for key in sorted(values)}


def _build_registered_request(
    name: str,
    route: RegisteredQuery,
    input_values: Mapping[str, Any],
) -> DynamicQueryRequest:
    built = route.build()
    request = DynamicQueryRequest.read(built) if route.kind == "read" else DynamicQueryRequest.write(built)  # type: ignore[arg-type]
    request.set_query_name(name)
    parameters = route.parameters()
    _reject_unknown_parameters(input_values, [parameter.name for parameter in parameters])
    values = (
        route.convert_input(input_values)
        if route.convert_input is not None
        else _convert_input_from_schema(_parameters_to_schemas(parameters), input_values)
    )
    for parameter in parameters:
        request.insert_parameter_type(parameter.name, parameter.ty)
    for param_name, value in values.items():
        request.insert_parameter_value(param_name, value)
    return request


def _parameters_to_schemas(parameters: Iterable[QueryParameter]) -> dict[str, ParamSchema]:
    return {parameter.name: _schema_from_param_type(parameter.ty) for parameter in parameters}


def _schema_from_param_type(ty: QueryParamType) -> ParamSchema:
    if ty.variant == "Bool":
        return param.bool()
    if ty.variant == "I64":
        return param.i64()
    if ty.variant == "F64":
        return param.f64()
    if ty.variant == "F32":
        return param.f32()
    if ty.variant == "String":
        return param.string()
    if ty.variant == "DateTime":
        return param.date_time()
    if ty.variant == "Bytes":
        return param.bytes()
    if ty.variant == "Value":
        return param.value()
    if ty.variant == "Object":
        return param.object()
    if ty.variant == "Array":
        if ty.inner is None:
            raise TypeError("array parameter type requires an inner type")
        return param.array(_schema_from_param_type(ty.inner))
    raise TypeError(f"unknown parameter type: {ty.variant}")


def _assert_unique_route_names(definitions: Mapping[str, Mapping[str, RegisteredQuery]]) -> None:
    names: set[str] = set()
    for name in definitions.get("read", {}):
        if name in names:
            raise GenerateError.duplicate_query_name(name)
        names.add(name)
    for name in definitions.get("write", {}):
        if name in names:
            raise GenerateError.duplicate_query_name(name)
        names.add(name)


def define_queries(definitions: QueryDefinitions) -> DefinedQueries:
    return DefinedQueries(definitions)


def build_query_bundle(definitions: QueryDefinitions) -> QueryBundle:
    _assert_unique_route_names(definitions)
    read_routes: dict[str, ReadBatch] = {}
    write_routes: dict[str, WriteBatch] = {}
    read_parameters: dict[str, list[QueryParameter]] = {}
    write_parameters: dict[str, list[QueryParameter]] = {}
    for name, route in definitions.get("read", {}).items():
        read_routes[name] = route.build()  # type: ignore[assignment]
        read_parameters[name] = route.parameters()
    for name, route in definitions.get("write", {}).items():
        write_routes[name] = route.build()  # type: ignore[assignment]
        write_parameters[name] = route.parameters()
    return QueryBundle(
        QUERY_BUNDLE_VERSION,
        read_routes,
        write_routes,
        read_parameters,
        write_parameters,
    )


def serialize_query_bundle(bundle: QueryBundle) -> str:
    return stringify_json(bundle, pretty=True)


def deserialize_query_bundle(data: str | bytes) -> JsonValue:
    parsed = json.loads(data)
    found = parsed.get("version", -1) if isinstance(parsed, dict) else -1
    if found != QUERY_BUNDLE_VERSION:
        raise GenerateError.unsupported_version(found, QUERY_BUNDLE_VERSION)
    return parsed


def write_query_bundle_to_path(bundle: QueryBundle, path: str | Path) -> None:
    Path(path).write_text(serialize_query_bundle(bundle), encoding="utf-8")


def read_query_bundle_from_path(path: str | Path) -> JsonValue:
    return deserialize_query_bundle(Path(path).read_text(encoding="utf-8"))


def generate_to_path(definitions: QueryDefinitions, path: str | Path) -> str:
    write_query_bundle_to_path(build_query_bundle(definitions), path)
    return str(path)


def generate(definitions: QueryDefinitions) -> str:
    return generate_to_path(definitions, "queries.json")


def _install_aliases() -> None:
    aliases = {
        Traversal: {
            "nWhere": "n_where",
            "nWithLabel": "n_with_label",
            "nWithLabelWhere": "n_with_label_where",
            "eWhere": "e_where",
            "eWithLabel": "e_with_label",
            "eWithLabelWhere": "e_with_label_where",
            "vectorSearchNodes": "vector_search_nodes",
            "vectorSearchNodesWith": "vector_search_nodes_with",
            "textSearchNodes": "text_search_nodes",
            "textSearchNodesWith": "text_search_nodes_with",
            "vectorSearchEdges": "vector_search_edges",
            "vectorSearchEdgesWith": "vector_search_edges_with",
            "textSearchEdges": "text_search_edges",
            "textSearchEdgesWith": "text_search_edges_with",
            "createIndexIfNotExists": "create_index_if_not_exists",
            "dropIndex": "drop_index",
            "createVectorIndexNodes": "create_vector_index_nodes",
            "createVectorIndexEdges": "create_vector_index_edges",
            "createTextIndexNodes": "create_text_index_nodes",
            "createTextIndexEdges": "create_text_index_edges",
            "outE": "out_e",
            "inE": "in_e",
            "bothE": "both_e",
            "outN": "out_n",
            "inN": "in_n",
            "otherN": "other_n",
            "hasLabel": "has_label",
            "hasKey": "has_key",
            "edgeHas": "edge_has",
            "edgeHasLabel": "edge_has_label",
            "valueMap": "value_map",
            "edgeProperties": "edge_properties",
            "orderBy": "order_by",
            "orderByMultiple": "order_by_multiple",
            "groupCount": "group_count",
            "aggregateBy": "aggregate_by",
            "simplePath": "simple_path",
            "withSack": "with_sack",
            "sackSet": "sack_set",
            "sackAdd": "sack_add",
            "sackGet": "sack_get",
            "addN": "add_n",
            "addE": "add_e",
            "setProperty": "set_property",
            "removeProperty": "remove_property",
            "dropEdge": "drop_edge",
            "dropEdgeLabeled": "drop_edge_labeled",
            "dropEdgeById": "drop_edge_by_id",
        },
        SubTraversal: {
            "outE": "out_e",
            "inE": "in_e",
            "bothE": "both_e",
            "outN": "out_n",
            "inN": "in_n",
            "otherN": "other_n",
            "hasLabel": "has_label",
            "hasKey": "has_key",
            "edgeHas": "edge_has",
            "edgeHasLabel": "edge_has_label",
            "orderBy": "order_by",
            "orderByMultiple": "order_by_multiple",
            "simplePath": "simple_path",
        },
        ReadBatch: {
            "varAs": "var_as",
            "varAsIf": "var_as_if",
            "forEachParam": "for_each_param",
            "toJsonString": "to_json_string",
            "toJsonBytes": "to_json_bytes",
            "toDynamicRequest": "to_dynamic_request",
            "toDynamicJson": "to_dynamic_json",
            "toDynamicBytes": "to_dynamic_bytes",
        },
        WriteBatch: {
            "varAs": "var_as",
            "varAsIf": "var_as_if",
            "forEachParam": "for_each_param",
            "toJsonString": "to_json_string",
            "toJsonBytes": "to_json_bytes",
            "toDynamicRequest": "to_dynamic_request",
            "toDynamicJson": "to_dynamic_json",
            "toDynamicBytes": "to_dynamic_bytes",
        },
        Predicate: {
            "hasKey": "has_key",
            "isNull": "is_null",
            "isNotNull": "is_not_null",
            "startsWith": "starts_with",
            "endsWith": "ends_with",
            "containsExpr": "contains_expr",
            "containsParam": "contains_param",
            "isIn": "is_in",
            "isInExpr": "is_in_expr",
            "isInParam": "is_in_param",
            "eqParam": "eq_param",
            "neqParam": "neq_param",
            "gtParam": "gt_param",
            "gteParam": "gte_param",
            "ltParam": "lt_param",
            "lteParam": "lte_param",
            "fromSource": "from_source",
        },
        SourcePredicate: {
            "hasKey": "has_key",
            "startsWith": "starts_with",
            "toPredicate": "to_predicate",
        },
        BatchCondition: {
            "varNotEmpty": "var_not_empty",
            "varEmpty": "var_empty",
            "varMinSize": "var_min_size",
            "prevNotEmpty": "prev_not_empty",
        },
        RepeatConfig: {
            "emitAll": "emit_all",
            "emitBefore": "emit_before",
            "emitAfter": "emit_after",
            "emitIf": "emit_if",
            "maxDepth": "max_depth",
        },
        DynamicQueryRequest: {
            "insertParameterValue": "insert_parameter_value",
            "insertParameterType": "insert_parameter_type",
            "withParameterValue": "with_parameter_value",
            "withParameterType": "with_parameter_type",
            "setQueryName": "set_query_name",
            "clearQueryName": "clear_query_name",
            "withQueryName": "with_query_name",
            "toJsonString": "to_json_string",
            "toJsonBytes": "to_json_bytes",
        },
        DateTime: {
            "fromMillis": "from_millis",
            "fromDatetime": "from_datetime",
            "parseRfc3339": "parse_rfc3339",
            "toRfc3339": "to_rfc3339",
        },
        PropertyValue: {
            "dateTime": "date_time",
            "datetimeMillis": "datetime_millis",
            "i64Array": "i64_array",
            "f64Array": "f64_array",
            "f32Array": "f32_array",
            "stringArray": "string_array",
            "fromValue": "from_value",
            "asStr": "as_str",
            "asI64": "as_i64",
            "asDatetimeMillis": "as_datetime_millis",
            "asF64": "as_f64",
            "asBool": "as_bool",
            "asArray": "as_array",
            "asObject": "as_object",
        },
        PropertyInput: {
            "fromValue": "from_value",
            "toExpr": "to_expr",
        },
        NodeRef: {"fromValue": "from_value"},
        EdgeRef: {"fromValue": "from_value"},
        Expr: {
            "dateTime": "date_time_now",
        },
        StreamBound: {"fromValue": "from_value"},
        Projection: {"fromValue": "from_value"},
        IndexSpec: {
            "nodeEquality": "node_equality",
            "nodeUniqueEquality": "node_unique_equality",
            "nodeRange": "node_range",
            "edgeEquality": "edge_equality",
            "edgeRange": "edge_range",
            "nodeVector": "node_vector",
            "nodeText": "node_text",
            "edgeVector": "edge_vector",
            "edgeText": "edge_text",
        },
        QueryParamType: {
            "dateTime": "date_time",
        },
        ParamSchema: {"toParamType": "to_param_type"},
        ParamRef: {"toExpr": "to_expr"},
    }
    for cls, cls_aliases in aliases.items():
        for alias, target in cls_aliases.items():
            setattr(cls, alias, getattr(cls, target))


_install_aliases()

readBatch = read_batch
writeBatch = write_batch
defineParams = define_params
defineQueries = define_queries
registerRead = register_read
registerWrite = register_write
buildQueryBundle = build_query_bundle
serializeQueryBundle = serialize_query_bundle
deserializeQueryBundle = deserialize_query_bundle
writeQueryBundleToPath = write_query_bundle_to_path
readQueryBundleFromPath = read_query_bundle_from_path
generateToPath = generate_to_path
bytes_value = bytes_


prelude = {
    "g": g,
    "sub": sub,
    "read_batch": read_batch,
    "write_batch": write_batch,
    "define_params": define_params,
    "define_queries": define_queries,
    "register_read": register_read,
    "register_write": register_write,
    "param": param,
    "DateTime": DateTime,
    "DynamicQueryRequest": DynamicQueryRequest,
    "DynamicQueryRequestType": DynamicQueryRequestType,
    "DynamicQueryValue": DynamicQueryValue,
    "PropertyValue": PropertyValue,
    "PropertyInput": PropertyInput,
    "NodeRef": NodeRef,
    "EdgeRef": EdgeRef,
    "Expr": Expr,
    "StreamBound": StreamBound,
    "CompareOp": CompareOp,
    "Predicate": Predicate,
    "SourcePredicate": SourcePredicate,
    "PropertyProjection": PropertyProjection,
    "ExprProjection": ExprProjection,
    "Projection": Projection,
    "Order": Order,
    "EmitBehavior": EmitBehavior,
    "AggregateFunction": AggregateFunction,
    "RepeatConfig": RepeatConfig,
    "IndexSpec": IndexSpec,
    "RangeIndexDirection": RangeIndexDirection,
    "Traversal": Traversal,
    "SubTraversal": SubTraversal,
    "ReadBatch": ReadBatch,
    "WriteBatch": WriteBatch,
    "BatchCondition": BatchCondition,
    "BatchEntry": BatchEntry,
    "QueryParamType": QueryParamType,
}


__all__ = [
    "AggregateFunction",
    "BatchCondition",
    "BatchEntry",
    "BatchQuery",
    "CompareOp",
    "DateTime",
    "DateTimeLiteral",
    "DefinedParams",
    "DefinedQueries",
    "DynamicQueryError",
    "DynamicQueryRequest",
    "DynamicQueryRequestType",
    "DynamicQueryValue",
    "EdgeId",
    "EdgeRef",
    "EmitBehavior",
    "Expr",
    "ExprProjection",
    "GenerateError",
    "BytesLiteral",
    "F32Literal",
    "F64Literal",
    "I64Literal",
    "IndexSpec",
    "RangeIndexDirection",
    "JsonValue",
    "NodeId",
    "NodeRef",
    "Order",
    "ParamRef",
    "ParamSchema",
    "ParamObject",
    "ParamValue",
    "Predicate",
    "Projection",
    "PropertyInput",
    "PropertyMap",
    "PropertyProjection",
    "PropertyValue",
    "QueryBundle",
    "QueryParamType",
    "QueryParameter",
    "ReadBatch",
    "RegisteredQuery",
    "RepeatConfig",
    "SourcePredicate",
    "Step",
    "StreamBound",
    "SubTraversal",
    "Traversal",
    "WriteBatch",
    "build_query_bundle",
    "buildQueryBundle",
    "bytes_",
    "bytes_value",
    "canonicalize_json",
    "date_time",
    "define_params",
    "define_queries",
    "defineParams",
    "defineQueries",
    "deserialize_query_bundle",
    "deserializeQueryBundle",
    "f32",
    "f64",
    "g",
    "generate",
    "generate_to_path",
    "generateToPath",
    "i64",
    "param",
    "parse_json_structural",
    "prelude",
    "read_batch",
    "read_query_bundle_from_path",
    "readBatch",
    "readQueryBundleFromPath",
    "register_read",
    "register_write",
    "registerRead",
    "registerWrite",
    "serialize_query_bundle",
    "serializeQueryBundle",
    "structural_json_equal",
    "stringify_json",
    "sub",
    "write_batch",
    "write_query_bundle_to_path",
    "writeBatch",
    "writeQueryBundleToPath",
]
