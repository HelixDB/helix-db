package helix

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var (
	ErrWriteTraversalInReadBatch = errors.New("helix: read batch cannot contain write traversal")
	ErrUnsupportedBytesParameter = errors.New("helix: query JSON cannot represent bytes parameters")
	ErrDuplicateParameter        = errors.New("helix: duplicate parameter")
	ErrEmptyParameterName        = errors.New("helix: parameter name must not be empty")
	ErrMixedParameterModes       = errors.New("helix: typed and untyped parameters cannot be mixed")
	ErrInvalidParameterType      = errors.New("helix: invalid parameter type")
	ErrInvalidDateTimeParameter  = errors.New("helix: invalid datetime parameter")
)

type PathError struct {
	Path string
	Err  error
}

func (e *PathError) Error() string {
	if e.Path == "" {
		return e.Err.Error()
	}
	return e.Path + ": " + e.Err.Error()
}

func (e *PathError) Unwrap() error { return e.Err }

type Request interface {
	json.Marshaler
	Validate() error
	isHelixRequest()
}

func snakeName(name string) string {
	runes := []rune(name)
	var out strings.Builder
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				nextIsLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
				if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextIsLower) {
					out.WriteByte('_')
				}
			}
			out.WriteRune(unicode.ToLower(r))
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

func astUnit(name string) any { return snakeName(name) }

func astNewtype(name string, value any) any {
	return map[string]any{snakeName(name): value}
}

func astStruct(name string, fields map[string]any) any {
	for key, value := range fields {
		if value == nil {
			delete(fields, key)
		}
	}
	return map[string]any{snakeName(name): fields}
}

func literalBound(value any) any { return astNewtype("Literal", value) }
func exprBound(value any) any    { return astNewtype("Expr", value) }

func jsonFields(value any) (map[string]any, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func withInput(input any, fields map[string]any) (map[string]any, error) {
	if input == nil {
		return nil, errors.New("helix: step requires a source AST node")
	}
	fields["input"] = input
	return fields, nil
}

func MarshalRequest(req Request) ([]byte, error) {
	if req == nil {
		return nil, errors.New("helix: nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	return req.MarshalJSON()
}

type DateTime struct{ millis int64 }

func DateTimeFromMillis(millis int64) DateTime { return DateTime{millis: millis} }

func ParseDateTimeRFC3339(input string) (DateTime, error) {
	t, err := time.Parse(time.RFC3339Nano, input)
	if err != nil {
		return DateTime{}, err
	}
	return dateTimeFromTime(t), nil
}

func (d DateTime) Millis() int64 { return d.millis }

func (d DateTime) RFC3339() (string, error) {
	sec := d.millis / 1000
	ms := d.millis % 1000
	if ms < 0 {
		sec--
		ms += 1000
	}
	return time.Unix(sec, ms*int64(time.Millisecond)).UTC().Format("2006-01-02T15:04:05.000Z"), nil
}

func dateTimeFromTime(t time.Time) DateTime {
	return DateTime{millis: t.UTC().UnixNano() / int64(time.Millisecond)}
}

type PropertyValue struct {
	kind  string
	value any
	err   error
}

func Null() PropertyValue                  { return PropertyValue{kind: "Null"} }
func Bool(v bool) PropertyValue            { return PropertyValue{kind: "Bool", value: v} }
func I64(v int64) PropertyValue            { return PropertyValue{kind: "I64", value: v} }
func DateTimeMillis(v int64) PropertyValue { return PropertyValue{kind: "DateTime", value: v} }
func F64(v float64) PropertyValue          { return propertyFloat("F64", v) }
func F32(v float32) PropertyValue          { return propertyFloat("F32", v) }
func String(v string) PropertyValue        { return PropertyValue{kind: "String", value: v} }
func Bytes(v []byte) PropertyValue {
	return PropertyValue{kind: "Bytes", value: append([]byte(nil), v...)}
}
func I64Array(v ...int64) PropertyValue {
	return PropertyValue{kind: "I64Array", value: append([]int64(nil), v...)}
}
func F64Array(v ...float64) PropertyValue {
	return PropertyValue{kind: "F64Array", value: append([]float64(nil), v...)}
}
func F32Array(v ...float32) PropertyValue {
	return PropertyValue{kind: "F32Array", value: append([]float32(nil), v...)}
}
func StringArray(v ...string) PropertyValue {
	return PropertyValue{kind: "StringArray", value: append([]string(nil), v...)}
}
func Array(v ...PropertyValue) PropertyValue {
	return PropertyValue{kind: "Array", value: append([]PropertyValue(nil), v...)}
}
func Object(v map[string]PropertyValue) PropertyValue {
	out := make(map[string]PropertyValue, len(v))
	for k, val := range v {
		out[k] = val
	}
	return PropertyValue{kind: "Object", value: out}
}

type ObjectEntry struct {
	Key   string
	Value PropertyValue
}

func Entry(key string, value any) ObjectEntry {
	return ObjectEntry{Key: key, Value: MustPropertyValue(value)}
}

func ObjectFromEntries(entries ...ObjectEntry) PropertyValue {
	out := make(map[string]PropertyValue, len(entries))
	for _, entry := range entries {
		out[entry.Key] = entry.Value
	}
	return Object(out)
}

func propertyFloat[T ~float32 | ~float64](kind string, v T) PropertyValue {
	f := float64(v)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return PropertyValue{kind: kind, err: errors.New("helix: non-finite float")}
	}
	return PropertyValue{kind: kind, value: v}
}

func MustPropertyValue(value any) PropertyValue {
	v, err := PropertyValueOf(value)
	if err != nil {
		return PropertyValue{err: err}
	}
	return v
}

func PropertyValueOf(value any) (PropertyValue, error) {
	switch v := value.(type) {
	case nil:
		return Null(), nil
	case PropertyValue:
		return v, v.err
	case DateTime:
		return DateTimeMillis(v.Millis()), nil
	case time.Time:
		return DateTimeMillis(dateTimeFromTime(v).Millis()), nil
	case string:
		return String(v), nil
	case bool:
		return Bool(v), nil
	case int:
		return I64(int64(v)), nil
	case int8:
		return I64(int64(v)), nil
	case int16:
		return I64(int64(v)), nil
	case int32:
		return I64(int64(v)), nil
	case int64:
		return I64(v), nil
	case uint:
		return uintToI64(uint64(v))
	case uint8:
		return I64(int64(v)), nil
	case uint16:
		return I64(int64(v)), nil
	case uint32:
		return I64(int64(v)), nil
	case uint64:
		return uintToI64(v)
	case float32:
		return F32(v), nil
	case float64:
		return F64(v), nil
	case []byte:
		return Bytes(v), nil
	case []int:
		vals := make([]int64, len(v))
		for i, val := range v {
			vals[i] = int64(val)
		}
		return I64Array(vals...), nil
	case []int64:
		return I64Array(v...), nil
	case []float64:
		return F64Array(v...), nil
	case []float32:
		return F32Array(v...), nil
	case []string:
		return StringArray(v...), nil
	case []any:
		vals := make([]PropertyValue, 0, len(v))
		for _, item := range v {
			pv, err := PropertyValueOf(item)
			if err != nil {
				return PropertyValue{}, err
			}
			vals = append(vals, pv)
		}
		return Array(vals...), nil
	case []QueryValue:
		vals := make([]PropertyValue, 0, len(v))
		for _, item := range v {
			pv, err := PropertyValueOf(item)
			if err != nil {
				return PropertyValue{}, err
			}
			vals = append(vals, pv)
		}
		return Array(vals...), nil
	case map[string]PropertyValue:
		return Object(v), nil
	case map[string]any:
		out := make(map[string]PropertyValue, len(v))
		for key, item := range v {
			pv, err := PropertyValueOf(item)
			if err != nil {
				return PropertyValue{}, &PathError{Path: key, Err: err}
			}
			out[key] = pv
		}
		return Object(out), nil
	case map[string]QueryValue:
		out := make(map[string]PropertyValue, len(v))
		for key, item := range v {
			pv, err := PropertyValueOf(item)
			if err != nil {
				return PropertyValue{}, &PathError{Path: key, Err: err}
			}
			out[key] = pv
		}
		return Object(out), nil
	default:
		rv := reflect.ValueOf(value)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			vals := make([]PropertyValue, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				pv, err := PropertyValueOf(rv.Index(i).Interface())
				if err != nil {
					return PropertyValue{}, &PathError{Path: fmt.Sprintf("[%d]", i), Err: err}
				}
				vals = append(vals, pv)
			}
			return Array(vals...), nil
		}
		return PropertyValue{}, fmt.Errorf("helix: unsupported property value %T", value)
	}
}

func uintToI64(v uint64) (PropertyValue, error) {
	if v > math.MaxInt64 {
		return PropertyValue{}, fmt.Errorf("helix: uint value %d overflows i64", v)
	}
	return I64(int64(v)), nil
}

func (p PropertyValue) MarshalJSON() ([]byte, error) {
	if p.err != nil {
		return nil, p.err
	}
	switch p.kind {
	case "Null":
		return json.Marshal(astUnit("Null"))
	case "Bytes":
		bytesValue := p.value.([]byte)
		ints := make([]int, len(bytesValue))
		for i, b := range bytesValue {
			ints[i] = int(b)
		}
		return json.Marshal(astNewtype(p.kind, ints))
	case "F32":
		return json.Marshal(astNewtype(p.kind, float32(p.value.(float32))))
	default:
		return json.Marshal(astNewtype(p.kind, p.value))
	}
}

type PropertyInput struct {
	value *PropertyValue
	expr  *Expr
	err   error
}

func ValueInput(value any) PropertyInput {
	pv, err := PropertyValueOf(value)
	if err != nil {
		return PropertyInput{err: err}
	}
	return PropertyInput{value: &pv}
}

func ExprInput(expr Expr) PropertyInput    { return PropertyInput{expr: &expr} }
func ParamInput(name string) PropertyInput { return ExprInput(ExprParam(name)) }

func propertyInputOf(value any) PropertyInput {
	switch v := value.(type) {
	case PropertyInput:
		return v
	case Expr:
		return ExprInput(v)
	case ParamRef:
		return v.Input()
	default:
		return ValueInput(value)
	}
}

func (p PropertyInput) MarshalJSON() ([]byte, error) {
	if p.err != nil {
		return nil, p.err
	}
	if p.expr != nil {
		return json.Marshal(astNewtype("Expr", *p.expr))
	}
	if p.value == nil {
		return json.Marshal(astNewtype("Value", Null()))
	}
	return json.Marshal(astNewtype("Value", *p.value))
}

type NodeRef struct {
	kind  string
	value any
}

func AllNodes() NodeRef        { return NodeRef{kind: "All"} }
func NodeID(id uint64) NodeRef { return NodeRef{kind: "Ids", value: []uint64{id}} }
func NodeIDs(ids ...uint64) NodeRef {
	return NodeRef{kind: "Ids", value: append([]uint64(nil), ids...)}
}
func NodeVar(name string) NodeRef   { return NodeRef{kind: "Var", value: name} }
func NodeParam(name string) NodeRef { return NodeRef{kind: "Param", value: name} }

func (n NodeRef) MarshalJSON() ([]byte, error) {
	if n.kind == "All" {
		return json.Marshal(astUnit("All"))
	}
	return json.Marshal(astNewtype(n.kind, n.value))
}

type EdgeRef struct {
	kind  string
	value any
}

func AllEdges() EdgeRef        { return EdgeRef{kind: "All"} }
func EdgeID(id uint64) EdgeRef { return EdgeRef{kind: "Ids", value: []uint64{id}} }
func EdgeIDs(ids ...uint64) EdgeRef {
	return EdgeRef{kind: "Ids", value: append([]uint64(nil), ids...)}
}
func EdgeVar(name string) EdgeRef   { return EdgeRef{kind: "Var", value: name} }
func EdgeParam(name string) EdgeRef { return EdgeRef{kind: "Param", value: name} }

func (e EdgeRef) MarshalJSON() ([]byte, error) {
	if e.kind == "All" {
		return json.Marshal(astUnit("All"))
	}
	return json.Marshal(astNewtype(e.kind, e.value))
}

type Expr struct {
	kind  string
	value any
}

func ExprProp(name string) Expr    { return Expr{kind: "Property", value: name} }
func ExprID() Expr                 { return Expr{kind: "Id"} }
func ExprTimestamp() Expr          { return Expr{kind: "Timestamp"} }
func ExprDateTime() Expr           { return Expr{kind: "DateTimeNow"} }
func ExprVal(value any) Expr       { return Expr{kind: "Constant", value: MustPropertyValue(value)} }
func ExprParam(name string) Expr   { return Expr{kind: "Param", value: name} }
func (e Expr) Add(other Expr) Expr { return Expr{kind: "Add", value: []Expr{e, other}} }
func (e Expr) Sub(other Expr) Expr { return Expr{kind: "Sub", value: []Expr{e, other}} }
func (e Expr) Mul(other Expr) Expr { return Expr{kind: "Mul", value: []Expr{e, other}} }
func (e Expr) Div(other Expr) Expr { return Expr{kind: "Div", value: []Expr{e, other}} }
func (e Expr) Mod(other Expr) Expr { return Expr{kind: "Mod", value: []Expr{e, other}} }
func (e Expr) Neg() Expr           { return Expr{kind: "Neg", value: e} }

type WhenThen struct {
	When Predicate
	Then Expr
}

func ExprCase(branches []WhenThen, elseExpr *Expr) Expr {
	whenThen := make([]map[string]any, len(branches))
	for i, branch := range branches {
		whenThen[i] = map[string]any{"when": branch.When, "then": branch.Then}
	}
	value := map[string]any{"when_then": whenThen}
	if elseExpr != nil {
		value["else_expr"] = *elseExpr
	}
	return Expr{kind: "Case", value: value}
}

func (e Expr) MarshalJSON() ([]byte, error) {
	switch e.kind {
	case "Id", "Timestamp", "DateTimeNow":
		return json.Marshal(astUnit(e.kind))
	case "Add", "Sub", "Mul", "Div", "Mod":
		values := e.value.([]Expr)
		return json.Marshal(astStruct(e.kind, map[string]any{"left": values[0], "right": values[1]}))
	case "Neg":
		return json.Marshal(astStruct("Neg", map[string]any{"expr": e.value}))
	case "Case":
		return json.Marshal(astStruct("Case", e.value.(map[string]any)))
	default:
		return json.Marshal(astNewtype(e.kind, e.value))
	}
}

type StreamBound struct {
	literal *int
	expr    *Expr
}

func BoundLiteral(value int) StreamBound { return StreamBound{literal: &value} }
func BoundExpr(expr Expr) StreamBound    { return StreamBound{expr: &expr} }

func streamBoundOf(value any) StreamBound {
	switch v := value.(type) {
	case StreamBound:
		return v
	case Expr:
		return BoundExpr(v)
	case ParamRef:
		return v.Bound()
	case int:
		if v >= 0 {
			return BoundLiteral(v)
		}
		return BoundExpr(ExprVal(int64(v)))
	case int64:
		if v >= 0 && v <= int64(math.MaxInt) {
			return BoundLiteral(int(v))
		}
		return BoundExpr(ExprVal(v))
	case uint64:
		if v <= uint64(math.MaxInt) {
			return BoundLiteral(int(v))
		}
		return BoundExpr(ExprVal(v))
	default:
		return BoundExpr(ExprVal(value))
	}
}

func (s StreamBound) MarshalJSON() ([]byte, error) {
	if s.expr != nil {
		return json.Marshal(exprBound(*s.expr))
	}
	value := 0
	if s.literal != nil {
		value = *s.literal
	}
	return json.Marshal(literalBound(value))
}

type CompareOp string

const (
	CompareEq  CompareOp = "eq"
	CompareNeq CompareOp = "neq"
	CompareGt  CompareOp = "gt"
	CompareGte CompareOp = "gte"
	CompareLt  CompareOp = "lt"
	CompareLte CompareOp = "lte"
)

type Predicate struct {
	kind  string
	value any
}

func PredEq(property string, value any) Predicate { return comparisonPredicate("Eq", property, value) }
func PredNeq(property string, value any) Predicate {
	return comparisonPredicate("Neq", property, value)
}
func PredGt(property string, value any) Predicate { return comparisonPredicate("Gt", property, value) }
func PredGte(property string, value any) Predicate {
	return comparisonPredicate("Gte", property, value)
}
func PredLt(property string, value any) Predicate { return comparisonPredicate("Lt", property, value) }
func PredLte(property string, value any) Predicate {
	return comparisonPredicate("Lte", property, value)
}
func PredHasKey(property string) Predicate    { return Predicate{kind: "HasKey", value: property} }
func PredIsNull(property string) Predicate    { return Predicate{kind: "IsNull", value: property} }
func PredIsNotNull(property string) Predicate { return Predicate{kind: "IsNotNull", value: property} }
func PredStartsWith(property, prefix string) Predicate {
	return Predicate{kind: "StartsWith", value: []any{property, prefix}}
}
func PredEndsWith(property, suffix string) Predicate {
	return Predicate{kind: "EndsWith", value: []any{property, suffix}}
}
func PredContains(property, needle string) Predicate {
	return Predicate{kind: "Contains", value: []any{property, needle}}
}
func PredContainsExpr(property string, expr Expr) Predicate {
	return Predicate{kind: "ContainsExpr", value: []any{property, expr}}
}
func PredIsIn(property string, value any) Predicate {
	if expr, ok := exprFromValue(value); ok {
		return Predicate{kind: "IsInExpr", value: []any{property, expr}}
	}
	return Predicate{kind: "IsIn", value: []any{property, MustPropertyValue(value)}}
}
func PredIsInExpr(property string, expr Expr) Predicate {
	return Predicate{kind: "IsInExpr", value: []any{property, expr}}
}
func PredAnd(preds ...Predicate) Predicate { return Predicate{kind: "And", value: preds} }
func PredOr(preds ...Predicate) Predicate  { return Predicate{kind: "Or", value: preds} }
func PredNot(pred Predicate) Predicate     { return Predicate{kind: "Not", value: pred} }
func PredCompare(left Expr, op CompareOp, right Expr) Predicate {
	return Predicate{kind: "Compare", value: map[string]any{"left": left, "op": op, "right": right}}
}
func PredBetween(property string, min any, max any) Predicate {
	minExpr, minIsExpr := exprFromValue(min)
	maxExpr, maxIsExpr := exprFromValue(max)
	if minIsExpr || maxIsExpr {
		if !minIsExpr {
			minExpr = ExprVal(min)
		}
		if !maxIsExpr {
			maxExpr = ExprVal(max)
		}
		return Predicate{kind: "BetweenExpr", value: []any{property, minExpr, maxExpr}}
	}
	return Predicate{kind: "Between", value: []any{property, MustPropertyValue(min), MustPropertyValue(max)}}
}

func comparisonPredicate(kind, property string, value any) Predicate {
	if expr, ok := exprFromValue(value); ok {
		return Predicate{kind: kind + "Expr", value: []any{property, expr}}
	}
	return Predicate{kind: kind, value: []any{property, MustPropertyValue(value)}}
}

func exprFromValue(value any) (Expr, bool) {
	switch v := value.(type) {
	case Expr:
		return v, true
	case ParamRef:
		return v.Expr(), true
	default:
		return Expr{}, false
	}
}

func (p Predicate) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.ast())
}

func (p Predicate) ast() any {
	propExpr := func(property string) Expr { return ExprProp(property) }
	asExpr := func(value any) Expr {
		if expr, ok := value.(Expr); ok {
			return expr
		}
		return ExprVal(value)
	}
	binary := func(name string, values []any) any {
		return astStruct(name, map[string]any{"left": propExpr(values[0].(string)), "right": asExpr(values[1])})
	}
	switch p.kind {
	case "Eq", "EqExpr":
		return binary("Eq", p.value.([]any))
	case "Neq", "NeqExpr":
		return binary("Neq", p.value.([]any))
	case "Gt", "GtExpr":
		return binary("Gt", p.value.([]any))
	case "Gte", "GteExpr":
		return binary("Gte", p.value.([]any))
	case "Lt", "LtExpr":
		return binary("Lt", p.value.([]any))
	case "Lte", "LteExpr":
		return binary("Lte", p.value.([]any))
	case "Between":
		values := p.value.([]any)
		return astStruct("Between", map[string]any{"value": propExpr(values[0].(string)), "min": asExpr(values[1]), "max": asExpr(values[2])})
	case "BetweenExpr":
		values := p.value.([]any)
		return astStruct("Between", map[string]any{"value": propExpr(values[0].(string)), "min": values[1], "max": values[2]})
	case "HasKey", "IsNull", "IsNotNull":
		return astStruct(p.kind, map[string]any{"property": p.value})
	case "StartsWith":
		values := p.value.([]any)
		return astStruct("StartsWith", map[string]any{"value": propExpr(values[0].(string)), "prefix": ExprVal(values[1])})
	case "EndsWith":
		values := p.value.([]any)
		return astStruct("EndsWith", map[string]any{"value": propExpr(values[0].(string)), "suffix": ExprVal(values[1])})
	case "Contains":
		values := p.value.([]any)
		return astStruct("Contains", map[string]any{"value": propExpr(values[0].(string)), "substring": ExprVal(values[1])})
	case "ContainsExpr":
		values := p.value.([]any)
		return astStruct("Contains", map[string]any{"value": propExpr(values[0].(string)), "substring": values[1]})
	case "IsIn":
		values := p.value.([]any)
		return astStruct("IsIn", map[string]any{"value": propExpr(values[0].(string)), "values": ExprVal(values[1])})
	case "IsInExpr":
		values := p.value.([]any)
		return astStruct("IsIn", map[string]any{"value": propExpr(values[0].(string)), "values": values[1]})
	case "And", "Or":
		return astStruct(p.kind, map[string]any{"predicates": p.value})
	case "Not":
		return astStruct("Not", map[string]any{"predicate": p.value})
	case "Compare":
		return astStruct("Compare", p.value.(map[string]any))
	default:
		return astNewtype(p.kind, p.value)
	}
}

type SourcePredicate = Predicate

func SourceEq(property string, value any) SourcePredicate {
	return PredEq(property, value)
}
func SourceNeq(property string, value any) SourcePredicate {
	return PredNeq(property, value)
}
func SourceGt(property string, value any) SourcePredicate {
	return PredGt(property, value)
}
func SourceGte(property string, value any) SourcePredicate {
	return PredGte(property, value)
}
func SourceLt(property string, value any) SourcePredicate {
	return PredLt(property, value)
}
func SourceLte(property string, value any) SourcePredicate {
	return PredLte(property, value)
}
func SourceHasKey(property string) SourcePredicate {
	return PredHasKey(property)
}
func SourceStartsWith(property, prefix string) SourcePredicate {
	return PredStartsWith(property, prefix)
}
func SourceEndsWith(property, suffix string) SourcePredicate {
	return PredEndsWith(property, suffix)
}
func SourceContains(property, needle string) SourcePredicate {
	return PredContains(property, needle)
}
func SourceContainsExpr(property string, expr Expr) SourcePredicate {
	return PredContainsExpr(property, expr)
}
func SourceIsIn(property string, value any) SourcePredicate { return PredIsIn(property, value) }
func SourceIsInExpr(property string, expr Expr) SourcePredicate {
	return PredIsInExpr(property, expr)
}
func SourceIsNull(property string) SourcePredicate    { return PredIsNull(property) }
func SourceIsNotNull(property string) SourcePredicate { return PredIsNotNull(property) }
func SourceAnd(preds ...SourcePredicate) SourcePredicate {
	return PredAnd(preds...)
}
func SourceOr(preds ...SourcePredicate) SourcePredicate {
	return PredOr(preds...)
}
func SourceNot(pred SourcePredicate) SourcePredicate { return PredNot(pred) }
func SourceCompare(left Expr, op CompareOp, right Expr) SourcePredicate {
	return PredCompare(left, op, right)
}
func SourceBetween(property string, min any, max any) SourcePredicate {
	return PredBetween(property, min, max)
}

type Projection struct {
	Source string `json:"source,omitempty"`
	Alias  string `json:"alias"`
	Expr   *Expr  `json:"expr,omitempty"`
}

func ProjectProp(source string) Projection          { return Projection{Source: source, Alias: source} }
func ProjectPropAs(source, alias string) Projection { return Projection{Source: source, Alias: alias} }
func ProjectFromEndpoint(source, alias string) Projection {
	return ProjectPropAs("$from."+source, alias)
}
func ProjectToEndpoint(source, alias string) Projection {
	return ProjectPropAs("$to."+source, alias)
}
func ProjectExpr(alias string, expr Expr) Projection { return Projection{Alias: alias, Expr: &expr} }

func (p Projection) MarshalJSON() ([]byte, error) {
	if p.Expr != nil {
		return json.Marshal(astNewtype("Expr", struct {
			Alias string `json:"alias"`
			Expr  Expr   `json:"expr"`
		}{p.Alias, *p.Expr}))
	}
	return json.Marshal(astNewtype("Property", struct {
		Source string `json:"source"`
		Alias  string `json:"alias"`
	}{p.Source, p.Alias}))
}

type BindingTarget struct {
	current bool
	name    string
}

func CurrentBinding() BindingTarget { return BindingTarget{current: true} }
func Binding(name string) BindingTarget {
	return BindingTarget{name: name}
}

func (t BindingTarget) MarshalJSON() ([]byte, error) {
	if t.current {
		return json.Marshal(astUnit("Current"))
	}
	return json.Marshal(astNewtype("Binding", t.name))
}

type BindingValueRef struct {
	Target BindingTarget `json:"target"`
	Source string        `json:"source"`
}

func BindingValue(target BindingTarget, source string) BindingValueRef {
	return BindingValueRef{Target: target, Source: source}
}
func CurrentBindingValue(source string) BindingValueRef {
	return BindingValue(CurrentBinding(), source)
}
func NamedBindingValue(name, source string) BindingValueRef {
	return BindingValue(Binding(name), source)
}

type BindingProjection struct {
	Kind   string            `json:"kind"`
	Target *BindingTarget    `json:"target,omitempty"`
	Source string            `json:"source,omitempty"`
	Alias  string            `json:"alias"`
	Refs   []BindingValueRef `json:"refs,omitempty"`
}

func (p BindingProjection) MarshalJSON() ([]byte, error) {
	if p.Kind == "Coalesce" {
		return json.Marshal(astStruct("Coalesce", map[string]any{"refs": p.Refs, "alias": p.Alias}))
	}
	return json.Marshal(astStruct("Property", map[string]any{"target": p.Target, "source": p.Source, "alias": p.Alias}))
}

func ProjectBinding(target BindingTarget, source, alias string) BindingProjection {
	return BindingProjection{Kind: "Property", Target: &target, Source: source, Alias: alias}
}
func ProjectCurrentBinding(source, alias string) BindingProjection {
	return ProjectBinding(CurrentBinding(), source, alias)
}
func ProjectNamedBinding(name, source, alias string) BindingProjection {
	return ProjectBinding(Binding(name), source, alias)
}
func ProjectBindingCoalesce(refs []BindingValueRef, alias string) BindingProjection {
	return BindingProjection{Kind: "Coalesce", Refs: refs, Alias: alias}
}

type projectBindingsStep struct {
	Projections []BindingProjection `json:"projections"`
	Distinct    bool                `json:"distinct"`
}

type Order string

const (
	OrderAsc  Order = "asc"
	OrderDesc Order = "desc"
)

type ShortestPathDirection string

const (
	ShortestPathOut  ShortestPathDirection = "out"
	ShortestPathIn   ShortestPathDirection = "in"
	ShortestPathBoth ShortestPathDirection = "both"
)

type ShortestPathOptions struct {
	Label     string
	Direction ShortestPathDirection
}

type shortestPathStep struct {
	Source    NodeRef               `json:"source"`
	Target    NodeRef               `json:"target"`
	Label     *string               `json:"label,omitempty"`
	Direction ShortestPathDirection `json:"direction"`
	MaxDepth  int                   `json:"max_depth"`
}

type Ordering struct {
	Property string
	Order    Order
}

func (o Ordering) MarshalJSON() ([]byte, error) { return json.Marshal([]any{o.Property, o.Order}) }

type AggregateFunction string

const (
	AggregateCount AggregateFunction = "count"
	AggregateSum   AggregateFunction = "sum"
	AggregateMin   AggregateFunction = "min"
	AggregateMax   AggregateFunction = "max"
	AggregateMean  AggregateFunction = "mean"
)

type EmitBehavior string

const (
	EmitNone   EmitBehavior = "none"
	EmitBefore EmitBehavior = "before"
	EmitAfter  EmitBehavior = "after"
	EmitAll    EmitBehavior = "all"
)

type RangeIndexDirection string

const (
	RangeIndexAsc  RangeIndexDirection = "asc"
	RangeIndexDesc RangeIndexDirection = "desc"
)

type VectorDistanceMetric string

const (
	VectorDistanceCosine    VectorDistanceMetric = "cosine"
	VectorDistanceEuclidean VectorDistanceMetric = "euclidean"
	VectorDistanceManhattan VectorDistanceMetric = "manhattan"
)

type RepeatConfig struct {
	Traversal     SubTraversal `json:"traversal"`
	Times         *int         `json:"times"`
	Until         *Predicate   `json:"until"`
	Emit          EmitBehavior `json:"emit"`
	EmitPredicate *Predicate   `json:"emit_predicate"`
	MaxDepth      int          `json:"max_depth"`
}

func Repeat(traversal SubTraversal) RepeatConfig {
	return RepeatConfig{Traversal: traversal, Emit: EmitNone, MaxDepth: 100}
}
func (r RepeatConfig) WithTimes(times int) RepeatConfig      { r.Times = &times; return r }
func (r RepeatConfig) UntilPred(pred Predicate) RepeatConfig { r.Until = &pred; return r }
func (r RepeatConfig) EmitAll() RepeatConfig                 { r.Emit = EmitAll; return r }
func (r RepeatConfig) EmitAfter() RepeatConfig               { r.Emit = EmitAfter; return r }
func (r RepeatConfig) EmitBefore() RepeatConfig              { r.Emit = EmitBefore; return r }
func (r RepeatConfig) EmitIf(pred Predicate) RepeatConfig {
	r.Emit = EmitAfter
	r.EmitPredicate = &pred
	return r
}
func (r RepeatConfig) WithMaxDepth(max int) RepeatConfig { r.MaxDepth = max; return r }

func (r RepeatConfig) MarshalJSON() ([]byte, error) {
	fields := map[string]any{
		"traversal": r.Traversal,
		"emit":      r.Emit,
		"max_depth": r.MaxDepth,
	}
	if r.Times != nil {
		fields["times"] = *r.Times
	}
	if r.Until != nil {
		fields["until"] = *r.Until
	}
	if r.EmitPredicate != nil {
		fields["emit_predicate"] = *r.EmitPredicate
	}
	return json.Marshal(fields)
}

type IndexSpec struct {
	kind  string
	value any
}

// IndexOperationID is a canonical lowercase non-nil lifecycle UUID.
type IndexOperationID string

var indexOperationIDPattern = regexp.MustCompile(`^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$`)

// validateIndexOperationID enforces the frozen lifecycle control identifier.
func validateIndexOperationID(value string) (IndexOperationID, error) {
	if !indexOperationIDPattern.MatchString(value) || value == "00000000-0000-0000-0000-000000000000" {
		return "", fmt.Errorf("helix: index operation ID must be a canonical lowercase non-nil UUID: %s", value)
	}
	return IndexOperationID(value), nil
}

// IndexDdlReceipt is implemented by every tagged CREATE/DROP receipt variant.
type IndexDdlReceipt interface{ indexDdlReceipt() }

// IndexDdlAccepted reports newly accepted durable lifecycle work.
type IndexDdlAccepted struct {
	Kind        string           `json:"kind"`
	OperationID IndexOperationID `json:"operation_id"`
	IndexID     string           `json:"index_id"`
	Generation  string           `json:"generation"`
}

func (IndexDdlAccepted) indexDdlReceipt() {}

// IndexDdlExistingOperation converges on already-running lifecycle work.
type IndexDdlExistingOperation struct {
	Kind        string           `json:"kind"`
	OperationID IndexOperationID `json:"operation_id"`
}

func (IndexDdlExistingOperation) indexDdlReceipt() {}

// IndexDdlAlreadyActive reports an identical active generation.
type IndexDdlAlreadyActive struct {
	Kind       string `json:"kind"`
	IndexID    string `json:"index_id"`
	Generation string `json:"generation"`
}

func (IndexDdlAlreadyActive) indexDdlReceipt() {}

// IndexErrorCode is a stable machine-readable public lifecycle error.
type IndexErrorCode string

const (
	IndexLifecycleUnavailable        IndexErrorCode = "index_lifecycle_unavailable"
	IndexAlreadyExists               IndexErrorCode = "index_already_exists"
	IndexDefinitionConflict          IndexErrorCode = "index_definition_conflict"
	IndexBusy                        IndexErrorCode = "index_busy"
	IndexNotFound                    IndexErrorCode = "index_not_found"
	IndexOperationNotFound           IndexErrorCode = "index_operation_not_found"
	IndexOperationNotAbortable       IndexErrorCode = "index_operation_not_abortable"
	IndexIDExhausted                 IndexErrorCode = "index_id_exhausted"
	VectorPhysicalIDExhausted        IndexErrorCode = "vector_physical_id_exhausted"
	IndexGenerationExhausted         IndexErrorCode = "index_generation_exhausted"
	IndexRevisionExhausted           IndexErrorCode = "index_revision_exhausted"
	IndexOperationRevisionExhausted  IndexErrorCode = "index_operation_revision_exhausted"
	StaleIndexGeneration             IndexErrorCode = "stale_index_generation"
	WriterFencedCommitOutcomeUnknown IndexErrorCode = "writer_fenced_commit_outcome_unknown"
)

// IndexOperationBlockerCode is a stable reason explicit control is required.
type IndexOperationBlockerCode string

const (
	IndexBlockerInvalidSourceData                   IndexOperationBlockerCode = "invalid_source_data"
	IndexBlockerUniquenessViolation                 IndexOperationBlockerCode = "uniqueness_violation"
	IndexBlockerOversizedEntity                     IndexOperationBlockerCode = "oversized_entity"
	IndexBlockerManifestLimit                       IndexOperationBlockerCode = "manifest_limit"
	IndexBlockerObjectStoreConfigurationUnavailable IndexOperationBlockerCode = "object_store_configuration_unavailable"
	IndexBlockerInvariantViolation                  IndexOperationBlockerCode = "invariant_violation"
)

// IndexOperationProgress contains decimal-string bounded-work counters.
type IndexOperationProgress struct {
	Entities         string `json:"entities"`
	InputBytes       string `json:"input_bytes"`
	OutputOperations string `json:"output_operations"`
	OutputBytes      string `json:"output_bytes"`
}

// IndexOperationStatusCommon contains fields shared by every status variant.
type IndexOperationStatusCommon struct {
	OperationID   IndexOperationID       `json:"operation_id"`
	IndexID       string                 `json:"index_id"`
	Generation    string                 `json:"generation"`
	OperationKind string                 `json:"operation_kind"`
	Family        string                 `json:"family"`
	Stage         string                 `json:"stage"`
	Attempt       uint32                 `json:"attempt"`
	Progress      IndexOperationProgress `json:"progress"`
}

// IndexOperationStatus is implemented by each tagged status variant.
type IndexOperationStatus interface{ indexOperationStatus() }

// IndexOperationQueued is runnable, including bounded retry delay.
type IndexOperationQueued struct {
	Status string `json:"status"`
	IndexOperationStatusCommon
}

func (IndexOperationQueued) indexOperationStatus() {}

// IndexOperationRunning is currently claimed by a fenced writer.
type IndexOperationRunning struct {
	Status string `json:"status"`
	IndexOperationStatusCommon
}

func (IndexOperationRunning) indexOperationStatus() {}

// IndexOperationBlocked requires an explicit retry or abort.
type IndexOperationBlocked struct {
	Status string `json:"status"`
	IndexOperationStatusCommon
	BlockerCode IndexOperationBlockerCode `json:"blocker_code"`
	Message     string                    `json:"message,omitempty"`
}

func (IndexOperationBlocked) indexOperationStatus() {}

// IndexOperationSucceeded completed a build or drop successfully.
type IndexOperationSucceeded struct {
	Status string `json:"status"`
	IndexOperationStatusCommon
}

func (IndexOperationSucceeded) indexOperationStatus() {}

// IndexOperationAborted completed cleanup for an explicitly aborted build.
type IndexOperationAborted struct {
	Status string `json:"status"`
	IndexOperationStatusCommon
}

func (IndexOperationAborted) indexOperationStatus() {}

var indexOperationStages = map[string]struct{}{
	"scan": {}, "scan_partitions": {}, "catch_up": {}, "validate": {},
	"validate_descriptor": {}, "validate_legacy_physical": {}, "compact": {}, "prepare_manifests": {},
	"validate_manifests": {}, "activate": {},
	"delete_entries": {}, "retire_cache": {}, "delete_physical": {},
	"delete_deltas": {}, "delete_metadata": {},
	"finalize":                {},
	"aborting_delete_entries": {}, "aborting_retire_cache": {}, "aborting_delete_physical": {},
	"aborting_delete_deltas": {}, "aborting_delete_metadata": {}, "aborting_finalize": {},
}

var indexOperationBlockers = map[IndexOperationBlockerCode]struct{}{
	IndexBlockerInvalidSourceData: {}, IndexBlockerUniquenessViolation: {},
	IndexBlockerOversizedEntity: {}, IndexBlockerManifestLimit: {},
	IndexBlockerObjectStoreConfigurationUnavailable: {}, IndexBlockerInvariantViolation: {},
}

// validateLifecycleU64 accepts only the canonical decimal-string wire shape.
func validateLifecycleU64(value, field string, allowZero bool) error {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return fmt.Errorf("helix: %s must be a canonical unsigned decimal string", field)
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return fmt.Errorf("helix: %s must be a canonical unsigned decimal string", field)
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || (!allowZero && parsed == 0) {
		return fmt.Errorf("helix: %s is outside the u64 range", field)
	}
	return nil
}

// requireLifecycleFields rejects responses missing frozen contract fields.
func requireLifecycleFields(payload map[string]json.RawMessage, fields ...string) error {
	for _, field := range fields {
		if _, ok := payload[field]; !ok {
			return fmt.Errorf("helix: lifecycle response is missing required field %s", field)
		}
	}
	return nil
}

// UnmarshalIndexDdlReceipt decodes a tagged receipt and ignores additive fields.
func UnmarshalIndexDdlReceipt(data []byte) (IndexDdlReceipt, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("helix: decode index DDL receipt: %w", err)
	}
	if err := requireLifecycleFields(payload, "kind"); err != nil {
		return nil, err
	}
	var tag struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &tag); err != nil {
		return nil, fmt.Errorf("helix: decode index DDL receipt kind: %w", err)
	}
	validateAccepted := func(receipt IndexDdlAccepted) (IndexDdlReceipt, error) {
		if receipt.Kind != "accepted" {
			return nil, fmt.Errorf("helix: invalid accepted receipt tag %q", receipt.Kind)
		}
		if _, err := validateIndexOperationID(string(receipt.OperationID)); err != nil {
			return nil, err
		}
		if err := validateLifecycleU64(receipt.IndexID, "index_id", false); err != nil {
			return nil, err
		}
		if err := validateLifecycleU64(receipt.Generation, "generation", false); err != nil {
			return nil, err
		}
		return receipt, nil
	}
	switch tag.Kind {
	case "accepted":
		if err := requireLifecycleFields(payload, "operation_id", "index_id", "generation"); err != nil {
			return nil, err
		}
		var receipt IndexDdlAccepted
		if err := json.Unmarshal(data, &receipt); err != nil {
			return nil, fmt.Errorf("helix: decode accepted receipt: %w", err)
		}
		return validateAccepted(receipt)
	case "existing_operation":
		if err := requireLifecycleFields(payload, "operation_id"); err != nil {
			return nil, err
		}
		var receipt IndexDdlExistingOperation
		if err := json.Unmarshal(data, &receipt); err != nil {
			return nil, fmt.Errorf("helix: decode existing-operation receipt: %w", err)
		}
		if _, err := validateIndexOperationID(string(receipt.OperationID)); err != nil {
			return nil, err
		}
		return receipt, nil
	case "already_active":
		if err := requireLifecycleFields(payload, "index_id", "generation"); err != nil {
			return nil, err
		}
		var receipt IndexDdlAlreadyActive
		if err := json.Unmarshal(data, &receipt); err != nil {
			return nil, fmt.Errorf("helix: decode already-active receipt: %w", err)
		}
		if err := validateLifecycleU64(receipt.IndexID, "index_id", false); err != nil {
			return nil, err
		}
		if err := validateLifecycleU64(receipt.Generation, "generation", false); err != nil {
			return nil, err
		}
		return receipt, nil
	default:
		return nil, fmt.Errorf("helix: unknown index DDL receipt kind %q", tag.Kind)
	}
}

// validateIndexOperationStatusCommon validates fields shared by every status.
func validateIndexOperationStatusCommon(common *IndexOperationStatusCommon) error {
	if _, err := validateIndexOperationID(string(common.OperationID)); err != nil {
		return err
	}
	if err := validateLifecycleU64(common.IndexID, "index_id", false); err != nil {
		return err
	}
	if err := validateLifecycleU64(common.Generation, "generation", false); err != nil {
		return err
	}
	if common.OperationKind != "build" && common.OperationKind != "drop" {
		return fmt.Errorf("helix: unknown index operation kind %q", common.OperationKind)
	}
	if common.Family != "secondary" && common.Family != "vector" && common.Family != "text" {
		return fmt.Errorf("helix: unknown index family %q", common.Family)
	}
	if _, ok := indexOperationStages[common.Stage]; !ok {
		return fmt.Errorf("helix: unknown index operation stage %q", common.Stage)
	}
	for field, value := range map[string]string{
		"progress.entities":          common.Progress.Entities,
		"progress.input_bytes":       common.Progress.InputBytes,
		"progress.output_operations": common.Progress.OutputOperations,
		"progress.output_bytes":      common.Progress.OutputBytes,
	} {
		if err := validateLifecycleU64(value, field, true); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalIndexOperationStatus decodes a tagged status and ignores additive fields.
func UnmarshalIndexOperationStatus(data []byte) (IndexOperationStatus, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("helix: decode index operation status: %w", err)
	}
	if err := requireLifecycleFields(
		payload,
		"status", "operation_id", "index_id", "generation", "operation_kind", "family", "stage", "attempt", "progress",
	); err != nil {
		return nil, err
	}
	var tag struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(data, &tag); err != nil {
		return nil, fmt.Errorf("helix: decode index operation status tag: %w", err)
	}
	var status IndexOperationStatus
	switch tag.Status {
	case "queued":
		status = &IndexOperationQueued{}
	case "running":
		status = &IndexOperationRunning{}
	case "blocked":
		if err := requireLifecycleFields(payload, "blocker_code"); err != nil {
			return nil, err
		}
		status = &IndexOperationBlocked{}
	case "succeeded":
		status = &IndexOperationSucceeded{}
	case "aborted":
		status = &IndexOperationAborted{}
	default:
		return nil, fmt.Errorf("helix: unknown index operation status %q", tag.Status)
	}
	if err := json.Unmarshal(data, status); err != nil {
		return nil, fmt.Errorf("helix: decode %s index operation status: %w", tag.Status, err)
	}
	var common *IndexOperationStatusCommon
	switch typed := status.(type) {
	case *IndexOperationQueued:
		common = &typed.IndexOperationStatusCommon
	case *IndexOperationRunning:
		common = &typed.IndexOperationStatusCommon
	case *IndexOperationBlocked:
		common = &typed.IndexOperationStatusCommon
		if _, ok := indexOperationBlockers[typed.BlockerCode]; !ok {
			return nil, fmt.Errorf("helix: unknown index operation blocker %q", typed.BlockerCode)
		}
	case *IndexOperationSucceeded:
		common = &typed.IndexOperationStatusCommon
	case *IndexOperationAborted:
		common = &typed.IndexOperationStatusCommon
	}
	if err := validateIndexOperationStatusCommon(common); err != nil {
		return nil, err
	}
	if tag.Status == "aborted" && (common.OperationKind != "build" || !strings.HasPrefix(common.Stage, "aborting_")) {
		return nil, fmt.Errorf("helix: aborted status must describe build cleanup")
	}
	return status, nil
}

func NodeEqualityIndex(label, property string) IndexSpec {
	return IndexSpec{kind: "NodeEquality", value: map[string]any{"label": label, "property": property, "unique": false}}
}
func NodeUniqueEqualityIndex(label, property string) IndexSpec {
	return IndexSpec{kind: "NodeEquality", value: map[string]any{"label": label, "property": property, "unique": true}}
}
func NodeRangeIndex(label, property string) IndexSpec {
	return NodeRangeIndexWithDirection(label, property, RangeIndexAsc)
}
func NodeRangeDescIndex(label, property string) IndexSpec {
	return NodeRangeIndexWithDirection(label, property, RangeIndexDesc)
}
func NodeRangeIndexWithDirection(label, property string, direction RangeIndexDirection) IndexSpec {
	return IndexSpec{kind: "NodeRange", value: rangeIndexFields(label, property, direction)}
}
func EdgeEqualityIndex(label, property string) IndexSpec {
	return IndexSpec{kind: "EdgeEquality", value: map[string]any{"label": label, "property": property}}
}
func EdgeRangeIndex(label, property string) IndexSpec {
	return EdgeRangeIndexWithDirection(label, property, RangeIndexAsc)
}
func EdgeRangeDescIndex(label, property string) IndexSpec {
	return EdgeRangeIndexWithDirection(label, property, RangeIndexDesc)
}
func EdgeRangeIndexWithDirection(label, property string, direction RangeIndexDirection) IndexSpec {
	return IndexSpec{kind: "EdgeRange", value: rangeIndexFields(label, property, direction)}
}
func NodeVectorIndex(label, property string, dimension uint, metric VectorDistanceMetric, tenantProperty ...string) IndexSpec {
	return vectorIndex("NodeVector", label, property, dimension, metric, tenantProperty...)
}
func NodeTextIndex(label, property string, tenantProperty ...string) IndexSpec {
	return tenantIndex("NodeText", label, property, tenantProperty...)
}
func EdgeVectorIndex(label, property string, dimension uint, metric VectorDistanceMetric, tenantProperty ...string) IndexSpec {
	return vectorIndex("EdgeVector", label, property, dimension, metric, tenantProperty...)
}
func EdgeTextIndex(label, property string, tenantProperty ...string) IndexSpec {
	return tenantIndex("EdgeText", label, property, tenantProperty...)
}

func rangeIndexFields(label, property string, direction RangeIndexDirection) map[string]any {
	return map[string]any{"label": label, "property": property, "direction": direction}
}

func tenantIndex(kind, label, property string, tenantProperty ...string) IndexSpec {
	value := map[string]any{"label": label, "property": property}
	if len(tenantProperty) > 0 && tenantProperty[0] != "" {
		value["tenant_property"] = tenantProperty[0]
	}
	return IndexSpec{kind: kind, value: value}
}

func vectorIndex(kind, label, property string, dimension uint, metric VectorDistanceMetric, tenantProperty ...string) IndexSpec {
	if dimension == 0 {
		panic("helix: vector dimension must be non-zero")
	}
	switch metric {
	case VectorDistanceCosine, VectorDistanceEuclidean, VectorDistanceManhattan:
	default:
		panic("helix: unsupported vector distance metric")
	}
	value := tenantIndex(kind, label, property, tenantProperty...)
	fields := value.value.(map[string]any)
	fields["dimension"] = dimension
	fields["metric"] = metric
	return value
}

func (i IndexSpec) MarshalJSON() ([]byte, error) {
	return json.Marshal(astNewtype(i.kind, i.value))
}

type searchNodesVectorStep struct {
	Label       string         `json:"label"`
	Property    string         `json:"property"`
	TenantValue *PropertyInput `json:"tenant_value,omitempty"`
	QueryVector PropertyInput  `json:"query_vector"`
	K           StreamBound    `json:"k"`
}

type searchNodesTextStep struct {
	Label       string         `json:"label"`
	Property    string         `json:"property"`
	TenantValue *PropertyInput `json:"tenant_value,omitempty"`
	QueryText   PropertyInput  `json:"query_text"`
	K           StreamBound    `json:"k"`
}

func CreateVectorIndexNodesStep(
	label, property string,
	dimension uint,
	metric VectorDistanceMetric,
	tenantProperty ...string,
) Step {
	return createIndexStep(NodeVectorIndex(label, property, dimension, metric, tenantProperty...))
}

func CreateVectorIndexEdgesStep(
	label, property string,
	dimension uint,
	metric VectorDistanceMetric,
	tenantProperty ...string,
) Step {
	return createIndexStep(EdgeVectorIndex(label, property, dimension, metric, tenantProperty...))
}

func CreateTextIndexNodesStep(label, property string, tenantProperty ...string) Step {
	return createIndexStep(NodeTextIndex(label, property, tenantProperty...))
}

func CreateTextIndexEdgesStep(label, property string, tenantProperty ...string) Step {
	return createIndexStep(EdgeTextIndex(label, property, tenantProperty...))
}

func createIndexStep(spec IndexSpec) Step {
	return step("CreateIndex", struct {
		Spec        IndexSpec `json:"spec"`
		IfNotExists bool      `json:"if_not_exists"`
	}{spec, true})
}

type Step struct {
	kind  string
	value any
	unit  bool
}

func unitStep(kind string) Step        { return Step{kind: kind, unit: true} }
func step(kind string, value any) Step { return Step{kind: kind, value: value} }

func (s Step) ToAST(input any) (any, error) {
	unary := func(name string, fields map[string]any) (any, error) {
		with, err := withInput(input, fields)
		if err != nil {
			return nil, err
		}
		return astStruct(name, with), nil
	}
	switch s.kind {
	case "N":
		return astStruct("Nodes", map[string]any{"reference": s.value}), nil
	case "NWhere":
		return astStruct("NodesWhere", map[string]any{"predicate": s.value}), nil
	case "ShortestPath":
		fields, err := jsonFields(s.value)
		if err != nil {
			return nil, err
		}
		return astStruct("ShortestPath", fields), nil
	case "E":
		return astStruct("Edges", map[string]any{"reference": s.value}), nil
	case "EWhere":
		return astStruct("EdgesWhere", map[string]any{"predicate": s.value}), nil
	case "VectorSearchNodes", "TextSearchNodes", "VectorSearchEdges", "TextSearchEdges":
		fields, err := jsonFields(s.value)
		if err != nil {
			return nil, err
		}
		return astStruct(s.kind, fields), nil
	case "VectorSearchNodesWithin", "VectorSearchEdgesWithin", "TextSearchNodesWithin", "TextSearchEdgesWithin":
		fields, err := jsonFields(s.value)
		if err != nil {
			return nil, err
		}
		return unary(s.kind, fields)
	case "Out", "In", "Both", "OutE", "InE", "BothE":
		return unary(s.kind, map[string]any{"label": s.value})
	case "OutN", "InN", "OtherN", "Dedup", "Count", "Exists", "Id", "Label", "EdgeProperties", "Fold", "Unfold", "Path", "SimplePath", "SackGet":
		return unary(s.kind, map[string]any{})
	case "Has":
		values := s.value.([]any)
		return unary("Has", map[string]any{"property": values[0], "value": values[1]})
	case "HasLabel":
		return unary("HasLabel", map[string]any{"label": s.value})
	case "HasKey":
		return unary("HasKey", map[string]any{"property": s.value})
	case "Where":
		return unary("Where", map[string]any{"predicate": s.value})
	case "Within":
		return unary("Within", map[string]any{"variable": s.value})
	case "Without":
		return unary("Without", map[string]any{"variable": s.value})
	case "EdgeHas":
		values := s.value.([]any)
		return unary("EdgeHas", map[string]any{"property": values[0], "value": values[1]})
	case "EdgeHasLabel":
		return unary("EdgeHasLabel", map[string]any{"label": s.value})
	case "Limit":
		return unary("Limit", map[string]any{"count": literalBound(s.value)})
	case "LimitBy":
		return unary("Limit", map[string]any{"count": exprBound(s.value)})
	case "Skip":
		return unary("Skip", map[string]any{"count": literalBound(s.value)})
	case "SkipBy":
		return unary("Skip", map[string]any{"count": exprBound(s.value)})
	case "Range":
		values := s.value.([]any)
		return unary("Range", map[string]any{"start": literalBound(values[0]), "end": literalBound(values[1])})
	case "RangeBy":
		values := s.value.([]any)
		return unary("Range", map[string]any{"start": values[0], "end": values[1]})
	case "As", "Store", "Select", "Bind":
		return unary(s.kind, map[string]any{"name": s.value})
	case "Inject":
		fields := map[string]any{"variable": s.value}
		if input != nil {
			fields["input"] = input
		}
		return astStruct("Inject", fields), nil
	case "Values":
		return unary("Values", map[string]any{"properties": s.value})
	case "ValueMap":
		return unary("ValueMap", map[string]any{"properties": s.value})
	case "Project":
		return unary("Project", map[string]any{"projections": s.value})
	case "ProjectBindings":
		fields, err := jsonFields(s.value)
		if err != nil {
			return nil, err
		}
		return unary("ProjectBindings", fields)
	case "CreateIndex", "DropIndex", "GetIndexOperation", "RetryIndexOperation", "AbortIndexOperation":
		fields, err := jsonFields(s.value)
		if err != nil {
			return nil, err
		}
		return astStruct(s.kind, fields), nil
	case "AddN":
		fields, err := jsonFields(s.value)
		if err != nil {
			return nil, err
		}
		if input != nil {
			fields["input"] = input
		}
		return astStruct("AddN", fields), nil
	case "AddE":
		fields, err := jsonFields(s.value)
		if err != nil {
			return nil, err
		}
		return unary("AddE", fields)
	case "SetProperty":
		values := s.value.([]any)
		return unary("SetProperty", map[string]any{"name": values[0], "value": values[1]})
	case "RemoveProperty":
		return unary("RemoveProperty", map[string]any{"name": s.value})
	case "Drop":
		return unary("Drop", map[string]any{})
	case "DropEdge":
		return unary("DropEdge", map[string]any{"to": s.value})
	case "DropEdgeLabeled":
		fields, err := jsonFields(s.value)
		if err != nil {
			return nil, err
		}
		return unary("DropEdgeLabeled", fields)
	case "DropEdgeById":
		fields := map[string]any{"edges": s.value}
		if input != nil {
			fields["input"] = input
		}
		return astStruct("DropEdgeById", fields), nil
	case "OrderBy":
		values := s.value.([]any)
		return unary("OrderBy", map[string]any{"property": values[0], "order": values[1]})
	case "OrderByMultiple":
		return unary("OrderByMultiple", map[string]any{"orderings": s.value})
	case "Repeat":
		return unary("Repeat", map[string]any{"config": s.value})
	case "Union":
		return unary("Union", map[string]any{"traversals": s.value})
	case "Choose":
		fields, err := jsonFields(s.value)
		if err != nil {
			return nil, err
		}
		return unary("Choose", fields)
	case "Coalesce":
		return unary("Coalesce", map[string]any{"traversals": s.value})
	case "Optional":
		return unary("Optional", map[string]any{"traversal": s.value})
	case "Group":
		return unary("Group", map[string]any{"property": s.value})
	case "GroupCount":
		return unary("GroupCount", map[string]any{"property": s.value})
	case "AggregateBy":
		values := s.value.([]any)
		return unary("AggregateBy", map[string]any{"function": values[0], "property": values[1]})
	case "WithSack":
		return unary("WithSack", map[string]any{"initial": s.value})
	case "SackSet", "SackAdd":
		return unary(s.kind, map[string]any{"property": s.value})
	default:
		return nil, fmt.Errorf("helix: unknown step %s", s.kind)
	}
}

func (s Step) MarshalJSON() ([]byte, error) {
	ast, err := s.ToAST("context")
	if err != nil {
		return nil, err
	}
	return json.Marshal(ast)
}

type PropPair struct {
	Name  string
	Value PropertyInput
}

type Props []PropPair

func Prop(name string, value any) PropPair {
	return PropPair{Name: name, Value: propertyInputOf(value)}
}
func PropInput(name string, value PropertyInput) PropPair { return PropPair{Name: name, Value: value} }
func (p PropPair) MarshalJSON() ([]byte, error)           { return json.Marshal([]any{p.Name, p.Value}) }

type Traversal struct {
	steps    []Step
	write    bool
	terminal bool
	err      error
}

func G() *Traversal { return &Traversal{} }

func TraversalFromSteps(steps []Step) *Traversal {
	return &Traversal{steps: append([]Step(nil), steps...)}
}
func (t *Traversal) Steps() []Step { return append([]Step(nil), t.steps...) }
func (t *Traversal) Root() (any, error) {
	if t == nil {
		return nil, errors.New("helix: nil traversal")
	}
	return stepsToAST(t.steps, nil)
}

func stepsToAST(steps []Step, initial any) (any, error) {
	root := initial
	for _, step := range steps {
		next, err := step.ToAST(root)
		if err != nil {
			return nil, err
		}
		root = next
	}
	if root == nil {
		return nil, errors.New("helix: traversal must contain at least one AST node before execution")
	}
	return root, nil
}
func (t *Traversal) Validate() error {
	if t == nil {
		return errors.New("helix: nil traversal")
	}
	return t.err
}
func (t *Traversal) Err() error { return t.Validate() }
func (t *Traversal) MarshalJSON() ([]byte, error) {
	root, err := t.Root()
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Root any `json:"root"`
	}{root})
}

func (t *Traversal) add(s Step) *Traversal {
	if t != nil {
		t.steps = append(t.steps, s)
	}
	return t
}
func (t *Traversal) addWrite(s Step) *Traversal {
	if t != nil {
		t.write = true
		t.steps = append(t.steps, s)
	}
	return t
}
func (t *Traversal) addTerminal(s Step) *Traversal {
	if t != nil {
		t.terminal = true
		t.steps = append(t.steps, s)
	}
	return t
}
func (t *Traversal) record(err error) *Traversal {
	if t != nil && t.err == nil {
		t.err = err
	}
	return t
}

func (t *Traversal) N(ref NodeRef) *Traversal               { return t.add(step("N", ref)) }
func (t *Traversal) NWhere(pred SourcePredicate) *Traversal { return t.add(step("NWhere", pred)) }
func (t *Traversal) NWithLabel(label string) *Traversal     { return t.NWhere(SourceEq("$label", label)) }
func (t *Traversal) NWithLabelWhere(label string, pred SourcePredicate) *Traversal {
	return t.NWhere(SourceAnd(SourceEq("$label", label), pred))
}
func (t *Traversal) ShortestPath(source, target NodeRef, maxDepth int, options ...ShortestPathOptions) *Traversal {
	direction := ShortestPathOut
	var label *string
	if len(options) > 0 {
		if options[0].Direction != "" {
			direction = options[0].Direction
		}
		if options[0].Label != "" {
			label = &options[0].Label
		}
	}
	return t.addTerminal(step("ShortestPath", shortestPathStep{
		Source:    source,
		Target:    target,
		Label:     label,
		Direction: direction,
		MaxDepth:  maxDepth,
	}))
}
func (t *Traversal) E(ref EdgeRef) *Traversal               { return t.add(step("E", ref)) }
func (t *Traversal) EWhere(pred SourcePredicate) *Traversal { return t.add(step("EWhere", pred)) }
func (t *Traversal) EWithLabel(label string) *Traversal     { return t.EWhere(SourceEq("$label", label)) }
func (t *Traversal) EWithLabelWhere(label string, pred SourcePredicate) *Traversal {
	return t.EWhere(SourceAnd(SourceEq("$label", label), pred))
}
func (t *Traversal) VectorSearchNodes(label, property string, queryVector any, k any, tenantValue ...any) *Traversal {
	return t.VectorSearchNodesWith(label, property, vectorSearchInput(queryVector), streamBoundOf(k), tenantInput(tenantValue))
}
func (t *Traversal) VectorSearchNodesWith(label, property string, queryVector PropertyInput, k StreamBound, tenantValue *PropertyInput) *Traversal {
	return t.add(step("VectorSearchNodes", searchNodesVectorStep{Label: label, Property: property, TenantValue: tenantValue, QueryVector: queryVector, K: k}))
}
func (t *Traversal) TextSearchNodes(label, property string, queryText any, k any, tenantValue ...any) *Traversal {
	return t.TextSearchNodesWith(label, property, propertyInputOf(queryText), streamBoundOf(k), tenantInput(tenantValue))
}
func (t *Traversal) TextSearchNodesWith(label, property string, queryText PropertyInput, k StreamBound, tenantValue *PropertyInput) *Traversal {
	return t.add(step("TextSearchNodes", searchNodesTextStep{Label: label, Property: property, TenantValue: tenantValue, QueryText: queryText, K: k}))
}
func (t *Traversal) VectorSearchEdges(label, property string, queryVector any, k any, tenantValue ...any) *Traversal {
	return t.VectorSearchEdgesWith(label, property, vectorSearchInput(queryVector), streamBoundOf(k), tenantInput(tenantValue))
}
func (t *Traversal) VectorSearchEdgesWith(label, property string, queryVector PropertyInput, k StreamBound, tenantValue *PropertyInput) *Traversal {
	return t.add(step("VectorSearchEdges", searchNodesVectorStep{Label: label, Property: property, TenantValue: tenantValue, QueryVector: queryVector, K: k}))
}

// VectorSearchNodesWithin ranks only the current node stream.
func (t *Traversal) VectorSearchNodesWithin(label, property string, queryVector any, k any, tenantValue ...any) *Traversal {
	return t.VectorSearchNodesWithinWith(label, property, vectorSearchInput(queryVector), streamBoundOf(k), tenantInput(tenantValue))
}
func (t *Traversal) VectorSearchNodesWithinWith(label, property string, queryVector PropertyInput, k StreamBound, tenantValue *PropertyInput) *Traversal {
	return t.add(step("VectorSearchNodesWithin", searchNodesVectorStep{Label: label, Property: property, TenantValue: tenantValue, QueryVector: queryVector, K: k}))
}

// VectorSearchEdgesWithin ranks only the current edge stream.
func (t *Traversal) VectorSearchEdgesWithin(label, property string, queryVector any, k any, tenantValue ...any) *Traversal {
	return t.VectorSearchEdgesWithinWith(label, property, vectorSearchInput(queryVector), streamBoundOf(k), tenantInput(tenantValue))
}
func (t *Traversal) VectorSearchEdgesWithinWith(label, property string, queryVector PropertyInput, k StreamBound, tenantValue *PropertyInput) *Traversal {
	return t.add(step("VectorSearchEdgesWithin", searchNodesVectorStep{Label: label, Property: property, TenantValue: tenantValue, QueryVector: queryVector, K: k}))
}

// TextSearchNodesWithin ranks only the current node stream.
func (t *Traversal) TextSearchNodesWithin(label, property string, queryText any, k any, tenantValue ...any) *Traversal {
	return t.TextSearchNodesWithinWith(label, property, propertyInputOf(queryText), streamBoundOf(k), tenantInput(tenantValue))
}
func (t *Traversal) TextSearchNodesWithinWith(label, property string, queryText PropertyInput, k StreamBound, tenantValue *PropertyInput) *Traversal {
	return t.add(step("TextSearchNodesWithin", searchNodesTextStep{Label: label, Property: property, TenantValue: tenantValue, QueryText: queryText, K: k}))
}

// TextSearchEdgesWithin ranks only the current edge stream.
func (t *Traversal) TextSearchEdgesWithin(label, property string, queryText any, k any, tenantValue ...any) *Traversal {
	return t.TextSearchEdgesWithinWith(label, property, propertyInputOf(queryText), streamBoundOf(k), tenantInput(tenantValue))
}
func (t *Traversal) TextSearchEdgesWithinWith(label, property string, queryText PropertyInput, k StreamBound, tenantValue *PropertyInput) *Traversal {
	return t.add(step("TextSearchEdgesWithin", searchNodesTextStep{Label: label, Property: property, TenantValue: tenantValue, QueryText: queryText, K: k}))
}
func (t *Traversal) TextSearchEdges(label, property string, queryText any, k any, tenantValue ...any) *Traversal {
	return t.TextSearchEdgesWith(label, property, propertyInputOf(queryText), streamBoundOf(k), tenantInput(tenantValue))
}
func (t *Traversal) TextSearchEdgesWith(label, property string, queryText PropertyInput, k StreamBound, tenantValue *PropertyInput) *Traversal {
	return t.add(step("TextSearchEdges", searchNodesTextStep{Label: label, Property: property, TenantValue: tenantValue, QueryText: queryText, K: k}))
}
func (t *Traversal) Out(label ...string) *Traversal { return t.add(step("Out", optionalString(label))) }
func (t *Traversal) In(label ...string) *Traversal  { return t.add(step("In", optionalString(label))) }
func (t *Traversal) Both(label ...string) *Traversal {
	return t.add(step("Both", optionalString(label)))
}
func (t *Traversal) OutE(label ...string) *Traversal {
	return t.add(step("OutE", optionalString(label)))
}
func (t *Traversal) InE(label ...string) *Traversal { return t.add(step("InE", optionalString(label))) }
func (t *Traversal) BothE(label ...string) *Traversal {
	return t.add(step("BothE", optionalString(label)))
}
func (t *Traversal) OutN() *Traversal   { return t.add(unitStep("OutN")) }
func (t *Traversal) InN() *Traversal    { return t.add(unitStep("InN")) }
func (t *Traversal) OtherN() *Traversal { return t.add(unitStep("OtherN")) }
func (t *Traversal) Has(property string, value any) *Traversal {
	return t.add(step("Has", []any{property, MustPropertyValue(value)}))
}
func (t *Traversal) HasLabel(label string) *Traversal  { return t.add(step("HasLabel", label)) }
func (t *Traversal) HasKey(property string) *Traversal { return t.add(step("HasKey", property)) }
func (t *Traversal) Where(pred Predicate) *Traversal   { return t.add(step("Where", pred)) }
func (t *Traversal) Dedup() *Traversal                 { return t.add(unitStep("Dedup")) }
func (t *Traversal) Within(name string) *Traversal     { return t.add(step("Within", name)) }
func (t *Traversal) Without(name string) *Traversal    { return t.add(step("Without", name)) }
func (t *Traversal) EdgeHas(property string, value any) *Traversal {
	return t.add(step("EdgeHas", []any{property, propertyInputOf(value)}))
}
func (t *Traversal) EdgeHasLabel(label string) *Traversal { return t.add(step("EdgeHasLabel", label)) }
func (t *Traversal) Limit(bound any) *Traversal {
	b := streamBoundOf(bound)
	if b.expr != nil {
		return t.add(step("LimitBy", *b.expr))
	}
	return t.add(step("Limit", *b.literal))
}
func (t *Traversal) Skip(bound any) *Traversal {
	b := streamBoundOf(bound)
	if b.expr != nil {
		return t.add(step("SkipBy", *b.expr))
	}
	return t.add(step("Skip", *b.literal))
}
func (t *Traversal) Range(start any, end any) *Traversal {
	s, e := streamBoundOf(start), streamBoundOf(end)
	if s.expr == nil && e.expr == nil {
		return t.add(step("Range", []any{*s.literal, *e.literal}))
	}
	return t.add(step("RangeBy", []any{s, e}))
}
func (t *Traversal) As(name string) *Traversal     { return t.add(step("As", name)) }
func (t *Traversal) Store(name string) *Traversal  { return t.add(step("Store", name)) }
func (t *Traversal) Select(name string) *Traversal { return t.add(step("Select", name)) }
func (t *Traversal) Inject(name string) *Traversal { return t.add(step("Inject", name)) }
func (t *Traversal) Bind(name string) *Traversal {
	if name == "" {
		return t.record(errors.New("helix: binding name must not be empty"))
	}
	return t.add(step("Bind", name))
}
func (t *Traversal) Count() *Traversal  { return t.addTerminal(unitStep("Count")) }
func (t *Traversal) Exists() *Traversal { return t.addTerminal(unitStep("Exists")) }
func (t *Traversal) ID() *Traversal     { return t.addTerminal(unitStep("Id")) }
func (t *Traversal) Label() *Traversal  { return t.addTerminal(unitStep("Label")) }
func (t *Traversal) Values(properties ...string) *Traversal {
	return t.addTerminal(step("Values", properties))
}
func (t *Traversal) ValueMap(properties ...string) *Traversal {
	if len(properties) == 0 {
		return t.addTerminal(step("ValueMap", nil))
	}
	return t.addTerminal(step("ValueMap", properties))
}
func (t *Traversal) ValueMapAll() *Traversal { return t.addTerminal(step("ValueMap", nil)) }
func (t *Traversal) Project(projections ...Projection) *Traversal {
	return t.addTerminal(step("Project", projections))
}
func (t *Traversal) ProjectBindings(projections ...BindingProjection) *Traversal {
	return t.addTerminal(step("ProjectBindings", projectBindingsStep{Projections: projections, Distinct: false}))
}
func (t *Traversal) ProjectDistinctBindings(projections ...BindingProjection) *Traversal {
	return t.addTerminal(step("ProjectBindings", projectBindingsStep{Projections: projections, Distinct: true}))
}
func (t *Traversal) EdgeProperties() *Traversal { return t.addTerminal(unitStep("EdgeProperties")) }
func (t *Traversal) AddN(label string, props Props) *Traversal {
	return t.addWrite(step("AddN", struct {
		Label      string `json:"label"`
		Properties Props  `json:"properties"`
	}{label, props}))
}
func (t *Traversal) AddE(label string, to NodeRef, props Props) *Traversal {
	return t.addWrite(step("AddE", struct {
		Label      string  `json:"label"`
		To         NodeRef `json:"to"`
		Properties Props   `json:"properties"`
	}{label, to, props}))
}
func (t *Traversal) SetProperty(name string, value any) *Traversal {
	return t.addWrite(step("SetProperty", []any{name, propertyInputOf(value)}))
}
func (t *Traversal) RemoveProperty(name string) *Traversal {
	return t.addWrite(step("RemoveProperty", name))
}
func (t *Traversal) Drop() *Traversal               { return t.addWrite(unitStep("Drop")) }
func (t *Traversal) DropEdge(to NodeRef) *Traversal { return t.addWrite(step("DropEdge", to)) }
func (t *Traversal) DropEdgeLabeled(to NodeRef, label string) *Traversal {
	return t.addWrite(step("DropEdgeLabeled", struct {
		To    NodeRef `json:"to"`
		Label string  `json:"label"`
	}{to, label}))
}
func (t *Traversal) DropEdgeByID(ref EdgeRef) *Traversal {
	return t.addWrite(step("DropEdgeById", ref))
}
func (t *Traversal) OrderBy(property string, order Order) *Traversal {
	return t.add(step("OrderBy", []any{property, order}))
}
func (t *Traversal) OrderByMultiple(orderings ...Ordering) *Traversal {
	return t.add(step("OrderByMultiple", orderings))
}
func (t *Traversal) Repeat(config RepeatConfig) *Traversal { return t.add(step("Repeat", config)) }
func (t *Traversal) Union(traversals ...SubTraversal) *Traversal {
	return t.add(step("Union", traversals))
}
func (t *Traversal) Choose(condition Predicate, thenTraversal SubTraversal, elseTraversal ...SubTraversal) *Traversal {
	var elseValue *SubTraversal
	if len(elseTraversal) > 0 {
		elseValue = &elseTraversal[0]
	}
	return t.add(step("Choose", struct {
		Condition Predicate     `json:"condition"`
		Then      SubTraversal  `json:"then_traversal"`
		Else      *SubTraversal `json:"else_traversal"`
	}{condition, thenTraversal, elseValue}))
}
func (t *Traversal) Coalesce(traversals ...SubTraversal) *Traversal {
	return t.add(step("Coalesce", traversals))
}
func (t *Traversal) Optional(traversal SubTraversal) *Traversal {
	return t.add(step("Optional", traversal))
}
func (t *Traversal) Group(property string) *Traversal { return t.addTerminal(step("Group", property)) }
func (t *Traversal) GroupCount(property string) *Traversal {
	return t.addTerminal(step("GroupCount", property))
}
func (t *Traversal) AggregateBy(fn AggregateFunction, property string) *Traversal {
	return t.addTerminal(step("AggregateBy", []any{fn, property}))
}
func (t *Traversal) CreateIndexIfNotExists(spec IndexSpec) *Traversal {
	return t.addWrite(step("CreateIndex", struct {
		Spec        IndexSpec `json:"spec"`
		IfNotExists bool      `json:"if_not_exists"`
	}{spec, true}))
}
func (t *Traversal) DropIndex(spec IndexSpec) *Traversal {
	return t.addWrite(step("DropIndex", struct {
		Spec IndexSpec `json:"spec"`
	}{spec}))
}

// GetIndexOperation reads one retained operation in the request storage scope.
func (t *Traversal) GetIndexOperation(operationID string) *Traversal {
	id, err := validateIndexOperationID(operationID)
	if err != nil {
		t.err = err
		return t
	}
	return t.addTerminal(step("GetIndexOperation", struct {
		OperationID IndexOperationID `json:"operation_id"`
	}{id}))
}

// RetryIndexOperation convergently requeues one blocked operation.
func (t *Traversal) RetryIndexOperation(operationID string) *Traversal {
	id, err := validateIndexOperationID(operationID)
	if err != nil {
		t.err = err
		return t
	}
	return t.addWrite(step("RetryIndexOperation", struct {
		OperationID IndexOperationID `json:"operation_id"`
	}{id}))
}

// AbortIndexOperation converts one constructing build into abort cleanup.
func (t *Traversal) AbortIndexOperation(operationID string) *Traversal {
	id, err := validateIndexOperationID(operationID)
	if err != nil {
		t.err = err
		return t
	}
	return t.addWrite(step("AbortIndexOperation", struct {
		OperationID IndexOperationID `json:"operation_id"`
	}{id}))
}
func (t *Traversal) CreateVectorIndexNodes(
	label, property string,
	dimension uint,
	metric VectorDistanceMetric,
	tenantProperty ...string,
) *Traversal {
	return t.CreateIndexIfNotExists(NodeVectorIndex(label, property, dimension, metric, tenantProperty...))
}
func (t *Traversal) CreateVectorIndexEdges(
	label, property string,
	dimension uint,
	metric VectorDistanceMetric,
	tenantProperty ...string,
) *Traversal {
	return t.CreateIndexIfNotExists(EdgeVectorIndex(label, property, dimension, metric, tenantProperty...))
}
func (t *Traversal) CreateTextIndexNodes(label, property string, tenantProperty ...string) *Traversal {
	return t.CreateIndexIfNotExists(NodeTextIndex(label, property, tenantProperty...))
}
func (t *Traversal) CreateTextIndexEdges(label, property string, tenantProperty ...string) *Traversal {
	return t.CreateIndexIfNotExists(EdgeTextIndex(label, property, tenantProperty...))
}
func (t *Traversal) Fold() *Traversal       { return t.add(unitStep("Fold")) }
func (t *Traversal) Unfold() *Traversal     { return t.add(unitStep("Unfold")) }
func (t *Traversal) Path() *Traversal       { return t.add(unitStep("Path")) }
func (t *Traversal) SimplePath() *Traversal { return t.add(unitStep("SimplePath")) }
func (t *Traversal) WithSack(value any) *Traversal {
	return t.add(step("WithSack", MustPropertyValue(value)))
}
func (t *Traversal) SackSet(property string) *Traversal { return t.add(step("SackSet", property)) }
func (t *Traversal) SackAdd(property string) *Traversal { return t.add(step("SackAdd", property)) }
func (t *Traversal) SackGet() *Traversal                { return t.add(unitStep("SackGet")) }

func optionalString(values []string) any {
	if len(values) == 0 || values[0] == "" {
		return nil
	}
	return values[0]
}

func vectorSearchInput(value any) PropertyInput {
	switch v := value.(type) {
	case []float32:
		return ValueInput(F32Array(v...))
	case []float64:
		vals := make([]float32, len(v))
		for i, val := range v {
			vals[i] = float32(val)
		}
		return ValueInput(F32Array(vals...))
	default:
		return propertyInputOf(value)
	}
}

func tenantInput(values []any) *PropertyInput {
	if len(values) == 0 || values[0] == nil {
		return nil
	}
	input := propertyInputOf(values[0])
	return &input
}

type SubTraversal struct{ steps []Step }

func Sub() SubTraversal { return SubTraversal{} }
func SubTraversalFromSteps(steps []Step) SubTraversal {
	return SubTraversal{steps: append([]Step(nil), steps...)}
}
func (s SubTraversal) MarshalJSON() ([]byte, error) {
	root, err := stepsToAST(s.steps, "context")
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Root any `json:"root"`
	}{root})
}
func (s SubTraversal) add(step Step) SubTraversal { s.steps = append(s.steps, step); return s }
func (s SubTraversal) Out(label ...string) SubTraversal {
	return s.add(step("Out", optionalString(label)))
}
func (s SubTraversal) In(label ...string) SubTraversal {
	return s.add(step("In", optionalString(label)))
}
func (s SubTraversal) Both(label ...string) SubTraversal {
	return s.add(step("Both", optionalString(label)))
}
func (s SubTraversal) Where(pred Predicate) SubTraversal { return s.add(step("Where", pred)) }
func (s SubTraversal) Limit(bound any) SubTraversal {
	b := streamBoundOf(bound)
	if b.expr != nil {
		return s.add(step("LimitBy", *b.expr))
	}
	return s.add(step("Limit", *b.literal))
}
func (s SubTraversal) Count() SubTraversal { return s.add(unitStep("Count")) }
func (s SubTraversal) Bind(name string) SubTraversal {
	if name == "" {
		return s
	}
	return s.add(step("Bind", name))
}

type BatchCondition struct {
	kind  string
	value any
}

func VarNotEmpty(name string) BatchCondition { return BatchCondition{kind: "VarNotEmpty", value: name} }
func VarEmpty(name string) BatchCondition    { return BatchCondition{kind: "VarEmpty", value: name} }
func VarMinSize(name string, size int) BatchCondition {
	if size < 0 {
		panic("helix: batch minimum size must be non-negative")
	}
	return BatchCondition{kind: "VarMinSize", value: []any{name, size}}
}
func PrevNotEmpty() BatchCondition { return BatchCondition{kind: "PrevNotEmpty"} }
func (b BatchCondition) MarshalJSON() ([]byte, error) {
	if b.kind == "PrevNotEmpty" {
		return json.Marshal(astUnit("PrevNotEmpty"))
	}
	if b.kind != "VarNotEmpty" && b.kind != "VarEmpty" && b.kind != "VarMinSize" {
		return nil, errors.New("helix: invalid batch condition")
	}
	return json.Marshal(astNewtype(b.kind, b.value))
}
func (b *BatchCondition) UnmarshalJSON(data []byte) error {
	var unit string
	if err := json.Unmarshal(data, &unit); err == nil {
		if unit != "prev_not_empty" {
			return fmt.Errorf("helix: unknown batch condition %q", unit)
		}
		*b = PrevNotEmpty()
		return nil
	}
	var tagged map[string]json.RawMessage
	if err := json.Unmarshal(data, &tagged); err != nil {
		return err
	}
	if len(tagged) != 1 {
		return errors.New("helix: batch condition must contain exactly one variant")
	}
	for name, payload := range tagged {
		switch name {
		case "var_not_empty":
			var variable string
			if err := json.Unmarshal(payload, &variable); err != nil {
				return err
			}
			*b = VarNotEmpty(variable)
		case "var_empty":
			var variable string
			if err := json.Unmarshal(payload, &variable); err != nil {
				return err
			}
			*b = VarEmpty(variable)
		case "var_min_size":
			var values []json.RawMessage
			if err := json.Unmarshal(payload, &values); err != nil {
				return err
			}
			if len(values) != 2 {
				return errors.New("helix: var_min_size requires a variable and minimum size")
			}
			var variable string
			var size int
			if err := json.Unmarshal(values[0], &variable); err != nil {
				return err
			}
			if err := json.Unmarshal(values[1], &size); err != nil {
				return err
			}
			if size < 0 {
				return errors.New("helix: batch minimum size must be non-negative")
			}
			*b = VarMinSize(variable, size)
		default:
			return fmt.Errorf("helix: unknown batch condition %q", name)
		}
		return nil
	}
	return errors.New("helix: missing batch condition variant")
}

type NamedQuery struct {
	Name      string          `json:"name,omitempty"`
	Root      any             `json:"root"`
	Condition *BatchCondition `json:"condition,omitempty"`
}

type BatchEntry struct {
	kind    string
	query   *NamedQuery
	forEach *forEachEntry
}
type forEachEntry struct {
	Param string       `json:"param"`
	Body  []BatchEntry `json:"body"`
}

func queryEntry(q NamedQuery) BatchEntry { return BatchEntry{kind: "Query", query: &q} }
func forEachParamEntry(param string, body []BatchEntry) BatchEntry {
	return BatchEntry{kind: "ForEach", forEach: &forEachEntry{Param: param, Body: body}}
}
func (b BatchEntry) MarshalJSON() ([]byte, error) {
	if b.kind == "ForEach" {
		return json.Marshal(astNewtype("ForEach", b.forEach))
	}
	return json.Marshal(astNewtype("Query", b.query))
}
func (b *BatchEntry) UnmarshalJSON(data []byte) error {
	var tagged map[string]json.RawMessage
	if err := json.Unmarshal(data, &tagged); err != nil {
		return err
	}
	if len(tagged) != 1 {
		return errors.New("helix: batch entry must contain exactly one variant")
	}
	if payload, ok := tagged["query"]; ok {
		var query struct {
			Name      string          `json:"name,omitempty"`
			Root      json.RawMessage `json:"root"`
			Condition *BatchCondition `json:"condition,omitempty"`
		}
		if err := json.Unmarshal(payload, &query); err != nil {
			return err
		}
		var root any
		decoder := json.NewDecoder(bytes.NewReader(query.Root))
		decoder.UseNumber()
		if err := decoder.Decode(&root); err != nil {
			return err
		}
		*b = queryEntry(NamedQuery{Name: query.Name, Root: root, Condition: query.Condition})
		return nil
	}
	if payload, ok := tagged["for_each"]; ok {
		var entry forEachEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			return err
		}
		*b = forEachParamEntry(entry.Param, entry.Body)
		return nil
	}
	return errors.New("helix: unknown batch entry variant")
}

type batchBase struct {
	queries []BatchEntry
	returns []string
	err     error
}

type batchPayload struct {
	Entries []BatchEntry `json:"entries"`
	Returns []string     `json:"returns"`
}

func canonicalBatchPayload(batch batchBase) batchPayload {
	return batchPayload{
		Entries: append([]BatchEntry{}, batch.queries...),
		Returns: append([]string{}, batch.returns...),
	}
}

func returningVars(vars []string) []string {
	if len(vars) == 0 {
		return []string{}
	}
	return append([]string(nil), vars...)
}

func (b *batchBase) Validate() error {
	if b == nil {
		return errors.New("helix: nil batch")
	}
	return b.err
}
func (b *batchBase) Err() error { return b.Validate() }

type ReadBatch struct{ batchBase }
type WriteBatch struct{ batchBase }

type QueryRequestType string

const (
	RequestTypeRead  QueryRequestType = "read"
	RequestTypeWrite QueryRequestType = "write"
)

// BatchQuery is the closed read-or-write batch union used by QueryRequest.
type BatchQuery struct {
	requestType QueryRequestType
	batch       batchBase
	err         error
}

func ReadBatchQuery(batch *ReadBatch) BatchQuery {
	if batch == nil {
		return BatchQuery{err: errors.New("helix: nil read batch")}
	}
	return BatchQuery{requestType: RequestTypeRead, batch: batch.batchBase}
}

func WriteBatchQuery(batch *WriteBatch) BatchQuery {
	if batch == nil {
		return BatchQuery{err: errors.New("helix: nil write batch")}
	}
	return BatchQuery{requestType: RequestTypeWrite, batch: batch.batchBase}
}

func (q BatchQuery) Validate() error {
	if q.err != nil {
		return q.err
	}
	if q.requestType != RequestTypeRead && q.requestType != RequestTypeWrite {
		return errors.New("helix: invalid batch query type")
	}
	return q.batch.err
}

func (q BatchQuery) MarshalJSON() ([]byte, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	batch := canonicalBatchPayload(q.batch)
	if q.requestType == RequestTypeRead {
		return json.Marshal(struct {
			Read any `json:"read"`
		}{batch})
	}
	return json.Marshal(struct {
		Write any `json:"write"`
	}{batch})
}

func Read() *ReadBatch   { return &ReadBatch{} }
func Write() *WriteBatch { return &WriteBatch{} }
func (b *ReadBatch) VarAs(name string, traversal *Traversal) *ReadBatch {
	if traversal == nil {
		b.err = errors.New("helix: nil traversal")
		return b
	}
	if err := traversal.Validate(); err != nil && b.err == nil {
		b.err = err
	}
	if traversal.write && b.err == nil {
		b.err = ErrWriteTraversalInReadBatch
	}
	root, err := traversal.Root()
	if err != nil && b.err == nil {
		b.err = err
	}
	b.queries = append(b.queries, queryEntry(NamedQuery{Name: name, Root: root}))
	return b
}
func (b *ReadBatch) VarAsIf(name string, condition BatchCondition, traversal *Traversal) *ReadBatch {
	before := len(b.queries)
	b.VarAs(name, traversal)
	if len(b.queries) > before {
		b.queries[len(b.queries)-1].query.Condition = &condition
	}
	return b
}
func (b *ReadBatch) ForEachParam(param string, body *ReadBatch) *ReadBatch {
	if body != nil {
		b.queries = append(b.queries, forEachParamEntry(param, body.queries))
	}
	return b
}
func (b *ReadBatch) Returning(vars ...string) *ReadBatch {
	b.returns = returningVars(vars)
	return b
}
func (b *ReadBatch) MarshalJSON() ([]byte, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(canonicalBatchPayload(b.batchBase))
}
func (b *ReadBatch) UnmarshalJSON(data []byte) error {
	var payload struct {
		Entries []BatchEntry `json:"entries"`
		Returns []string     `json:"returns"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	b.batchBase = batchBase{queries: payload.Entries, returns: returningVars(payload.Returns)}
	return nil
}

func (b *WriteBatch) VarAs(name string, traversal *Traversal) *WriteBatch {
	if traversal == nil {
		b.err = errors.New("helix: nil traversal")
		return b
	}
	if err := traversal.Validate(); err != nil && b.err == nil {
		b.err = err
	}
	root, err := traversal.Root()
	if err != nil && b.err == nil {
		b.err = err
	}
	b.queries = append(b.queries, queryEntry(NamedQuery{Name: name, Root: root}))
	return b
}
func (b *WriteBatch) VarAsIf(name string, condition BatchCondition, traversal *Traversal) *WriteBatch {
	before := len(b.queries)
	b.VarAs(name, traversal)
	if len(b.queries) > before {
		b.queries[len(b.queries)-1].query.Condition = &condition
	}
	return b
}
func (b *WriteBatch) ForEachParam(param string, body *WriteBatch) *WriteBatch {
	if body != nil {
		b.queries = append(b.queries, forEachParamEntry(param, body.queries))
	}
	return b
}
func (b *WriteBatch) Returning(vars ...string) *WriteBatch {
	b.returns = returningVars(vars)
	return b
}
func (b *WriteBatch) MarshalJSON() ([]byte, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(canonicalBatchPayload(b.batchBase))
}
func (b *WriteBatch) UnmarshalJSON(data []byte) error {
	var payload struct {
		Entries []BatchEntry `json:"entries"`
		Returns []string     `json:"returns"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	b.batchBase = batchBase{queries: payload.Entries, returns: returningVars(payload.Returns)}
	return nil
}

type ParamKind uint8

const (
	paramKindBool ParamKind = iota
	paramKindI64
	paramKindF64
	paramKindF32
	paramKindString
	paramKindDateTime
	paramKindBytes
	paramKindValue
	paramKindObject
	paramKindArray
)

type QueryParamType struct {
	kind  ParamKind
	inner *QueryParamType
}

func ParamTypeBool() QueryParamType     { return QueryParamType{} }
func ParamTypeI64() QueryParamType      { return QueryParamType{kind: paramKindI64} }
func ParamTypeF64() QueryParamType      { return QueryParamType{kind: paramKindF64} }
func ParamTypeF32() QueryParamType      { return QueryParamType{kind: paramKindF32} }
func ParamTypeString() QueryParamType   { return QueryParamType{kind: paramKindString} }
func ParamTypeDateTime() QueryParamType { return QueryParamType{kind: paramKindDateTime} }
func ParamTypeBytes() QueryParamType    { return QueryParamType{kind: paramKindBytes} }
func ParamTypeValue() QueryParamType    { return QueryParamType{kind: paramKindValue} }
func ParamTypeObject() QueryParamType   { return QueryParamType{kind: paramKindObject} }
func ParamTypeArray(inner QueryParamType) QueryParamType {
	return QueryParamType{kind: paramKindArray, inner: &inner}
}
func (q QueryParamType) Validate() error {
	switch q.kind {
	case paramKindBool, paramKindI64, paramKindF64, paramKindF32, paramKindString,
		paramKindDateTime, paramKindBytes, paramKindValue, paramKindObject:
		if q.inner != nil {
			return errors.New("helix: scalar parameter type cannot have an inner type")
		}
		return nil
	case paramKindArray:
		if q.inner == nil {
			return errors.New("helix: array parameter type requires an inner type")
		}
		return q.inner.Validate()
	default:
		return errors.New("helix: unknown query parameter type")
	}
}
func (q QueryParamType) MarshalJSON() ([]byte, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if q.kind == paramKindArray {
		return json.Marshal(astNewtype("Array", q.inner))
	}
	return json.Marshal(astUnit(paramKindName(q.kind)))
}
func (q *QueryParamType) UnmarshalJSON(data []byte) error {
	var unit string
	if err := json.Unmarshal(data, &unit); err == nil {
		kind, ok := parseScalarParamKind(unit)
		if !ok {
			return fmt.Errorf("helix: unknown query parameter type %q", unit)
		}
		*q = QueryParamType{kind: kind}
		return nil
	}
	var tagged map[string]json.RawMessage
	if err := json.Unmarshal(data, &tagged); err != nil {
		return err
	}
	rawInner, ok := tagged["array"]
	if !ok || len(tagged) != 1 || string(rawInner) == "null" {
		return errors.New("helix: array parameter type requires exactly one inner type")
	}
	var inner QueryParamType
	if err := json.Unmarshal(rawInner, &inner); err != nil {
		return err
	}
	*q = ParamTypeArray(inner)
	return nil
}

func paramKindName(kind ParamKind) string {
	switch kind {
	case paramKindBool:
		return "Bool"
	case paramKindI64:
		return "I64"
	case paramKindF64:
		return "F64"
	case paramKindF32:
		return "F32"
	case paramKindString:
		return "String"
	case paramKindDateTime:
		return "DateTime"
	case paramKindBytes:
		return "Bytes"
	case paramKindValue:
		return "Value"
	case paramKindObject:
		return "Object"
	case paramKindArray:
		return "Array"
	default:
		panic("validated parameter kind must be known")
	}
}

func parseScalarParamKind(value string) (ParamKind, bool) {
	for _, kind := range []ParamKind{
		paramKindBool,
		paramKindI64,
		paramKindF64,
		paramKindF32,
		paramKindString,
		paramKindDateTime,
		paramKindBytes,
		paramKindValue,
		paramKindObject,
	} {
		if value == snakeName(paramKindName(kind)) {
			return kind, true
		}
	}
	return paramKindBool, false
}

type ParamRef struct {
	Name string
	Type QueryParamType
}

func (p ParamRef) Expr() Expr                   { return ExprParam(p.Name) }
func (p ParamRef) Input() PropertyInput         { return ParamInput(p.Name) }
func (p ParamRef) Bound() StreamBound           { return BoundExpr(p.Expr()) }
func (p ParamRef) MarshalJSON() ([]byte, error) { return p.Expr().MarshalJSON() }

type QueryValue any

func QueryNull() QueryValue                               { return nil }
func QueryBool(value bool) QueryValue                     { return value }
func QueryI64(value int64) QueryValue                     { return value }
func QueryF64(value float64) QueryValue                   { return value }
func QueryF32(value float32) QueryValue                   { return value }
func QueryString(value string) QueryValue                 { return value }
func QueryArray(values ...QueryValue) QueryValue          { return values }
func QueryObject(values map[string]QueryValue) QueryValue { return values }

type QueryRequest struct {
	requestType   QueryRequestType
	queryName     *string
	batch         batchBase
	parameters    map[string]QueryValue
	types         map[string]QueryParamType
	parameterMode queryParameterMode
	err           error
}

type queryParameterMode uint8

const (
	queryParametersUnset queryParameterMode = iota
	queryParametersUntyped
	queryParametersTyped
)

type ReadQueryBuilder struct{ QueryRequest }
type WriteQueryBuilder struct{ QueryRequest }

func ReadQuery(name string) *ReadQueryBuilder {
	return &ReadQueryBuilder{QueryRequest: newQueryRequest(RequestTypeRead, name)}
}
func WriteQuery(name string) *WriteQueryBuilder {
	return &WriteQueryBuilder{QueryRequest: newQueryRequest(RequestTypeWrite, name)}
}
func NewQueryRequest(query BatchQuery) *QueryRequest {
	request := newQueryRequest(query.requestType, "")
	request.batch = query.batch
	request.err = query.err
	return &request
}
func NewReadQueryRequest(query *ReadBatch) *QueryRequest {
	return NewQueryRequest(ReadBatchQuery(query))
}
func NewWriteQueryRequest(query *WriteBatch) *QueryRequest {
	return NewQueryRequest(WriteBatchQuery(query))
}
func newQueryRequest(requestType QueryRequestType, name string) QueryRequest {
	var queryName *string
	if name != "" {
		queryName = &name
	}
	return QueryRequest{
		requestType: requestType,
		queryName:   queryName,
		parameters:  map[string]QueryValue{},
		types:       map[string]QueryParamType{},
	}
}
func (q *QueryRequest) Validate() error {
	if q.err != nil {
		return q.err
	}
	if q.requestType != RequestTypeRead && q.requestType != RequestTypeWrite {
		return errors.New("helix: invalid query request type")
	}
	return q.batch.err
}
func (q *QueryRequest) isHelixRequest() {}
func (q *QueryRequest) addParam(name string, ty QueryParamType, value QueryValue, err error) ParamRef {
	if err != nil && q.err == nil {
		q.err = &PathError{Path: name, Err: err}
		return ParamRef{Name: name, Type: ty}
	}
	if err := q.insertTypedParameter(name, ty, value); err != nil && q.err == nil {
		q.err = &PathError{Path: name, Err: err}
	}
	return ParamRef{Name: name, Type: ty}
}
func (q *QueryRequest) RequestType() QueryRequestType { return q.requestType }
func (q *QueryRequest) InsertUntypedParameter(name string, value QueryValue) error {
	if name == "" {
		return ErrEmptyParameterName
	}
	if q.parameterMode == queryParametersTyped {
		return ErrMixedParameterModes
	}
	if _, exists := q.parameters[name]; exists {
		return ErrDuplicateParameter
	}
	normalized, err := queryValueFromValue(value, name)
	if err != nil {
		return err
	}
	q.parameters[name] = normalized
	q.parameterMode = queryParametersUntyped
	return nil
}
func (q *QueryRequest) insertTypedParameter(name string, ty QueryParamType, value QueryValue) error {
	if name == "" {
		return ErrEmptyParameterName
	}
	if q.parameterMode == queryParametersUntyped {
		return ErrMixedParameterModes
	}
	if _, exists := q.parameters[name]; exists {
		return ErrDuplicateParameter
	}
	if err := ty.Validate(); err != nil {
		return err
	}
	normalized, err := normalizeTypedQueryValue(ty, value, name)
	if err != nil {
		return err
	}
	q.parameters[name] = normalized
	q.types[name] = ty
	q.parameterMode = queryParametersTyped
	return nil
}
func (q *QueryRequest) InsertTypedParameter(name string, ty QueryParamType, value QueryValue) error {
	return q.insertTypedParameter(name, ty, value)
}
func (q *QueryRequest) WithUntypedParameter(name string, value QueryValue) *QueryRequest {
	if err := q.InsertUntypedParameter(name, value); err != nil && q.err == nil {
		q.err = &PathError{Path: name, Err: err}
	}
	return q
}
func (q *QueryRequest) WithTypedParameter(name string, ty QueryParamType, value QueryValue) *QueryRequest {
	if err := q.InsertTypedParameter(name, ty, value); err != nil && q.err == nil {
		q.err = &PathError{Path: name, Err: err}
	}
	return q
}
func (q *QueryRequest) SetQueryName(name string) { q.queryName = &name }
func (q *QueryRequest) ClearQueryName()          { q.queryName = nil }
func (q *QueryRequest) WithQueryName(name string) *QueryRequest {
	q.SetQueryName(name)
	return q
}
func (q *QueryRequest) ParamBool(name string, value bool) ParamRef {
	return q.addParam(name, ParamTypeBool(), QueryBool(value), nil)
}
func (q *QueryRequest) ParamI64(name string, value any) ParamRef {
	v, err := queryI64(value)
	return q.addParam(name, ParamTypeI64(), QueryI64(v), err)
}
func (q *QueryRequest) ParamF64(name string, value any) ParamRef {
	v, err := queryFloat64(value)
	return q.addParam(name, ParamTypeF64(), QueryF64(v), err)
}
func (q *QueryRequest) ParamF32(name string, value any) ParamRef {
	v, err := queryFloat64(value)
	return q.addParam(name, ParamTypeF32(), QueryF32(float32(v)), err)
}
func (q *QueryRequest) ParamString(name string, value string) ParamRef {
	return q.addParam(name, ParamTypeString(), QueryString(value), nil)
}
func (q *QueryRequest) ParamDateTime(name string, value any) ParamRef {
	v, err := queryDateTime(value)
	return q.addParam(name, ParamTypeDateTime(), QueryString(v), err)
}
func (q *QueryRequest) ParamValue(name string, value any) ParamRef {
	v, err := queryValueFromValue(value, name)
	return q.addParam(name, ParamTypeValue(), v, err)
}
func (q *QueryRequest) ParamObject(name string, value any, inner ...QueryParamType) ParamRef {
	v, err := queryValueFromValue(value, name)
	return q.addParam(name, ParamTypeObject(), v, err)
}
func (q *QueryRequest) ParamArray(name string, value any, inner QueryParamType) ParamRef {
	v, err := queryValueFromValue(value, name)
	return q.addParam(name, ParamTypeArray(inner), v, err)
}
func (q *QueryRequest) MarshalJSON() ([]byte, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	batch := canonicalBatchPayload(q.batch)
	query := struct {
		Read  any `json:"read,omitempty"`
		Write any `json:"write,omitempty"`
	}{}
	if q.requestType == RequestTypeWrite {
		query.Write = batch
	} else {
		query.Read = batch
	}
	payload := struct {
		RequestType    QueryRequestType          `json:"request_type"`
		QueryName      *string                   `json:"query_name"`
		Query          any                       `json:"query"`
		Parameters     map[string]QueryValue     `json:"parameters,omitempty"`
		ParameterTypes map[string]QueryParamType `json:"parameter_types,omitempty"`
	}{RequestType: q.requestType, QueryName: q.queryName, Query: query}
	if len(q.parameters) > 0 {
		payload.Parameters = q.parameters
	}
	if len(q.types) > 0 {
		payload.ParameterTypes = q.types
	}
	return json.Marshal(payload)
}

func (q *ReadQueryBuilder) VarAs(name string, traversal *Traversal) *ReadQueryBuilder {
	if traversal == nil {
		q.err = errors.New("helix: nil traversal")
		return q
	}
	if err := traversal.Validate(); err != nil && q.err == nil {
		q.err = err
	}
	if traversal.write && q.err == nil {
		q.err = ErrWriteTraversalInReadBatch
	}
	root, err := traversal.Root()
	if err != nil && q.err == nil {
		q.err = err
	}
	q.batch.queries = append(q.batch.queries, queryEntry(NamedQuery{Name: name, Root: root}))
	return q
}
func (q *ReadQueryBuilder) VarAsIf(name string, condition BatchCondition, traversal *Traversal) *ReadQueryBuilder {
	before := len(q.batch.queries)
	q.VarAs(name, traversal)
	if len(q.batch.queries) > before {
		q.batch.queries[len(q.batch.queries)-1].query.Condition = &condition
	}
	return q
}
func (q *ReadQueryBuilder) ForEachParam(param string, body *ReadBatch) *ReadQueryBuilder {
	if body != nil {
		q.batch.queries = append(q.batch.queries, forEachParamEntry(param, body.queries))
	}
	return q
}
func (q *ReadQueryBuilder) Returning(vars ...string) Request {
	q.batch.returns = returningVars(vars)
	return &q.QueryRequest
}
func (q *ReadQueryBuilder) ParamBool(name string, value bool) ParamRef {
	return q.QueryRequest.ParamBool(name, value)
}
func (q *ReadQueryBuilder) ParamI64(name string, value any) ParamRef {
	return q.QueryRequest.ParamI64(name, value)
}
func (q *ReadQueryBuilder) ParamF64(name string, value any) ParamRef {
	return q.QueryRequest.ParamF64(name, value)
}
func (q *ReadQueryBuilder) ParamF32(name string, value any) ParamRef {
	return q.QueryRequest.ParamF32(name, value)
}
func (q *ReadQueryBuilder) ParamString(name string, value string) ParamRef {
	return q.QueryRequest.ParamString(name, value)
}
func (q *ReadQueryBuilder) ParamDateTime(name string, value any) ParamRef {
	return q.QueryRequest.ParamDateTime(name, value)
}
func (q *ReadQueryBuilder) ParamValue(name string, value any) ParamRef {
	return q.QueryRequest.ParamValue(name, value)
}
func (q *ReadQueryBuilder) ParamObject(name string, value any, inner ...QueryParamType) ParamRef {
	return q.QueryRequest.ParamObject(name, value, inner...)
}
func (q *ReadQueryBuilder) ParamArray(name string, value any, inner QueryParamType) ParamRef {
	return q.QueryRequest.ParamArray(name, value, inner)
}
func (q *ReadQueryBuilder) WithUntypedParameter(name string, value QueryValue) *ReadQueryBuilder {
	q.QueryRequest.WithUntypedParameter(name, value)
	return q
}
func (q *ReadQueryBuilder) WithTypedParameter(name string, ty QueryParamType, value QueryValue) *ReadQueryBuilder {
	q.QueryRequest.WithTypedParameter(name, ty, value)
	return q
}

func (q *WriteQueryBuilder) VarAs(name string, traversal *Traversal) *WriteQueryBuilder {
	if traversal == nil {
		q.err = errors.New("helix: nil traversal")
		return q
	}
	if err := traversal.Validate(); err != nil && q.err == nil {
		q.err = err
	}
	root, err := traversal.Root()
	if err != nil && q.err == nil {
		q.err = err
	}
	q.batch.queries = append(q.batch.queries, queryEntry(NamedQuery{Name: name, Root: root}))
	return q
}
func (q *WriteQueryBuilder) VarAsIf(name string, condition BatchCondition, traversal *Traversal) *WriteQueryBuilder {
	before := len(q.batch.queries)
	q.VarAs(name, traversal)
	if len(q.batch.queries) > before {
		q.batch.queries[len(q.batch.queries)-1].query.Condition = &condition
	}
	return q
}
func (q *WriteQueryBuilder) ForEachParam(param string, body *WriteBatch) *WriteQueryBuilder {
	if body != nil {
		q.batch.queries = append(q.batch.queries, forEachParamEntry(param, body.queries))
	}
	return q
}
func (q *WriteQueryBuilder) Returning(vars ...string) Request {
	q.batch.returns = returningVars(vars)
	return &q.QueryRequest
}
func (q *WriteQueryBuilder) ParamBool(name string, value bool) ParamRef {
	return q.QueryRequest.ParamBool(name, value)
}
func (q *WriteQueryBuilder) ParamI64(name string, value any) ParamRef {
	return q.QueryRequest.ParamI64(name, value)
}
func (q *WriteQueryBuilder) ParamF64(name string, value any) ParamRef {
	return q.QueryRequest.ParamF64(name, value)
}
func (q *WriteQueryBuilder) ParamF32(name string, value any) ParamRef {
	return q.QueryRequest.ParamF32(name, value)
}
func (q *WriteQueryBuilder) ParamString(name string, value string) ParamRef {
	return q.QueryRequest.ParamString(name, value)
}
func (q *WriteQueryBuilder) ParamDateTime(name string, value any) ParamRef {
	return q.QueryRequest.ParamDateTime(name, value)
}
func (q *WriteQueryBuilder) ParamValue(name string, value any) ParamRef {
	return q.QueryRequest.ParamValue(name, value)
}
func (q *WriteQueryBuilder) ParamObject(name string, value any, inner ...QueryParamType) ParamRef {
	return q.QueryRequest.ParamObject(name, value, inner...)
}
func (q *WriteQueryBuilder) ParamArray(name string, value any, inner QueryParamType) ParamRef {
	return q.QueryRequest.ParamArray(name, value, inner)
}
func (q *WriteQueryBuilder) WithUntypedParameter(name string, value QueryValue) *WriteQueryBuilder {
	q.QueryRequest.WithUntypedParameter(name, value)
	return q
}
func (q *WriteQueryBuilder) WithTypedParameter(name string, ty QueryParamType, value QueryValue) *WriteQueryBuilder {
	q.QueryRequest.WithTypedParameter(name, ty, value)
	return q
}

func queryI64(value any) (int64, error) {
	pv, err := PropertyValueOf(value)
	if err != nil {
		return 0, err
	}
	if pv.kind != "I64" {
		return 0, ErrInvalidParameterType
	}
	return pv.value.(int64), nil
}
func queryFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, ErrInvalidParameterType
		}
		return v, nil
	case float32:
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, ErrInvalidParameterType
		}
		return f, nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	default:
		return 0, ErrInvalidParameterType
	}
}
func queryDateTime(value any) (string, error) {
	switch v := value.(type) {
	case DateTime:
		return v.RFC3339()
	case time.Time:
		return dateTimeFromTime(v).RFC3339()
	case string:
		dt, err := ParseDateTimeRFC3339(v)
		if err != nil {
			return "", ErrInvalidDateTimeParameter
		}
		return dt.RFC3339()
	case int64:
		return DateTimeFromMillis(v).RFC3339()
	case int:
		return DateTimeFromMillis(int64(v)).RFC3339()
	default:
		return "", ErrInvalidDateTimeParameter
	}
}

func normalizeTypedQueryValue(parameterType QueryParamType, value QueryValue, path string) (QueryValue, error) {
	switch parameterType.kind {
	case paramKindBool:
		typed, ok := value.(bool)
		if !ok {
			return nil, ErrInvalidParameterType
		}
		return typed, nil
	case paramKindI64:
		typed, err := queryI64(value)
		if err != nil {
			return nil, err
		}
		return QueryI64(typed), nil
	case paramKindF64:
		typed, err := queryFloat64(value)
		if err != nil {
			return nil, err
		}
		return QueryF64(typed), nil
	case paramKindF32:
		typed, err := queryFloat64(value)
		if err != nil {
			return nil, err
		}
		normalized := float32(typed)
		if math.IsInf(float64(normalized), 0) {
			return nil, ErrInvalidParameterType
		}
		return QueryF32(normalized), nil
	case paramKindString:
		typed, ok := value.(string)
		if !ok {
			return nil, ErrInvalidParameterType
		}
		return QueryString(typed), nil
	case paramKindDateTime:
		typed, ok := value.(string)
		if !ok {
			return nil, ErrInvalidDateTimeParameter
		}
		dateTime, err := ParseDateTimeRFC3339(typed)
		if err != nil {
			return nil, ErrInvalidDateTimeParameter
		}
		normalized, err := dateTime.RFC3339()
		if err != nil {
			return nil, ErrInvalidDateTimeParameter
		}
		return QueryString(normalized), nil
	case paramKindBytes:
		return nil, ErrUnsupportedBytesParameter
	case paramKindValue:
		return queryValueFromValue(value, path)
	case paramKindObject:
		reflected := reflect.ValueOf(value)
		if !reflected.IsValid() || reflected.Kind() != reflect.Map || reflected.Type().Key().Kind() != reflect.String {
			return nil, ErrInvalidParameterType
		}
		return queryValueFromValue(value, path)
	case paramKindArray:
		reflected := reflect.ValueOf(value)
		if !reflected.IsValid() || (reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array) {
			return nil, ErrInvalidParameterType
		}
		if parameterType.inner == nil {
			return nil, ErrInvalidParameterType
		}
		values := make([]QueryValue, reflected.Len())
		for index := 0; index < reflected.Len(); index++ {
			normalized, err := normalizeTypedQueryValue(
				*parameterType.inner,
				reflected.Index(index).Interface(),
				fmt.Sprintf("%s[%d]", path, index),
			)
			if err != nil {
				return nil, err
			}
			values[index] = normalized
		}
		return values, nil
	default:
		return nil, ErrInvalidParameterType
	}
}

func queryValueFromValue(value any, path string) (QueryValue, error) {
	pv, err := PropertyValueOf(value)
	if err != nil {
		return nil, err
	}
	return queryValueFromPropertyValue(pv, path)
}
func queryValueFromPropertyValue(value PropertyValue, path string) (QueryValue, error) {
	if value.err != nil {
		return nil, value.err
	}
	switch value.kind {
	case "Null":
		return nil, nil
	case "Bool", "I64", "F64", "F32", "String":
		return value.value, nil
	case "DateTime":
		return DateTimeFromMillis(value.value.(int64)).RFC3339()
	case "Bytes":
		return nil, ErrUnsupportedBytesParameter
	case "I64Array", "F64Array", "F32Array", "StringArray":
		return value.value, nil
	case "Array":
		vals := value.value.([]PropertyValue)
		out := make([]QueryValue, len(vals))
		for i, val := range vals {
			converted, err := queryValueFromPropertyValue(val, fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return nil, err
			}
			out[i] = converted
		}
		return out, nil
	case "Object":
		vals := value.value.(map[string]PropertyValue)
		out := make(map[string]QueryValue, len(vals))
		for key, val := range vals {
			converted, err := queryValueFromPropertyValue(val, path+"."+key)
			if err != nil {
				return nil, err
			}
			out[key] = converted
		}
		return out, nil
	default:
		return nil, ErrInvalidParameterType
	}
}

func compactJSON(value any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(buf.Bytes()), nil
}
