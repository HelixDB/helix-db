package helix

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"
)

var (
	ErrWriteTraversalInReadBatch = errors.New("helix: read batch cannot contain write traversal")
	ErrUnsupportedBytesParameter = errors.New("helix: dynamic query JSON cannot represent bytes parameters")
	ErrDuplicateParameter        = errors.New("helix: duplicate parameter")
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
		return json.Marshal("Null")
	case "Bytes":
		bytesValue := p.value.([]byte)
		ints := make([]int, len(bytesValue))
		for i, b := range bytesValue {
			ints[i] = int(b)
		}
		return json.Marshal(map[string]any{p.kind: ints})
	case "F32":
		return json.Marshal(map[string]any{p.kind: float32(p.value.(float32))})
	default:
		return json.Marshal(map[string]any{p.kind: p.value})
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
		return json.Marshal(map[string]any{"Expr": *p.expr})
	}
	if p.value == nil {
		return json.Marshal(map[string]any{"Value": Null()})
	}
	return json.Marshal(map[string]any{"Value": *p.value})
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
		return json.Marshal("All")
	}
	return json.Marshal(map[string]any{n.kind: n.value})
}

type EdgeRef struct {
	kind  string
	value any
}

func EdgeID(id uint64) EdgeRef { return EdgeRef{kind: "Ids", value: []uint64{id}} }
func EdgeIDs(ids ...uint64) EdgeRef {
	return EdgeRef{kind: "Ids", value: append([]uint64(nil), ids...)}
}
func EdgeVar(name string) EdgeRef   { return EdgeRef{kind: "Var", value: name} }
func EdgeParam(name string) EdgeRef { return EdgeRef{kind: "Param", value: name} }

func (e EdgeRef) MarshalJSON() ([]byte, error) { return json.Marshal(map[string]any{e.kind: e.value}) }

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

type CaseBranch struct {
	When Predicate
	Then Expr
}

func ExprCase(branches []CaseBranch, elseExpr *Expr) Expr {
	whenThen := make([][2]any, len(branches))
	for i, branch := range branches {
		whenThen[i] = [2]any{branch.When, branch.Then}
	}
	return Expr{kind: "Case", value: struct {
		WhenThen [][2]any `json:"when_then"`
		ElseExpr *Expr    `json:"else_expr"`
	}{WhenThen: whenThen, ElseExpr: elseExpr}}
}

func (e Expr) MarshalJSON() ([]byte, error) {
	switch e.kind {
	case "Id", "Timestamp", "DateTimeNow":
		return json.Marshal(e.kind)
	default:
		return json.Marshal(map[string]any{e.kind: e.value})
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
		return json.Marshal(map[string]any{"Expr": *s.expr})
	}
	value := 0
	if s.literal != nil {
		value = *s.literal
	}
	return json.Marshal(map[string]any{"Literal": value})
}

type CompareOp string

const (
	CompareEq  CompareOp = "Eq"
	CompareNeq CompareOp = "Neq"
	CompareGt  CompareOp = "Gt"
	CompareGte CompareOp = "Gte"
	CompareLt  CompareOp = "Lt"
	CompareLte CompareOp = "Lte"
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
	return Predicate{kind: "Compare", value: struct {
		Left  Expr      `json:"left"`
		Op    CompareOp `json:"op"`
		Right Expr      `json:"right"`
	}{left, op, right}}
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
	return json.Marshal(map[string]any{p.kind: p.value})
}

type SourcePredicate struct {
	kind  string
	value any
}

func SourceEq(property string, value any) SourcePredicate {
	return sourceComparison("Eq", property, value)
}
func SourceNeq(property string, value any) SourcePredicate {
	return sourceComparison("Neq", property, value)
}
func SourceGt(property string, value any) SourcePredicate {
	return sourceComparison("Gt", property, value)
}
func SourceGte(property string, value any) SourcePredicate {
	return sourceComparison("Gte", property, value)
}
func SourceLt(property string, value any) SourcePredicate {
	return sourceComparison("Lt", property, value)
}
func SourceLte(property string, value any) SourcePredicate {
	return sourceComparison("Lte", property, value)
}
func SourceHasKey(property string) SourcePredicate {
	return SourcePredicate{kind: "HasKey", value: property}
}
func SourceStartsWith(property, prefix string) SourcePredicate {
	return SourcePredicate{kind: "StartsWith", value: []any{property, prefix}}
}
func SourceAnd(preds ...SourcePredicate) SourcePredicate {
	return SourcePredicate{kind: "And", value: preds}
}
func SourceOr(preds ...SourcePredicate) SourcePredicate {
	return SourcePredicate{kind: "Or", value: preds}
}
func SourceBetween(property string, min any, max any) SourcePredicate {
	pred := PredBetween(property, min, max)
	return SourcePredicate{kind: pred.kind, value: pred.value}
}

func sourceComparison(kind, property string, value any) SourcePredicate {
	pred := comparisonPredicate(kind, property, value)
	return SourcePredicate{kind: pred.kind, value: pred.value}
}

func (s SourcePredicate) Predicate() Predicate { return Predicate{kind: s.kind, value: s.value} }
func (s SourcePredicate) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{s.kind: s.value})
}

type Projection struct {
	Source string `json:"source,omitempty"`
	Alias  string `json:"alias"`
	Expr   *Expr  `json:"expr,omitempty"`
}

func ProjectProp(source string) Projection           { return Projection{Source: source, Alias: source} }
func ProjectPropAs(source, alias string) Projection  { return Projection{Source: source, Alias: alias} }
func ProjectExpr(alias string, expr Expr) Projection { return Projection{Alias: alias, Expr: &expr} }

func (p Projection) MarshalJSON() ([]byte, error) {
	if p.Expr != nil {
		return json.Marshal(struct {
			Alias string `json:"alias"`
			Expr  Expr   `json:"expr"`
		}{p.Alias, *p.Expr})
	}
	return json.Marshal(struct {
		Source string `json:"source"`
		Alias  string `json:"alias"`
	}{p.Source, p.Alias})
}

type Order string

const (
	OrderAsc  Order = "Asc"
	OrderDesc Order = "Desc"
)

type Ordering struct {
	Property string
	Order    Order
}

func (o Ordering) MarshalJSON() ([]byte, error) { return json.Marshal([]any{o.Property, o.Order}) }

type AggregateFunction string

const (
	AggregateCount AggregateFunction = "Count"
	AggregateSum   AggregateFunction = "Sum"
	AggregateMin   AggregateFunction = "Min"
	AggregateMax   AggregateFunction = "Max"
	AggregateMean  AggregateFunction = "Mean"
)

type EmitBehavior string

const (
	EmitNone   EmitBehavior = "None"
	EmitBefore EmitBehavior = "Before"
	EmitAfter  EmitBehavior = "After"
	EmitAll    EmitBehavior = "All"
)

type RangeIndexDirection string

const (
	RangeIndexAsc  RangeIndexDirection = "Asc"
	RangeIndexDesc RangeIndexDirection = "Desc"
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

type IndexSpec struct {
	kind  string
	value any
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
func NodeVectorIndex(label, property string, tenantProperty ...string) IndexSpec {
	return tenantIndex("NodeVector", label, property, tenantProperty...)
}
func NodeTextIndex(label, property string, tenantProperty ...string) IndexSpec {
	return tenantIndex("NodeText", label, property, tenantProperty...)
}
func EdgeVectorIndex(label, property string, tenantProperty ...string) IndexSpec {
	return tenantIndex("EdgeVector", label, property, tenantProperty...)
}
func EdgeTextIndex(label, property string, tenantProperty ...string) IndexSpec {
	return tenantIndex("EdgeText", label, property, tenantProperty...)
}

func rangeIndexFields(label, property string, direction RangeIndexDirection) map[string]any {
	value := map[string]any{"label": label, "property": property}
	if direction != RangeIndexAsc {
		value["direction"] = direction
	}
	return value
}

func tenantIndex(kind, label, property string, tenantProperty ...string) IndexSpec {
	value := map[string]any{"label": label, "property": property}
	if len(tenantProperty) > 0 && tenantProperty[0] != "" {
		value["tenant_property"] = tenantProperty[0]
	}
	return IndexSpec{kind: kind, value: value}
}

func (i IndexSpec) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{i.kind: i.value})
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

type indexStepSpec struct {
	Label          string  `json:"label"`
	Property       string  `json:"property"`
	TenantProperty *string `json:"tenant_property,omitempty"`
}

func CreateVectorIndexNodesStep(label, property string, tenantProperty ...string) Step {
	return step("CreateVectorIndexNodes", newIndexStepSpec(label, property, tenantProperty...))
}

func CreateVectorIndexEdgesStep(label, property string, tenantProperty ...string) Step {
	return step("CreateVectorIndexEdges", newIndexStepSpec(label, property, tenantProperty...))
}

func CreateTextIndexNodesStep(label, property string, tenantProperty ...string) Step {
	return step("CreateTextIndexNodes", newIndexStepSpec(label, property, tenantProperty...))
}

func CreateTextIndexEdgesStep(label, property string, tenantProperty ...string) Step {
	return step("CreateTextIndexEdges", newIndexStepSpec(label, property, tenantProperty...))
}

func newIndexStepSpec(label, property string, tenantProperty ...string) indexStepSpec {
	spec := indexStepSpec{Label: label, Property: property}
	if len(tenantProperty) > 0 && tenantProperty[0] != "" {
		spec.TenantProperty = &tenantProperty[0]
	}
	return spec
}

type Step struct {
	kind  string
	value any
	unit  bool
}

func unitStep(kind string) Step        { return Step{kind: kind, unit: true} }
func step(kind string, value any) Step { return Step{kind: kind, value: value} }

func (s Step) MarshalJSON() ([]byte, error) {
	if s.unit {
		return json.Marshal(s.kind)
	}
	return json.Marshal(map[string]any{s.kind: s.value})
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
func (t *Traversal) Validate() error {
	if t == nil {
		return errors.New("helix: nil traversal")
	}
	return t.err
}
func (t *Traversal) Err() error { return t.Validate() }
func (t *Traversal) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Steps []Step `json:"steps"`
	}{t.steps})
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
func (t *Traversal) Count() *Traversal             { return t.addTerminal(unitStep("Count")) }
func (t *Traversal) Exists() *Traversal            { return t.addTerminal(unitStep("Exists")) }
func (t *Traversal) ID() *Traversal                { return t.addTerminal(unitStep("Id")) }
func (t *Traversal) Label() *Traversal             { return t.addTerminal(unitStep("Label")) }
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
func (t *Traversal) CreateVectorIndexNodes(label, property string, tenantProperty ...string) *Traversal {
	return t.CreateIndexIfNotExists(NodeVectorIndex(label, property, tenantProperty...))
}
func (t *Traversal) CreateVectorIndexEdges(label, property string, tenantProperty ...string) *Traversal {
	return t.CreateIndexIfNotExists(EdgeVectorIndex(label, property, tenantProperty...))
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

func optionalString(values []string) *string {
	if len(values) == 0 || values[0] == "" {
		return nil
	}
	return &values[0]
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
	return json.Marshal(struct {
		Steps []Step `json:"steps"`
	}{s.steps})
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

type BatchCondition struct {
	kind  string
	value any
}

func VarNotEmpty(name string) BatchCondition { return BatchCondition{kind: "VarNotEmpty", value: name} }
func VarEmpty(name string) BatchCondition    { return BatchCondition{kind: "VarEmpty", value: name} }
func VarMinSize(name string, size int) BatchCondition {
	return BatchCondition{kind: "VarMinSize", value: []any{name, size}}
}
func PrevNotEmpty() BatchCondition { return BatchCondition{kind: "PrevNotEmpty"} }
func (b BatchCondition) MarshalJSON() ([]byte, error) {
	if b.kind == "PrevNotEmpty" {
		return json.Marshal("PrevNotEmpty")
	}
	return json.Marshal(map[string]any{b.kind: b.value})
}

type NamedQuery struct {
	Name      string          `json:"name"`
	Steps     []Step          `json:"steps"`
	Condition *BatchCondition `json:"condition"`
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
		return json.Marshal(map[string]any{"ForEach": b.forEach})
	}
	return json.Marshal(map[string]any{"Query": b.query})
}

type batchBase struct {
	queries []BatchEntry
	returns []string
	err     error
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
	b.queries = append(b.queries, queryEntry(NamedQuery{Name: name, Steps: traversal.Steps()}))
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
	return json.Marshal(struct {
		Queries []BatchEntry `json:"queries"`
		Returns []string     `json:"returns"`
	}{b.queries, b.returns})
}

func (b *WriteBatch) VarAs(name string, traversal *Traversal) *WriteBatch {
	if traversal == nil {
		b.err = errors.New("helix: nil traversal")
		return b
	}
	if err := traversal.Validate(); err != nil && b.err == nil {
		b.err = err
	}
	b.queries = append(b.queries, queryEntry(NamedQuery{Name: name, Steps: traversal.Steps()}))
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
	return json.Marshal(struct {
		Queries []BatchEntry `json:"queries"`
		Returns []string     `json:"returns"`
	}{b.queries, b.returns})
}

type ParamKind string
type QueryParamType struct {
	Kind  ParamKind
	Inner *QueryParamType
}

func ParamTypeBool() QueryParamType     { return QueryParamType{Kind: "Bool"} }
func ParamTypeI64() QueryParamType      { return QueryParamType{Kind: "I64"} }
func ParamTypeF64() QueryParamType      { return QueryParamType{Kind: "F64"} }
func ParamTypeF32() QueryParamType      { return QueryParamType{Kind: "F32"} }
func ParamTypeString() QueryParamType   { return QueryParamType{Kind: "String"} }
func ParamTypeDateTime() QueryParamType { return QueryParamType{Kind: "DateTime"} }
func ParamTypeBytes() QueryParamType    { return QueryParamType{Kind: "Bytes"} }
func ParamTypeValue() QueryParamType    { return QueryParamType{Kind: "Value"} }
func ParamTypeObject() QueryParamType   { return QueryParamType{Kind: "Object"} }
func ParamTypeArray(inner QueryParamType) QueryParamType {
	return QueryParamType{Kind: "Array", Inner: &inner}
}
func (q QueryParamType) MarshalJSON() ([]byte, error) {
	if q.Kind == "Array" {
		return json.Marshal(map[string]any{"Array": q.Inner})
	}
	return json.Marshal(string(q.Kind))
}

type ParamRef struct {
	Name string
	Type QueryParamType
}

func (p ParamRef) Expr() Expr                   { return ExprParam(p.Name) }
func (p ParamRef) Input() PropertyInput         { return ParamInput(p.Name) }
func (p ParamRef) Bound() StreamBound           { return BoundExpr(p.Expr()) }
func (p ParamRef) MarshalJSON() ([]byte, error) { return p.Expr().MarshalJSON() }

type DynamicValue any

func DynamicNull() DynamicValue                                 { return nil }
func DynamicBool(value bool) DynamicValue                       { return value }
func DynamicI64(value int64) DynamicValue                       { return value }
func DynamicF64(value float64) DynamicValue                     { return value }
func DynamicF32(value float32) DynamicValue                     { return value }
func DynamicString(value string) DynamicValue                   { return value }
func DynamicArray(values ...DynamicValue) DynamicValue          { return values }
func DynamicObject(values map[string]DynamicValue) DynamicValue { return values }

type queryBuilder struct {
	requestType string
	queryName   *string
	batch       batchBase
	parameters  map[string]DynamicValue
	types       map[string]QueryParamType
	err         error
	write       bool
}

type ReadQueryBuilder struct{ queryBuilder }
type WriteQueryBuilder struct{ queryBuilder }

func ReadQuery(name string) *ReadQueryBuilder {
	return &ReadQueryBuilder{queryBuilder: newQueryBuilder("read", name, false)}
}
func WriteQuery(name string) *WriteQueryBuilder {
	return &WriteQueryBuilder{queryBuilder: newQueryBuilder("write", name, true)}
}
func newQueryBuilder(requestType, name string, write bool) queryBuilder {
	var queryName *string
	if name != "" {
		queryName = &name
	}
	return queryBuilder{requestType: requestType, queryName: queryName, parameters: map[string]DynamicValue{}, types: map[string]QueryParamType{}, write: write}
}
func (q *queryBuilder) Validate() error {
	if q.err != nil {
		return q.err
	}
	return q.batch.err
}
func (q *queryBuilder) isHelixRequest() {}
func (q *queryBuilder) addParam(name string, ty QueryParamType, value DynamicValue, err error) ParamRef {
	if _, exists := q.types[name]; exists && q.err == nil {
		q.err = &PathError{Path: name, Err: ErrDuplicateParameter}
	}
	if err != nil && q.err == nil {
		q.err = &PathError{Path: name, Err: err}
	}
	q.types[name] = ty
	q.parameters[name] = value
	return ParamRef{Name: name, Type: ty}
}
func (q *queryBuilder) ParamBool(name string, value bool) ParamRef {
	return q.addParam(name, ParamTypeBool(), DynamicBool(value), nil)
}
func (q *queryBuilder) ParamI64(name string, value any) ParamRef {
	v, err := dynamicI64(value)
	return q.addParam(name, ParamTypeI64(), DynamicI64(v), err)
}
func (q *queryBuilder) ParamF64(name string, value any) ParamRef {
	v, err := dynamicFloat64(value)
	return q.addParam(name, ParamTypeF64(), DynamicF64(v), err)
}
func (q *queryBuilder) ParamF32(name string, value any) ParamRef {
	v, err := dynamicFloat64(value)
	return q.addParam(name, ParamTypeF32(), DynamicF32(float32(v)), err)
}
func (q *queryBuilder) ParamString(name string, value string) ParamRef {
	return q.addParam(name, ParamTypeString(), DynamicString(value), nil)
}
func (q *queryBuilder) ParamDateTime(name string, value any) ParamRef {
	v, err := dynamicDateTime(value)
	return q.addParam(name, ParamTypeDateTime(), DynamicString(v), err)
}
func (q *queryBuilder) ParamValue(name string, value any) ParamRef {
	v, err := dynamicFromValue(value, name)
	return q.addParam(name, ParamTypeValue(), v, err)
}
func (q *queryBuilder) ParamObject(name string, value any, inner ...QueryParamType) ParamRef {
	v, err := dynamicFromValue(value, name)
	return q.addParam(name, ParamTypeObject(), v, err)
}
func (q *queryBuilder) ParamArray(name string, value any, inner QueryParamType) ParamRef {
	v, err := dynamicFromValue(value, name)
	return q.addParam(name, ParamTypeArray(inner), v, err)
}
func (q *queryBuilder) MarshalJSON() ([]byte, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	payload := struct {
		RequestType    string                    `json:"request_type"`
		QueryName      *string                   `json:"query_name"`
		Query          any                       `json:"query"`
		Parameters     map[string]DynamicValue   `json:"parameters,omitempty"`
		ParameterTypes map[string]QueryParamType `json:"parameter_types,omitempty"`
	}{RequestType: q.requestType, QueryName: q.queryName, Query: struct {
		Queries []BatchEntry `json:"queries"`
		Returns []string     `json:"returns"`
	}{q.batch.queries, q.batch.returns}}
	if len(q.parameters) > 0 {
		payload.Parameters = q.parameters
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
	q.batch.queries = append(q.batch.queries, queryEntry(NamedQuery{Name: name, Steps: traversal.Steps()}))
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
	return &q.queryBuilder
}
func (q *ReadQueryBuilder) ParamBool(name string, value bool) ParamRef {
	return q.queryBuilder.ParamBool(name, value)
}
func (q *ReadQueryBuilder) ParamI64(name string, value any) ParamRef {
	return q.queryBuilder.ParamI64(name, value)
}
func (q *ReadQueryBuilder) ParamF64(name string, value any) ParamRef {
	return q.queryBuilder.ParamF64(name, value)
}
func (q *ReadQueryBuilder) ParamF32(name string, value any) ParamRef {
	return q.queryBuilder.ParamF32(name, value)
}
func (q *ReadQueryBuilder) ParamString(name string, value string) ParamRef {
	return q.queryBuilder.ParamString(name, value)
}
func (q *ReadQueryBuilder) ParamDateTime(name string, value any) ParamRef {
	return q.queryBuilder.ParamDateTime(name, value)
}
func (q *ReadQueryBuilder) ParamValue(name string, value any) ParamRef {
	return q.queryBuilder.ParamValue(name, value)
}
func (q *ReadQueryBuilder) ParamObject(name string, value any, inner ...QueryParamType) ParamRef {
	return q.queryBuilder.ParamObject(name, value, inner...)
}
func (q *ReadQueryBuilder) ParamArray(name string, value any, inner QueryParamType) ParamRef {
	return q.queryBuilder.ParamArray(name, value, inner)
}

func (q *WriteQueryBuilder) VarAs(name string, traversal *Traversal) *WriteQueryBuilder {
	if traversal == nil {
		q.err = errors.New("helix: nil traversal")
		return q
	}
	if err := traversal.Validate(); err != nil && q.err == nil {
		q.err = err
	}
	q.batch.queries = append(q.batch.queries, queryEntry(NamedQuery{Name: name, Steps: traversal.Steps()}))
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
	return &q.queryBuilder
}
func (q *WriteQueryBuilder) ParamBool(name string, value bool) ParamRef {
	return q.queryBuilder.ParamBool(name, value)
}
func (q *WriteQueryBuilder) ParamI64(name string, value any) ParamRef {
	return q.queryBuilder.ParamI64(name, value)
}
func (q *WriteQueryBuilder) ParamF64(name string, value any) ParamRef {
	return q.queryBuilder.ParamF64(name, value)
}
func (q *WriteQueryBuilder) ParamF32(name string, value any) ParamRef {
	return q.queryBuilder.ParamF32(name, value)
}
func (q *WriteQueryBuilder) ParamString(name string, value string) ParamRef {
	return q.queryBuilder.ParamString(name, value)
}
func (q *WriteQueryBuilder) ParamDateTime(name string, value any) ParamRef {
	return q.queryBuilder.ParamDateTime(name, value)
}
func (q *WriteQueryBuilder) ParamValue(name string, value any) ParamRef {
	return q.queryBuilder.ParamValue(name, value)
}
func (q *WriteQueryBuilder) ParamObject(name string, value any, inner ...QueryParamType) ParamRef {
	return q.queryBuilder.ParamObject(name, value, inner...)
}
func (q *WriteQueryBuilder) ParamArray(name string, value any, inner QueryParamType) ParamRef {
	return q.queryBuilder.ParamArray(name, value, inner)
}

func dynamicI64(value any) (int64, error) {
	pv, err := PropertyValueOf(value)
	if err != nil {
		return 0, err
	}
	if pv.kind != "I64" {
		return 0, ErrInvalidParameterType
	}
	return pv.value.(int64), nil
}
func dynamicFloat64(value any) (float64, error) {
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
func dynamicDateTime(value any) (string, error) {
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
func dynamicFromValue(value any, path string) (DynamicValue, error) {
	pv, err := PropertyValueOf(value)
	if err != nil {
		return nil, err
	}
	return dynamicFromPropertyValue(pv, path)
}
func dynamicFromPropertyValue(value PropertyValue, path string) (DynamicValue, error) {
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
		out := make([]DynamicValue, len(vals))
		for i, val := range vals {
			converted, err := dynamicFromPropertyValue(val, fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return nil, err
			}
			out[i] = converted
		}
		return out, nil
	case "Object":
		vals := value.value.(map[string]PropertyValue)
		out := make(map[string]DynamicValue, len(vals))
		for key, val := range vals {
			converted, err := dynamicFromPropertyValue(val, path+"."+key)
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
