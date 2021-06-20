package sqltypes

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/operators"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

var (
	Date     = decls.NewAbstractType("DATE")
	Time     = decls.NewAbstractType("TIME")
	DateTime = decls.NewAbstractType("DATETIME")
	Interval = decls.NewAbstractType("INTERVAL")
	DatePart = decls.NewAbstractType("date_part")
)

func newConstantString(str string) *expr.Constant {
	return &expr.Constant{ConstantKind: &expr.Constant_StringValue{StringValue: str}}
}

var SQLTypeDeclarations = cel.Declarations(
	// constants
	decls.NewConst("MICROSECOND", DatePart, newConstantString("MICROSECOND")),
	decls.NewConst("MILLISECOND", DatePart, newConstantString("MILLISECOND")),
	decls.NewConst("SECOND", DatePart, newConstantString("SECOND")),
	decls.NewConst("MINUTE", DatePart, newConstantString("MINUTE")),
	decls.NewConst("HOUR", DatePart, newConstantString("HOUR")),
	decls.NewConst("DAY", DatePart, newConstantString("DAY")),
	decls.NewConst("DAYOFWEEK", DatePart, newConstantString("DAYOFWEEK")),
	decls.NewConst("WEEK", DatePart, newConstantString("WEEK")),
	decls.NewConst("ISOWEEK", DatePart, newConstantString("ISOWEEK")),
	decls.NewConst("MONTH", DatePart, newConstantString("MONTH")),
	decls.NewConst("QUARTER", DatePart, newConstantString("QUARTER")),
	decls.NewConst("YEAR", DatePart, newConstantString("YEAR")),
	decls.NewConst("ISOYEAR", DatePart, newConstantString("ISOYEAR")),

	// functions
	decls.NewFunction("date",
		decls.NewOverload("date_construct_year_month_day", []*expr.Type{decls.Int, decls.Int, decls.Int}, Date),
		decls.NewOverload("date_construct_string", []*expr.Type{decls.String}, Date),
	),
	decls.NewFunction("time",
		decls.NewOverload("time_construct_hour_minute_second", []*expr.Type{decls.Int, decls.Int, decls.Int}, Time),
		decls.NewOverload("time_construct_string", []*expr.Type{decls.String}, Time),
		decls.NewOverload("time_construct_datetime", []*expr.Type{DateTime}, Time),
		decls.NewOverload("time_construct_timestamp", []*expr.Type{decls.Timestamp}, Time),
		decls.NewOverload("time_construct_timestamp_timezone", []*expr.Type{decls.Timestamp, decls.String}, Time),
	),
	decls.NewFunction("datetime",
		decls.NewOverload("datetime_construct_year_month_day_hour_minute_second", []*expr.Type{decls.Int, decls.Int, decls.Int, decls.Int, decls.Int, decls.Int}, DateTime),
		decls.NewOverload("datetime_construct_string", []*expr.Type{decls.String}, DateTime),
		decls.NewOverload("datetime_construct_date", []*expr.Type{Date}, DateTime),
		decls.NewOverload("datetime_construct_date_time", []*expr.Type{Date, Time}, DateTime),
		decls.NewOverload("datetime_construct_timestamp", []*expr.Type{decls.Timestamp}, DateTime),
		decls.NewOverload("datetime_construct_timestamp_timezone", []*expr.Type{decls.Timestamp, decls.String}, DateTime),
	),
	decls.NewFunction("timestamp",
		decls.NewOverload("timestamp_construct_string_timezone", []*expr.Type{decls.String, decls.String}, decls.Timestamp),
		decls.NewOverload("timestamp_construct_date", []*expr.Type{Date}, decls.Timestamp),
		decls.NewOverload("timestamp_construct_date_timezone", []*expr.Type{Date, decls.String}, decls.Timestamp),
		decls.NewOverload("timestamp_construct_datetime", []*expr.Type{DateTime}, decls.Timestamp),
		decls.NewOverload("timestamp_construct_datetime_timezone", []*expr.Type{DateTime, decls.String}, decls.Timestamp),
	),
	decls.NewFunction("interval",
		decls.NewOverload("interval_construct", []*expr.Type{decls.Int, DatePart}, Interval),
	),
	decls.NewFunction("current_date",
		decls.NewOverload("current_date", []*expr.Type{}, Date),
		decls.NewOverload("current_date_timezone", []*expr.Type{decls.String}, Date),
	),
	decls.NewFunction("current_time",
		decls.NewOverload("current_time", []*expr.Type{}, Time),
		decls.NewOverload("current_time_timezone", []*expr.Type{decls.String}, Time),
	),
	decls.NewFunction("current_datetime",
		decls.NewOverload("current_datetime", []*expr.Type{}, DateTime),
		decls.NewOverload("current_datetime_timezone", []*expr.Type{decls.String}, DateTime),
	),
	decls.NewFunction("current_timestamp",
		decls.NewOverload("current_timestamp", []*expr.Type{}, decls.Timestamp),
	),

	// operators
	decls.NewFunction(operators.Less,
		decls.NewOverload("less_date", []*expr.Type{Date, Date}, decls.Bool),
		decls.NewOverload("less_time", []*expr.Type{Time, Time}, decls.Bool),
		decls.NewOverload("less_datetime", []*expr.Type{DateTime, DateTime}, decls.Bool),
	),
	decls.NewFunction(operators.LessEquals,
		decls.NewOverload("less_equals_date", []*expr.Type{Date, Date}, decls.Bool),
		decls.NewOverload("less_equals_time", []*expr.Type{Time, Time}, decls.Bool),
		decls.NewOverload("less_equals_datetime", []*expr.Type{DateTime, DateTime}, decls.Bool),
	),
	decls.NewFunction(operators.Greater,
		decls.NewOverload("greater_date", []*expr.Type{Date, Date}, decls.Bool),
		decls.NewOverload("greater_time", []*expr.Type{Time, Time}, decls.Bool),
		decls.NewOverload("greater_datetime", []*expr.Type{DateTime, DateTime}, decls.Bool),
	),
	decls.NewFunction(operators.GreaterEquals,
		decls.NewOverload("greater_equals_date", []*expr.Type{Date, Date}, decls.Bool),
		decls.NewOverload("greater_equals_time", []*expr.Type{Time, Time}, decls.Bool),
		decls.NewOverload("greater_equals_datetime", []*expr.Type{DateTime, DateTime}, decls.Bool),
	),
	decls.NewFunction(operators.Add,
		decls.NewOverload("add_date_int", []*expr.Type{Date, decls.Int}, Date),
		decls.NewOverload("add_int_date", []*expr.Type{decls.Int, Date}, Date),
	),
	decls.NewFunction(operators.Subtract,
		decls.NewOverload("subtract_date_int", []*expr.Type{Date, decls.Int}, Date),
	),
	decls.NewFunction(operators.Add,
		decls.NewOverload("add_date_interval", []*expr.Type{Date, Interval}, Date),
		decls.NewOverload("add_date_duration", []*expr.Type{Date, decls.Duration}, Date),
		decls.NewOverload("add_interval_date", []*expr.Type{Interval, Date}, Date),
		decls.NewOverload("add_duration_date", []*expr.Type{decls.Duration, Date}, Date),
		decls.NewOverload("add_time_interval", []*expr.Type{Time, Interval}, Time),
		decls.NewOverload("add_time_duration", []*expr.Type{Time, decls.Duration}, Time),
		decls.NewOverload("add_interval_time", []*expr.Type{Interval, Time}, Time),
		decls.NewOverload("add_duration_time", []*expr.Type{decls.Duration, Time}, Time),
		decls.NewOverload("add_datetime_interval", []*expr.Type{DateTime, Interval}, DateTime),
		decls.NewOverload("add_datetime_duration", []*expr.Type{DateTime, decls.Duration}, DateTime),
		decls.NewOverload("add_interval_datetime", []*expr.Type{Interval, DateTime}, DateTime),
		decls.NewOverload("add_duration_datetime", []*expr.Type{decls.Duration, DateTime}, DateTime),
		decls.NewOverload("add_timestamp_interval", []*expr.Type{decls.Timestamp, Interval}, decls.Timestamp),
		decls.NewOverload("add_interval_timestamp", []*expr.Type{Interval, decls.Timestamp}, decls.Timestamp),
	),
	decls.NewFunction(operators.Subtract,
		decls.NewOverload("subtract_date_interval", []*expr.Type{Date, Interval}, Date),
		decls.NewOverload("subtract_date_duration", []*expr.Type{Date, decls.Duration}, Date),
		decls.NewOverload("subtract_time_interval", []*expr.Type{Time, Interval}, Time),
		decls.NewOverload("subtract_time_duration", []*expr.Type{Time, decls.Duration}, Time),
		decls.NewOverload("subtract_datetime_interval", []*expr.Type{DateTime, Interval}, DateTime),
		decls.NewOverload("subtract_datetime_duration", []*expr.Type{DateTime, decls.Duration}, DateTime),
		decls.NewOverload("subtract_timestamp_interval", []*expr.Type{decls.Timestamp, Interval}, decls.Timestamp),
	),
)
