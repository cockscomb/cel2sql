package sqltypes

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/operators"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

var (
	Date      = decls.NewAbstractType("DATE")
	Time      = decls.NewAbstractType("TIME")
	DateTime  = decls.NewAbstractType("DATETIME")
	Timestamp = decls.NewAbstractType("TIMESTAMP")
)

var SQLTypeDeclarations = cel.Declarations(
	decls.NewFunction("date",
		decls.NewOverload("date_construct_year_month_day", []*expr.Type{decls.Int, decls.Int, decls.Int}, Date),
		decls.NewOverload("date_construct_string", []*expr.Type{decls.String}, Date),
	),
	decls.NewFunction("time",
		decls.NewOverload("time_construct_hour_minute_second", []*expr.Type{decls.Int, decls.Int, decls.Int}, Time),
		decls.NewOverload("time_construct_string", []*expr.Type{decls.String}, Time),
		decls.NewOverload("time_construct_datetime", []*expr.Type{DateTime}, Time),
		decls.NewOverload("time_construct_timestamp", []*expr.Type{Timestamp}, Time),
		decls.NewOverload("time_construct_timestamp_timezone", []*expr.Type{Timestamp, decls.String}, Time),
	),
	decls.NewFunction("datetime",
		decls.NewOverload("datetime_construct_year_month_day_hour_minute_second", []*expr.Type{decls.Int, decls.Int, decls.Int, decls.Int, decls.Int, decls.Int}, DateTime),
		decls.NewOverload("datetime_construct_string", []*expr.Type{decls.String}, DateTime),
		decls.NewOverload("datetime_construct_date", []*expr.Type{Date}, DateTime),
		decls.NewOverload("datetime_construct_date_time", []*expr.Type{Date, Time}, DateTime),
		decls.NewOverload("datetime_construct_timestamp", []*expr.Type{Timestamp}, DateTime),
		decls.NewOverload("datetime_construct_timestamp_timezone", []*expr.Type{Timestamp, decls.String}, DateTime),
	),
	decls.NewFunction("timestamp",
		decls.NewOverload("timestamp_construct_string", []*expr.Type{decls.String}, Timestamp),
		decls.NewOverload("timestamp_construct_string_timezone", []*expr.Type{decls.String, decls.String}, Timestamp),
		decls.NewOverload("timestamp_construct_date", []*expr.Type{Date}, Timestamp),
		decls.NewOverload("timestamp_construct_date_timezone", []*expr.Type{Date, decls.String}, Timestamp),
		decls.NewOverload("timestamp_construct_datetime", []*expr.Type{DateTime}, Timestamp),
		decls.NewOverload("timestamp_construct_datetime_timezone", []*expr.Type{DateTime, decls.String}, Timestamp),
	),

	// operators
	decls.NewFunction(operators.Less,
		decls.NewOverload("less_date", []*expr.Type{Date, Date}, decls.Bool),
		decls.NewOverload("less_time", []*expr.Type{Time, Time}, decls.Bool),
		decls.NewOverload("less_datetime", []*expr.Type{DateTime, DateTime}, decls.Bool),
		decls.NewOverload("less_timestamp", []*expr.Type{Timestamp, Timestamp}, decls.Bool),
	),
	decls.NewFunction(operators.LessEquals,
		decls.NewOverload("less_equals_date", []*expr.Type{Date, Date}, decls.Bool),
		decls.NewOverload("less_equals_time", []*expr.Type{Time, Time}, decls.Bool),
		decls.NewOverload("less_equals_datetime", []*expr.Type{DateTime, DateTime}, decls.Bool),
		decls.NewOverload("less_equals_timestamp", []*expr.Type{Timestamp, Timestamp}, decls.Bool),
	),
	decls.NewFunction(operators.Greater,
		decls.NewOverload("greater_date", []*expr.Type{Date, Date}, decls.Bool),
		decls.NewOverload("greater_time", []*expr.Type{Time, Time}, decls.Bool),
		decls.NewOverload("greater_datetime", []*expr.Type{DateTime, DateTime}, decls.Bool),
		decls.NewOverload("greater_timestamp", []*expr.Type{Timestamp, Timestamp}, decls.Bool),
	),
	decls.NewFunction(operators.GreaterEquals,
		decls.NewOverload("greater_equals_date", []*expr.Type{Date, Date}, decls.Bool),
		decls.NewOverload("greater_equals_time", []*expr.Type{Time, Time}, decls.Bool),
		decls.NewOverload("greater_equals_datetime", []*expr.Type{DateTime, DateTime}, decls.Bool),
		decls.NewOverload("greater_equals_timestamp", []*expr.Type{Timestamp, Timestamp}, decls.Bool),
	),
	decls.NewFunction(operators.Add,
		decls.NewOverload("add_date_int", []*expr.Type{Date, decls.Int}, Date),
		decls.NewOverload("add_int_date", []*expr.Type{decls.Int, Date}, Date),
	),
	decls.NewFunction(operators.Subtract,
		decls.NewOverload("subtract_date_int", []*expr.Type{Date, decls.Int}, Date),
	),
)