package cel2sql_test

import (
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cockscomb/cel2sql"
	"github.com/cockscomb/cel2sql/bq"
	"github.com/cockscomb/cel2sql/filters"
	"github.com/cockscomb/cel2sql/sqltypes"
	"github.com/cockscomb/cel2sql/test"
)

func TestConvert(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeProvider(bq.NewTypeProvider(map[string]bigquery.Schema{
			"trigrams":  test.NewTrigramsTableMetadata().Schema,
			"wikipedia": test.NewWikipediaTableMetadata().Schema,
		})),
		sqltypes.SQLTypeDeclarations,
		cel.Declarations(
			decls.NewVar("name", decls.String),
			decls.NewVar("age", decls.Int),
			decls.NewVar("adult", decls.Bool),
			decls.NewVar("height", decls.Double),
			decls.NewVar("string_list", decls.NewListType(decls.String)),
			decls.NewVar("string_int_map", decls.NewMapType(decls.String, decls.Int)),
			decls.NewVar("null_var", decls.Null),
			decls.NewVar("birthday", sqltypes.Date),
			decls.NewVar("fixed_time", sqltypes.Time),
			decls.NewVar("scheduled_at", sqltypes.DateTime),
			decls.NewVar("created_at", decls.Timestamp),
			decls.NewVar("trigram", decls.NewObjectType("trigrams")),
			decls.NewVar("page", decls.NewObjectType("wikipedia")),
		),
		filters.Declarations,
	)
	require.NoError(t, err)
	type args struct {
		source string
	}
	tests := []struct {
		name           string
		args           args
		want           string
		wantCompileErr bool
		wantErr        bool
	}{
		{
			name:    "startsWith",
			args:    args{source: `name.startsWith("a")`},
			want:    "STARTS_WITH(`name`, \"a\")",
			wantErr: false,
		},
		{
			name:    "endsWith",
			args:    args{source: `name.endsWith("z")`},
			want:    "ENDS_WITH(`name`, \"z\")",
			wantErr: false,
		},
		{
			name:    "matches",
			args:    args{source: `name.matches("a+")`},
			want:    "REGEXP_CONTAINS(`name`, \"a+\")",
			wantErr: false,
		},
		{
			name:    "contains",
			args:    args{source: `name.contains("abc")`},
			want:    "INSTR(`name`, \"abc\") != 0",
			wantErr: false,
		},
		{
			name:    "&&",
			args:    args{source: `name.startsWith("a") && name.endsWith("z")`},
			want:    "STARTS_WITH(`name`, \"a\") AND ENDS_WITH(`name`, \"z\")",
			wantErr: false,
		},
		{
			name:    "||",
			args:    args{source: `name.startsWith("a") || name.endsWith("z")`},
			want:    "STARTS_WITH(`name`, \"a\") OR ENDS_WITH(`name`, \"z\")",
			wantErr: false,
		},
		{
			name:    "()",
			args:    args{source: `age >= 10 && (name.startsWith("a") || name.endsWith("z"))`},
			want:    "`age` >= 10 AND (STARTS_WITH(`name`, \"a\") OR ENDS_WITH(`name`, \"z\"))",
			wantErr: false,
		},
		{
			name:    "IF",
			args:    args{source: `name == "a" ? "a" : "b"`},
			want:    "IF(`name` = \"a\", \"a\", \"b\")",
			wantErr: false,
		},
		{
			name:    "==",
			args:    args{source: `name == "a"`},
			want:    "`name` = \"a\"",
			wantErr: false,
		},
		{
			name:    "!=",
			args:    args{source: `age != 20`},
			want:    "`age` != 20",
			wantErr: false,
		},
		{
			name:    "IS NULL",
			args:    args{source: `null_var == null`},
			want:    "`null_var` IS NULL",
			wantErr: false,
		},
		{
			name:    "IS NOT TRUE",
			args:    args{source: `adult != true`},
			want:    "`adult` IS NOT TRUE",
			wantErr: false,
		},
		{
			name:    "<",
			args:    args{source: `age < 20`},
			want:    "`age` < 20",
			wantErr: false,
		},
		{
			name:    ">=",
			args:    args{source: `height >= 1.6180339887`},
			want:    "`height` >= 1.6180339887",
			wantErr: false,
		},
		{
			name:    "NOT",
			args:    args{source: `!adult`},
			want:    "NOT `adult`",
			wantErr: false,
		},
		{
			name:    "-",
			args:    args{source: `-1`},
			want:    "-1",
			wantErr: false,
		},
		{
			name:    "list",
			args:    args{source: `[1, 2, 3][0] == 1`},
			want:    "[1, 2, 3][OFFSET(0)] = 1",
			wantErr: false,
		},
		{
			name:    "list_var",
			args:    args{source: `string_list[0] == "a"`},
			want:    "`string_list`[OFFSET(0)] = \"a\"",
			wantErr: false,
		},
		{
			name:    "map",
			args:    args{source: `{"one": 1, "two": 2, "three": 3}["one"] == 1`},
			want:    "STRUCT(1 AS one, 2 AS two, 3 AS three).`one` = 1",
			wantErr: false,
		},
		{
			name:    "map_var",
			args:    args{source: `string_int_map["one"] == 1`},
			want:    "`string_int_map`.`one` = 1",
			wantErr: false,
		},
		{
			name:    "invalidFieldType",
			args:    args{source: `{1: 1}[1]`},
			want:    "",
			wantErr: true,
		},
		{
			name:    "invalidFieldName",
			args:    args{source: `{"on e": 1}["on e"]`},
			want:    "",
			wantErr: true,
		},
		{
			name:    "add",
			args:    args{source: `1 + 2 == 3`},
			want:    "1 + 2 = 3",
			wantErr: false,
		},
		{
			name:    "concatString",
			args:    args{source: `"a" + "b" == "ab"`},
			want:    "\"a\" || \"b\" = \"ab\"",
			wantErr: false,
		},
		{
			name:    "concatList",
			args:    args{source: `1 in [1] + [2, 3]`},
			want:    "1 IN UNNEST([1] || [2, 3])",
			wantErr: false,
		},
		{
			name:    "modulo",
			args:    args{source: `5 % 3 == 2`},
			want:    "MOD(5, 3) = 2",
			wantErr: false,
		},
		{
			name:    "date",
			args:    args{source: `birthday > date(2000, 1, 1) + 1`},
			want:    "`birthday` > DATE(2000, 1, 1) + 1",
			wantErr: false,
		},
		{
			name:    "time",
			args:    args{source: `fixed_time == time("18:00:00")`},
			want:    "`fixed_time` = TIME(\"18:00:00\")",
			wantErr: false,
		},
		{
			name:    "datetime",
			args:    args{source: `scheduled_at != datetime(date("2021-09-01"), fixed_time)`},
			want:    "`scheduled_at` != DATETIME(DATE(\"2021-09-01\"), `fixed_time`)",
			wantErr: false,
		},
		{
			name:    "timestamp",
			args:    args{source: `created_at - duration("60m") <= timestamp(datetime("2021-09-01 18:00:00"), "Asia/Tokyo")`},
			want:    "TIMESTAMP_SUB(`created_at`, INTERVAL 1 HOUR) <= TIMESTAMP(DATETIME(\"2021-09-01 18:00:00\"), \"Asia/Tokyo\")",
			wantErr: false,
		},
		{
			name:    "duration_second",
			args:    args{source: `duration("10s")`},
			want:    "INTERVAL 10 SECOND",
			wantErr: false,
		},
		{
			name:    "duration_minute",
			args:    args{source: `duration("1h1m")`},
			want:    "INTERVAL 61 MINUTE",
			wantErr: false,
		},
		{
			name:    "duration_hour",
			args:    args{source: `duration("60m")`},
			want:    "INTERVAL 1 HOUR",
			wantErr: false,
		},
		{
			name:    "interval",
			args:    args{source: `interval(1, MONTH)`},
			want:    "INTERVAL 1 MONTH",
			wantErr: false,
		},
		{
			name:    "date_add",
			args:    args{source: `date("2021-09-01") + interval(1, DAY)`},
			want:    "DATE_ADD(DATE(\"2021-09-01\"), INTERVAL 1 DAY)",
			wantErr: false,
		},
		{
			name:    "date_sub",
			args:    args{source: `current_date() - interval(1, DAY)`},
			want:    "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
			wantErr: false,
		},
		{
			name:    "time_add",
			args:    args{source: `time("09:00:00") + interval(1, MINUTE)`},
			want:    "TIME_ADD(TIME(\"09:00:00\"), INTERVAL 1 MINUTE)",
			wantErr: false,
		},
		{
			name:    "time_sub",
			args:    args{source: `time("09:00:00") - interval(1, MINUTE)`},
			want:    "TIME_SUB(TIME(\"09:00:00\"), INTERVAL 1 MINUTE)",
			wantErr: false,
		},
		{
			name:    "datetime_add",
			args:    args{source: `datetime("2021-09-01 18:00:00") + interval(1, MINUTE)`},
			want:    "DATETIME_ADD(DATETIME(\"2021-09-01 18:00:00\"), INTERVAL 1 MINUTE)",
			wantErr: false,
		},
		{
			name:    "datetime_sub",
			args:    args{source: `current_datetime("Asia/Tokyo") - interval(1, MINUTE)`},
			want:    "DATETIME_SUB(CURRENT_DATETIME(\"Asia/Tokyo\"), INTERVAL 1 MINUTE)",
			wantErr: false,
		},
		{
			name:    "timestamp_add",
			args:    args{source: `duration("1h") + timestamp("2021-09-01T18:00:00Z")`},
			want:    "TIMESTAMP_ADD(TIMESTAMP(\"2021-09-01T18:00:00Z\"), INTERVAL 1 HOUR)",
			wantErr: false,
		},
		{
			name:    "timestamp_sub",
			args:    args{source: `created_at - interval(1, HOUR)`},
			want:    "TIMESTAMP_SUB(`created_at`, INTERVAL 1 HOUR)",
			wantErr: false,
		},
		{
			name:    "timestamp_getSeconds",
			args:    args{source: `created_at.getSeconds()`},
			want:    "EXTRACT(SECOND FROM `created_at`)",
			wantErr: false,
		},
		{
			name:    "\"timestamp_getHours_withTimezone",
			args:    args{source: `created_at.getHours("Asia/Tokyo")`},
			want:    "EXTRACT(HOUR FROM `created_at` AT \"Asia/Tokyo\")",
			wantErr: false,
		},
		{
			name:    "date_getFullYear",
			args:    args{source: `birthday.getFullYear()`},
			want:    "EXTRACT(YEAR FROM `birthday`)",
			wantErr: false,
		},
		{
			name:    "datetime_getMonth",
			args:    args{source: `scheduled_at.getMonth()`},
			want:    "EXTRACT(MONTH FROM `scheduled_at`) - 1",
			wantErr: false,
		},
		{
			name:    "datetime_getDayOfMonth",
			args:    args{source: `scheduled_at.getDayOfMonth()`},
			want:    "EXTRACT(DAY FROM `scheduled_at`) - 1",
			wantErr: false,
		},
		{
			name:    "time_getMinutes",
			args:    args{source: `fixed_time.getMinutes()`},
			want:    "EXTRACT(MINUTE FROM `fixed_time`)",
			wantErr: false,
		},
		{
			name:    "fieldSelect",
			args:    args{source: `page.title == "test"`},
			want:    "`page`.`title` = \"test\"",
			wantErr: false,
		},
		{
			name:    "fieldSelect_startsWith",
			args:    args{source: `page.title.startsWith("test")`},
			want:    "STARTS_WITH(`page`.`title`, \"test\")",
			wantErr: false,
		},
		{
			name:    "fieldSelect_add",
			args:    args{source: `trigram.cell[0].page_count + 1`},
			want:    "`trigram`.`cell`[OFFSET(0)].`page_count` + 1",
			wantErr: false,
		},
		{
			name:    "fieldSelect_concatString",
			args:    args{source: `trigram.cell[0].sample[0].title + "test"`},
			want:    "`trigram`.`cell`[OFFSET(0)].`sample`[OFFSET(0)].`title` || \"test\"",
			wantErr: false,
		},
		{
			name:    "fieldSelect_in",
			args:    args{source: `"test" in trigram.cell[0].value`},
			want:    "\"test\" IN UNNEST(`trigram`.`cell`[OFFSET(0)].`value`)",
			wantErr: false,
		},
		{
			name:    "cast_bool",
			args:    args{source: `bool(0) == false`},
			want:    "CAST(0 AS BOOL) IS FALSE",
			wantErr: false,
		},
		{
			name:    "cast_bytes",
			args:    args{source: `bytes("test")`},
			want:    "CAST(\"test\" AS BYTES)",
			wantErr: false,
		},
		{
			name:    "cast_int",
			args:    args{source: `int(true) == 1`},
			want:    "CAST(TRUE AS INT64) = 1",
			wantErr: false,
		},
		{
			name:    "cast_string",
			args:    args{source: `string(true) == "true"`},
			want:    "CAST(TRUE AS STRING) = \"true\"",
			wantErr: false,
		},
		{
			name:    "cast_string_from_timestamp",
			args:    args{source: `string(created_at)`},
			want:    "CAST(`created_at` AS STRING)",
			wantErr: false,
		},
		{
			name:    "cast_int_epoch",
			args:    args{source: `int(created_at)`},
			want:    "UNIX_SECONDS(`created_at`)",
			wantErr: false,
		},
		{
			name:    "size_string",
			args:    args{source: `size("test")`},
			want:    "LENGTH(\"test\")",
			wantErr: false,
		},
		{
			name:    "size_bytes",
			args:    args{source: `size(bytes("test"))`},
			want:    "LENGTH(CAST(\"test\" AS BYTES))",
			wantErr: false,
		},
		{
			name:    "size_list",
			args:    args{source: `size(string_list)`},
			want:    "ARRAY_LENGTH(`string_list`)",
			wantErr: false,
		},
		{
			name:    "inplace_array_exists",
			args:    args{source: `["foo", "bar"].exists(x, x == "foo")`},
			want:    "EXISTS (SELECT * FROM UNNEST([\"foo\", \"bar\"]) AS x WHERE `x` = \"foo\")",
			wantErr: false,
		},
		{
			name: "filters_exists_equals",
			args: args{source: `"foo".existsEquals("bar") && "foo".existsEquals(["bar"]) && ["foo"].existsEquals("bar") && ["foo"].existsEquals(["bar"])`},
			want: `"foo" = "bar" AND "foo" = "bar" AND "bar" IN UNNEST(["foo"]) AND "bar" IN UNNEST(["foo"])`,
		},
		{
			name: "filters_exists_equals_ci",
			args: args{source: `"foo".existsEqualsCI("bar") && "foo".existsEqualsCI(["bar"]) && ["foo"].existsEqualsCI("bar") && ["foo"].existsEqualsCI(["bar"])`},
			want: `COLLATE("foo", "und:ci") = "bar" AND COLLATE("foo", "und:ci") = "bar" AND COLLATE("bar", "und:ci") IN UNNEST(["foo"]) AND COLLATE("bar", "und:ci") IN UNNEST(["foo"])`,
		},
		{
			name:    "filters_exists_regexp",
			args:    args{source: `"foo".existsRegexp("bar") && "foo".existsRegexp(["bar"]) && ["foo"].existsRegexp("bar") && ["foo"].existsRegexp(["bar"])`},
			want:    "REGEXP_CONTAINS(\"foo\", \"^(bar)$\") AND REGEXP_CONTAINS(\"foo\", \"^(bar)$\") AND REGEXP_CONTAINS(\"\\x00\" || ARRAY_TO_STRING([\"foo\"], \"\\x00\") || \"\\x00\", \"\\x00((bar))\\x00\") AND REGEXP_CONTAINS(\"\\x00\" || ARRAY_TO_STRING([\"foo\"], \"\\x00\") || \"\\x00\", \"\\x00((bar))\\x00\")",
			wantErr: false,
		},
		{
			name:    "filters_exists_regexp_ci",
			args:    args{source: `"foo".existsRegexpCI("bar") && "foo".existsRegexpCI(["bar"]) && ["foo"].existsRegexpCI("bar") && ["foo"].existsRegexpCI(["bar"])`},
			want:    `REGEXP_CONTAINS("foo", "(?i)^(bar)$") AND REGEXP_CONTAINS("foo", "(?i)^(bar)$") AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["foo"], "\x00") || "\x00", "(?i)\x00((bar))\x00") AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["foo"], "\x00") || "\x00", "(?i)\x00((bar))\x00")`,
			wantErr: false,
		},
		{
			name:           "filters_no_args",
			args:           args{source: `"foo".existsEquals() && "foo".existsStartsCI() && ["foo"].existsEnds() && ["foo"].existsContainsCI() && "foo".existsRegexp()`},
			wantCompileErr: true,
		},
		{
			name: "filters_empty_array_args",
			args: args{source: `"foo".existsEqualsCI([]) && "foo".existsStarts([]) && ["foo"].existsEndsCI([]) && ["foo"].existsContains([]) && "foo".existsRegexpCI([])`},
			want: "FALSE AND FALSE AND FALSE AND FALSE AND FALSE",
		},
	}

	tracker := bq.NewBigQueryNamedTracker()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, issues := env.Compile(tt.args.source)
			if tt.wantCompileErr {
				require.NotEmpty(t, issues)
				return
			}
			require.Empty(t, issues)

			got, err := cel2sql.Convert(ast, cel2sql.WithExtension(&filters.Extension{}))
			if !tt.wantErr && assert.NoError(t, err) {
				assert.Equal(t, tt.want, got)
			} else {
				assert.Error(t, err)
			}

			t.Run("WithValueTracker", func(t *testing.T) {
				got, err := cel2sql.Convert(ast, cel2sql.WithValueTracker(tracker), cel2sql.WithExtension(&filters.Extension{}))
				for _, v := range tracker.Values {
					got = strings.ReplaceAll(got, "@"+v.Name, cel2sql.ValueToString(v.Value))
				}
				if !tt.wantErr && assert.NoError(t, err) {
					assert.Equal(t, tt.want, got)
				} else {
					assert.Error(t, err)
				}
			})
		})
	}
}
