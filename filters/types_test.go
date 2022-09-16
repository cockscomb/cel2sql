package filters

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinRegexps(t *testing.T) {
	cases := []struct {
		patterns    []string
		escapeItems bool
		want        string
	}{
		{
			patterns:    []string{"foo", "bar"},
			escapeItems: false,
			want:        `(foo)|(bar)`,
		},
		{
			patterns:    []string{"foo"},
			escapeItems: false,
			want:        `foo`,
		},
		{
			patterns:    []string{"foo"},
			escapeItems: true,
			want:        `foo`,
		},
		{
			patterns:    []string{"[fo]o"},
			escapeItems: false,
			want:        `[fo]o`,
		},
		{
			patterns:    []string{"[fo]o"},
			escapeItems: true,
			want:        `\[fo\]o`,
		},
		{
			patterns:    []string{"foo", "bar"},
			escapeItems: true,
			want:        `foo|bar`,
		},
		{
			patterns:    []string{"f|oo", "bar"},
			escapeItems: true,
			want:        `f\|oo|bar`,
		},
		{
			patterns:    []string{"f|oo", "b(a)r"},
			escapeItems: true,
			want:        `f\|oo|b\(a\)r`,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.want, func(t *testing.T) {
			got := joinRegexps(tc.patterns, tc.escapeItems)
			require.Equal(t, tc.want, got)
		})
	}
}
