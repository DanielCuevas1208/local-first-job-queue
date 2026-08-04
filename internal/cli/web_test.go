package cli

import (
	"strings"
	"testing"
)

// TestDashboardURL verifies the helper that turns a listen address into a
// clickable URL. A bare port becomes localhost so the printed link always
// works in a browser.
func TestDashboardURL(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{":8080", "http://localhost:8080"},
		{"127.0.0.1:9090", "http://127.0.0.1:9090"},
		{"0.0.0.0:1234", "http://localhost:1234"},
		{"[::1]:8080", "http://[::1]:8080"},
	}
	for _, tc := range cases {
		if got := dashboardURL(tc.in); got != tc.want {
			t.Errorf("dashboardURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestWebRequiresBothCredentials verifies that -user and -pass must be supplied
// together. Setting only one is a configuration error, not a silent one.
func TestWebRequiresBothCredentials(t *testing.T) {
	cases := [][]string{
		{"-user", "alice"},
		{"-pass", "secret"},
	}
	for _, args := range cases {
		err := Web(args)
		if err == nil {
			t.Fatalf("Web(%v) should fail with one credential missing", args)
		}
		if !strings.Contains(err.Error(), "must be set together") {
			t.Errorf("Web(%v) error = %q, want the pairing message", args, err)
		}
	}
}
