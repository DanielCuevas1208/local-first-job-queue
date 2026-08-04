package cli

import "testing"

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
