package cli

import "strings"

// moveFirstPositionalToEnd moves the first non-flag argument to the end of the
// slice. The Go flag package stops parsing at the first non-flag token, so
// without this a user who writes "history <id> -db path" would see -db ignored.
func moveFirstPositionalToEnd(args []string) []string {
	for i, a := range args {
		if a == "--" {
			return args
		}
		if strings.HasPrefix(a, "-") {
			continue
		}
		// Found the first positional token. Push it to the end.
		return append(append(append([]string{}, args[:i]...), args[i+1:]...), a)
	}
	return args
}
