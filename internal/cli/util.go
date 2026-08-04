package cli

import "strings"

// valueFlags lists the CLI flags that consume a following value. The helper
// uses this set to tell flag values apart from positional arguments. Boolean
// flags and flags written as -flag=value are not included.
var valueFlags = map[string]bool{
	"-addr":            true,
	"-age":             true,
	"-aging":           true,
	"-concurrency":     true,
	"-db":              true,
	"-idempotency-key": true,
	"-kind":            true,
	"-lease":           true,
	"-max-attempts":    true,
	"-max-events":      true,
	"-payload":         true,
	"-poll":            true,
	"-priority":        true,
	"-run-after":       true,
	"-run-at":          true,
}

// moveFirstPositionalToEnd moves positional arguments to the end of the slice.
// The Go flag package stops parsing at the first non-flag token, so without
// this a user who writes "history <id> -db path" would see -db ignored. The
// helper leaves flag values in place and only relocates true positionals, so
// the id may appear before or after the flags.
func moveFirstPositionalToEnd(args []string) []string {
	var reordered []string
	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			return args
		}
		if strings.HasPrefix(a, "-") && a != "-" {
			reordered = append(reordered, a)
			if valueFlags[a] && !strings.Contains(a, "=") && i+1 < len(args) {
				reordered = append(reordered, args[i+1])
				i++
			}
			continue
		}
		positional = append(positional, a)
	}
	if len(positional) == 0 {
		return args
	}
	return append(reordered, positional...)
}
