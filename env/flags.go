package env

import (
	"strings"
)

// ParseFlags parses (commandline) flags and returns them  as key/value pairs.
// The following forms are permitted:
// -flag     => just a boolean flag
// --flag    => double dashes are also permitted
// -flag=x   => single dash
// -flag x   => single dash, no equal
func ParseFlags(args []string) map[string]any {
	fs := map[string]any{}

	flag := func(s string) (string, bool) {
		switch {
		case strings.HasPrefix(s, "--"):
			return strings.TrimPrefix(s, "--"), true
		case strings.HasPrefix(s, "-"):
			return strings.TrimPrefix(s, "-"), true
		default:
			return "", false
		}
	}

	var currName string

	for _, arg := range args {
		flag, ok := flag(arg)
		if ok {
			if currName != "" {
				// prev is a bool flag
				fs[currName] = true
				currName = ""
			}

			//a new name
			//see if there's a "="
			if name, val, ok := strings.Cut(flag, "="); ok {
				fs[name] = val
			} else {
				currName = flag
			}
		} else {
			// a value
			if currName != "" {
				fs[currName] = arg
				currName = ""
			}
			//else just an arg
		}
	}
	if currName != "" {
		fs[currName] = true
	}
	return fs
}
