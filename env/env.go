// Package env provides a uniform way of dealing with environment such as .env files, os.Environ and (command line) flags.
// The goal is, that applications don't have to care about the source from a variable but just handle the values.
package env

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// merge merges "from" env into "to" env, keeping already existing values
func merge(from map[string]any, to map[string]any) {
	for k, v := range from {
		if _, ok := to[k]; !ok {
			to[k] = v
		}
	}
}

func unquote(s string) string {
	if (strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)) ||
		(strings.HasPrefix(s, `'`) && strings.HasSuffix(s, `'`)) {
		return s[1 : len(s)-1]
	}
	return s
}

type Var struct {
	Key   string
	Value any
}

func MkVar(k string, v any) Var {
	return Var{Key: k, Value: v}
}

type Env map[string]any

func (env Env) add(k string, v any) {
	k = strings.TrimSpace(k)
	if k == "" {
		return
	}
	env[k] = v
}

// Load loads environment variables from all available sources. It takes additional vars, which may be passed to the environment at runtime.
func Load(vars ...Var) Env {
	env := Env{}
	for _, osev := range os.Environ() {
		k, v, _ := strings.Cut(osev, "=")
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		v = unquote(strings.TrimSpace(v))
		if v == "" {
			env.add(k, true)
		} else {
			env.add(k, v)
		}
	}
	denv := LoadDotenv()
	for k, v := range denv {
		env.add(k, v)
	}
	flags := ParseFlags(os.Args)
	for k, v := range flags {
		env.add(k, v)
	}
	for _, v := range vars {
		env.add(v.Key, v.Value)
	}

	// expand all values to allow for "inline" string vars
	repl := env.Expander()
	for k, v := range env {
		if s, ok := v.(string); ok {
			env[k] = repl.Replace(s)
		}
	}
	return env
}

func (env Env) Expander() *strings.Replacer {
	var oldnew []string
	for k := range env {
		new, ok := env.String(k)
		if !ok {
			continue
		}
		//new = escape(new)
		oldnew = append(oldnew, fmt.Sprintf("{%s}", k), new)
	}
	repl := strings.NewReplacer(oldnew...)
	return repl
}

// Var returns the value for the passed key if exists, otherwise, false
func (env Env) Var(key string) (any, bool) {
	v, ok := env[key]
	if !ok {
		return nil, false
	}
	return v, true
}

// String returns the string-value for the passed key if exists, otherwise, false
func (env Env) String(key string) (string, bool) {
	s, ok := env[key]
	if !ok {
		return "", false
	}
	return fmt.Sprintf("%v", s), true
}

// Int returns the int-value for the passed key if exists, otherwise, false
func (env Env) Int(key string) (int, bool) {
	s, ok := env.String(key)
	if !ok {
		return 0, false
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return int(n), true
	}

	return 0, false
}

// StringOrDefault first tries to lookup the passed key, otherwise return def
func (env Env) StringOrDefault(key string, def string) string {
	if v, ok := env.String(key); ok {
		return v
	}
	return def
}

// IntOrDefault first tries to lookup the passed key, otherwise return def
func (env Env) IntOrDefault(key string, def int) int {
	if v, ok := env.Int(key); ok {
		return v
	}
	return def
}

// StringWithTag first to lookup "key.tag", otherwise return Env.String
func (env Env) StringWithTag(key string, tag string) (string, bool) {
	tagProbe := fmt.Sprintf("%s.%s", key, tag)
	if tagVal, ok := env.String(tagProbe); ok {
		return tagVal, true
	}
	return env.String(key)
}

// StringWithTagOrDefault first tries to lookup "key.tag", otherwise return Env.StringOrDefault
func (env Env) StringWithTagOrDefault(key string, tag string, def string) string {
	tagProbe := fmt.Sprintf("%s.%s", key, tag)
	if tagVal, ok := env.String(tagProbe); ok {
		return tagVal
	}
	return env.StringOrDefault(key, def)
}
