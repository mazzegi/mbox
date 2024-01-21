package env

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

func loadDotenv(path string) (map[string]any, error) {
	vs := map[string]any{}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, _ := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		v = unquote(strings.TrimSpace(v))
		if v == "" {
			vs[k] = true
		} else {
			vs[k] = v
		}
	}
	return vs, nil
}

func loadDotenvToml(path string) (map[string]any, error) {
	vs := map[string]any{}
	_, err := toml.DecodeFile(path, &vs)
	if err != nil {
		return nil, err
	}
	return vs, nil
}

// LoadDotenv looks up .env and related files in the current directory and the in parent directories.
// Existing values are not overwritten by higher level files.
// Filenames considered are ".env" (usual .env files ), ".env.toml" (decoded as toml), where ".env" is evaluated before ".env.toml"
func LoadDotenv() map[string]any {
	// get working-dir as absolute path
	wd, err := os.Getwd()
	if err != nil {
		return map[string]any{}
	}
	wd, err = filepath.Abs(wd)
	if err != nil {
		return map[string]any{}
	}

	// build slices of dirs
	vol := filepath.VolumeName(wd)
	wd = strings.TrimPrefix(wd, vol)
	sep := string(filepath.Separator)
	var dirSl []string
	for _, e := range strings.Split(wd, sep) {
		if e == "" {
			continue
		}
		dirSl = append(dirSl, e)
	}

	const (
		dotEnvFile     = ".env"
		dotEnvFileToml = ".env.toml"
	)
	all := map[string]any{}
	for {
		currDir := vol + sep + filepath.Join(dirSl...)
		if vs, err := loadDotenv(filepath.Join(currDir, dotEnvFile)); err == nil {
			merge(vs, all)
		}
		if vs, err := loadDotenvToml(filepath.Join(currDir, dotEnvFileToml)); err == nil {
			merge(vs, all)
		}

		if len(dirSl) > 0 {
			dirSl = dirSl[:len(dirSl)-1]
		}

		if len(dirSl) == 0 {
			//done
			break
		}
	}
	return all
}
