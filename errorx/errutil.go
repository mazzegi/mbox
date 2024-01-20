package errorx

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

func ExitWhen(err error) {
	if err == nil {
		return
	}
	_, file, line, _ := runtime.Caller(2)
	file = filepath.Base(file)
	fmt.Fprintf(os.Stderr, "ERROR (EXIT): %v - (%s:%d)\n", err, file, line)
	os.Exit(1)
}
