package hrx

import (
	"fmt"
	"strings"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

const (
	kB = 1024
	mB = 1024 * kB
	gB = 1024 * mB
)

func Size(v float64) string {
	switch {
	case v < kB:
		return fmt.Sprintf("%.0f B", v)
	case v < mB:
		return fmt.Sprintf("%.1f KB", float64(v)/float64(kB))
	case v < gB:
		return fmt.Sprintf("%.1f MB", float64(v)/float64(mB))
	default:
		return fmt.Sprintf("%.1f GB", float64(v)/float64(gB))
	}
}

func Bytes(v int64) string {
	return Size(float64(v))
}

func PrintInt(n int, thousandsSep string) string {
	s := message.NewPrinter(language.German).Sprintf("%d", n)
	return strings.ReplaceAll(s, ".", thousandsSep)
}
