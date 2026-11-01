package httpx

import (
	"net/http"
	"time"

	"github.com/mazzegi/log"
)

type statusWriter struct {
	w           http.ResponseWriter
	statusCode  int
	codeWritten bool
}

func NewStatusWriter(w http.ResponseWriter) *statusWriter {
	return &statusWriter{
		w: w,
	}
}

func (sw *statusWriter) Header() http.Header {
	return sw.w.Header()
}

func (sw *statusWriter) Write(p []byte) (n int, err error) {
	return sw.w.Write(p)
}

func (sw *statusWriter) Code() int {
	return sw.statusCode
}

func (sw *statusWriter) WriteHeader(code int) {
	if sw.codeWritten {
		return
	}
	sw.statusCode = code
	sw.w.WriteHeader(code)
	sw.codeWritten = true
}

func Logger() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sw := NewStatusWriter(w)
			t0 := time.Now()
			next.ServeHTTP(sw, r)
			log.Infof("ACCESS: %s host=%q path=%q query=%q => status %d (%s) in %s", r.Method, r.Host, r.URL.Path, r.URL.RawQuery, sw.statusCode, http.StatusText(sw.statusCode), time.Since(t0))
		})
	}
}
