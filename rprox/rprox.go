package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/mazzegi/log"
)

// Route ...
type Route struct {
	Prefix string
	URL    string
}

// Config ...
type Config struct {
	Bind       string
	DefaultURL string
	Routes     []Route
}

func setupProxy(prefix string, rurl string) error {
	u, err := url.Parse(rurl)
	if err != nil {
		return fmt.Errorf("parse-url %q: %v: %w", rurl, err, err)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)

	var InsecureTransport http.RoundTripper = &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		TLSHandshakeTimeout: 10 * time.Second,
	}
	proxy.Transport = InsecureTransport

	http.Handle(prefix, proxy)
	log.Infof("setup proxy: %q => %q", prefix, rurl)
	return nil
}

func createSampleConfig(file string) {
	c := Config{
		Bind:       "127.0.0.1:8080",
		DefaultURL: "http://127.0.0.1:5000",
		Routes: []Route{
			{
				Prefix: "/api/",
				URL:    "http://127.0.0.1:7000",
			},
			{
				Prefix: "/api/v2/",
				URL:    "http://127.0.0.1:7002",
			},
		},
	}
	f, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	err = toml.NewEncoder(f).Encode(c)
	if err != nil {
		panic(err)
	}
}

func main() {
	file := flag.String("f", "rprox.toml", "specify config file")
	flag.Parse()

	var cfg Config
	_, err := toml.DecodeFile(*file, &cfg)
	if err != nil {
		log.Errorf("decode config %q: %v", *file, err)
		createSampleConfig(*file)
		log.Infof("wrote sample to %q", *file)
		os.Exit(1)
	}

	err = setupProxy("/", cfg.DefaultURL)
	if err != nil {
		log.Errorf("parse-url %q: %v", cfg.DefaultURL, err)
		os.Exit(2)
	}

	for _, r := range cfg.Routes {
		err := setupProxy(r.Prefix, r.URL)
		if err != nil {
			log.Errorf("parse-url %q: %v", r.URL, err)
			os.Exit(2)
		}
	}

	err = http.ListenAndServe(cfg.Bind, nil)
	if err != nil && err != http.ErrServerClosed {
		log.Errorf("error: %q", err)
		os.Exit(2)
	}

	log.Infof("serve on %q", cfg.Bind)
}
