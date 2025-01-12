package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"
	"unsafe"

	"github.com/a8m/envsubst"
	"github.com/cespare/xxhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

const (
	DEF_LINE_SIZE = 2 * 1024
	MAX_LINE_SIZE = 64 * 1024
)

type LineHash struct {
	Count uint64
	Exp   time.Time
}

var (
	bufferPool = &sync.Pool{
		New: func() any {
			buf := make([]byte, 0, DEF_LINE_SIZE)
			return &buf
		},
	}

	errorCount  = 0
	lineCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "peavy_processed_lines_total",
		Help: "The total number of processed lines",
	})
	byteCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "peavy_processed_bytes_total",
		Help: "The total number of processed bytes",
	})
	_ = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "peavy_line_limiter_size",
		Help: "The size of the line limiter map",
	}, func() float64 {
		return float64(lineLimiterMap.Size())
	})
	rejectedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "peavy_line_limiter_rejected_total",
		Help: "The total number of rejected lines by limiter",
	})

	lineLimiterMap = xsync.NewMapOf[uint64, *LineHash]()

	promHandler = fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())
)

func byteCmp(a []byte, b string) bool {
	return *(*string)(unsafe.Pointer(&a)) == b
}

func acceptLine(line []byte) bool {
	hash := xxhash.Sum64(line)
	l, existed := lineLimiterMap.LoadOrStore(hash, &LineHash{Count: 1, Exp: time.Now().Add(5 * time.Second)})
	if existed {
		if l.Count > 10 {
			rejectedCounter.Inc()
			return false
		}
		l.Count++
	}
	return true
}

func cleanLineLimiterExpiry(ticker *time.Ticker) {
	for {
		<-ticker.C

		todo := lineLimiterMap.Size() / 5
		if todo < 100 {
			continue
		}
		i := 0
		lineLimiterMap.Range(func(hash uint64, l *LineHash) bool {
			if l.Exp.Before(time.Now()) {
				lineLimiterMap.Delete(hash)
			}
			i++
			return i < todo
		})
	}
}

func handleHealth(ctx *fasthttp.RequestCtx) {
	if errorCount > 3 {
		ctx.Response.SetStatusCode(fasthttp.StatusInternalServerError)
		fmt.Fprintf(ctx, "ERROR %d\n", errorCount)
	} else {
		ctx.Response.SetStatusCode(fasthttp.StatusOK)
		fmt.Fprint(ctx, "OK\n")
	}
	errorCount = 0
}

func handler(ctx *fasthttp.RequestCtx) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic: %v", r)
			errorCount++
			ctx.Response.SetStatusCode(fasthttp.StatusInternalServerError)
			fmt.Fprint(ctx, "Internal Server Error\n")
		}
	}()

	if byteCmp(ctx.Path(), "/healthz") {
		handleHealth(ctx)
		return
	} else if byteCmp(ctx.Path(), "/metrics") {
		promHandler(ctx)
		return
	} else if !byteCmp(ctx.Method(), fasthttp.MethodPost) {
		ctx.Response.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		fmt.Fprint(ctx, "Method Not Allowed\n")
		return
	}

	reader := ctx.Request.BodyStream()
	if byteCmp(ctx.Request.Header.ContentEncoding(), "gzip") {
		var err error
		reader, err = gzip.NewReader(reader)
		if err != nil {
			ctx.Response.SetStatusCode(fasthttp.StatusBadRequest)
			fmt.Fprintf(ctx, "gzip error: %s", err)
			return
		}
	}

	buffered := bufio.NewScanner(reader)
	buf := *bufferPool.Get().(*[]byte)
	buffered.Buffer(buf, MAX_LINE_SIZE)

	conn, err := net.Dial("tcp", "127.0.0.1:8001")
	if err != nil {
		log.Printf("error connecting to fluentbit: %s", err)
		panic(err)
	}
	defer conn.Close()

	for buffered.Scan() {
		bytes := buffered.Bytes()

		// Simple limiter that prevents the exact same line
		// from being processed too many times, usually as
		// a result of a misconfigured client.
		if !acceptLine(bytes) {
			continue
		}

		_, err = conn.Write(bytes)
		if err != nil {
			log.Printf("error writing to fluentbit: %s", err)
			panic(err)
		}

		lineCounter.Inc()
		byteCounter.Add(float64(len(bytes)))
	}
	bufferPool.Put(&buf)

	if closer, ok := reader.(io.Closer); ok {
		closer.Close()
	}

	ctx.Response.SetStatusCode(fasthttp.StatusCreated)
}

func startFluentBit() {
	conf, err := envsubst.ReadFile("/fluent-bit/etc/fluent-bit.conf")
	if err != nil {
		log.Fatalf("error reading fluent-bit.conf: %s", err)
	}
	err = os.WriteFile("/fluent-bit/etc/fluent-bit.conf", conf, 0644)
	if err != nil {
		log.Fatalf("error writing fluent-bit.conf: %s", err)
	}

	cmd := exec.Command("/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("error starting fluent-bit: %s", err)
	}
	if err := cmd.Wait(); err != nil {
		log.Fatalf("fluent-bit error: %s", err)
	}
}

func main() {
	go startFluentBit()
	go cleanLineLimiterExpiry(time.NewTicker(1 * time.Second))

	server := &fasthttp.Server{
		Handler:           handler,
		Name:              "peavy",
		StreamRequestBody: true,
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Println("peavy listening, port " + port)

	if err := server.ListenAndServe(":" + port); err != nil {
		log.Fatalf("error starting: %s", err)
	}
}
