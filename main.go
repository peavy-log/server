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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

const (
	MAX_LINE_SIZE = 256 * 1024
)

var (
	errorCount  = 0
	lineCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "peavy_processed_lines_total",
		Help: "The total number of processed lines",
	})
	byteCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "peavy_processed_bytes_total",
		Help: "The total number of processed bytes",
	})

	promHandler = fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())
)

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

	if string(ctx.Path()) == "/healthz" {
		handleHealth(ctx)
		return
	} else if string(ctx.Path()) == "/metrics" {
		promHandler(ctx)
		return
	} else if string(ctx.Method()) != fasthttp.MethodPost {
		ctx.Response.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		fmt.Fprint(ctx, "Method Not Allowed\n")
		return
	}

	reader := ctx.Request.BodyStream()
	if string(ctx.Request.Header.ContentEncoding()) == "gzip" {
		var err error
		reader, err = gzip.NewReader(reader)
		if err != nil {
			ctx.Response.SetStatusCode(fasthttp.StatusBadRequest)
			fmt.Fprintf(ctx, "gzip error: %s", err)
			return
		}
	}

	buffered := bufio.NewScanner(reader)
	buf := make([]byte, 0, MAX_LINE_SIZE)
	buffered.Buffer(buf, MAX_LINE_SIZE)

	for buffered.Scan() {
		bytes := buffered.Bytes()
		writer(bytes)
		lineCounter.Inc()
		byteCounter.Add(float64(len(bytes)))
	}

	if closer, ok := reader.(io.Closer); ok {
		closer.Close()
	}

	ctx.Response.SetStatusCode(fasthttp.StatusCreated)
}

func writer(bytes []byte) {
	conn, err := net.Dial("tcp", "127.0.0.1:8001")
	if err != nil {
		log.Printf("error connecting to fluentbit: %s", err)
		panic(err)
	}

	_, err = conn.Write(bytes)
	if err != nil {
		log.Printf("error writing to fluentbit: %s", err)
		panic(err)
	}
}

func startFluentBit() {
	cmd := exec.Command("/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.yaml")
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
