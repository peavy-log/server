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

	"github.com/valyala/fasthttp"
)

const (
	MAX_LINE_SIZE = 256 * 1024
)

var errorCount = 0

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
	}

	reader := ctx.Request.BodyStream()
	var err error
	if string(ctx.Request.Header.ContentEncoding()) == "gzip" {
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
		writer(buffered.Bytes())
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
	err := cmd.Start()
	if err != nil {
		log.Fatalf("error starting fluent-bit: %s", err)
	}
	err = cmd.Wait()
	if err != nil {
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
	err := server.ListenAndServe(":" + port)
	if err != nil {
		log.Fatalf("error starting: %s", err)
	}
}
