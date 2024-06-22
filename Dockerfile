FROM golang:1.22 AS builder

WORKDIR /src
COPY main.go go.mod go.sum ./
RUN GOOS=linux CGO_ENABLED=0 go build -ldflags "-s -w" -o ndproxy

FROM fluent/fluent-bit:latest

COPY --from=builder /src/ndproxy /
COPY fluentbit/ /fluent-bit/etc/
ENTRYPOINT [ "/ndproxy" ]