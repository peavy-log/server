# Peavy Server

FluentBit with a thin proxy in front that translates
gzipped application/ndjson requests into individual log lines
and forwards it to the tcp listener in FluentBit.

Default FluentBit configuration includes a lua filter
that converts the log lines from peavy request format
into a suitable Google Cloud Logging json format.

The lua filter also parses and transforms the iso8601 timestamp.

## Usage

```bash
docker run -p 8000:8000 ghcr.io/peavy-log/server
```