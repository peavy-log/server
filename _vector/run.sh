#!/bin/bash

script_path=$(dirname "$(readlink -f "$0")")

docker run --rm -it \
    -v "$script_path"/vector.yaml:/etc/vector/vector.yaml:ro \
    -p 8686:8686 -p 8000:8000 \
    --name vector \
    timberio/vector:latest-distroless-static
