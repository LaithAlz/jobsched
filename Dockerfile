# Multi-stage build: compile with the full toolchain, ship only the binary.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Dependencies are copied and downloaded first so this layer is cached and only re-runs when
# go.mod/go.sum change — not on every source edit.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a statically linked binary with no libc dependency, so it runs on a
# minimal base image. -trimpath keeps build paths out of the binary.
RUN CGO_ENABLED=0 go build -trimpath -o /out/scheduler ./cmd/scheduler

FROM alpine:3.20

# Alpine rather than scratch: the scheduler itself needs no shell, but having one makes
# `docker exec` debugging of the compose stack possible, which is the whole point of Stage 2.
RUN adduser -D -u 10001 scheduler

COPY --from=build /out/scheduler /usr/local/bin/scheduler

EXPOSE 8080

# NOTE: docker-compose.yml overrides this to run as root, because access to the mounted Docker
# socket requires membership of the host's docker group, whose GID varies by machine. See
# NOTES.md D-044 — this stack is a local development tool, not a deployment posture.
USER scheduler

ENTRYPOINT ["/usr/local/bin/scheduler"]
