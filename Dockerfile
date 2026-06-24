# Pure-Go build (modernc.org/sqlite — no CGO, FTS5 compiled in). Multi-arch via
# the platform args the builder injects; arcd builds linux/amd64 for hanzo-k8s.
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags='-s -w' -o /out/search-fts5 ./cmd/search-fts5

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/search-fts5 /usr/local/bin/search-fts5
# Data dir is a mounted PVC in k8s; default matches the CR.
ENV FTS5_DB_PATH=/data/search.db FTS5_ADDR=:7700
EXPOSE 7700
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/search-fts5"]
