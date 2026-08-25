FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /castai-workload-resize-migrator ./cmd/castai-workload-resize-migrator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /castai-workload-resize-migrator /castai-workload-resize-migrator
USER 65532:65532
ENTRYPOINT ["/castai-workload-resize-migrator"]
