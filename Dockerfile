FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /woop-rebalance-controller ./cmd/woop-rebalance-controller

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /woop-rebalance-controller /woop-rebalance-controller
USER 65532:65532
ENTRYPOINT ["/woop-rebalance-controller"]
