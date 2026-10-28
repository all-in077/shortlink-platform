FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o gateway-service ./cmd

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/gateway-service .
EXPOSE 8082
CMD ["./gateway-service"]