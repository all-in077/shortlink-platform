FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o auth-service ./cmd

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/auth-service .
EXPOSE 8080 50051 9090
CMD ["./auth-service"]