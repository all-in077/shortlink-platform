FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o notification-service ./cmd

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/notification-service .
CMD ["./notification-service"]