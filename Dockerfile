FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /npi-rates ./cmd/npi-rates

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /npi-rates /npi-rates
ENTRYPOINT ["/npi-rates"]
