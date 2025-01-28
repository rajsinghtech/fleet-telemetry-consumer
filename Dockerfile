FROM golang:1.23 AS builder

WORKDIR /app

# Install build dependencies including librdkafka
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application with CGO enabled
RUN CGO_ENABLED=1 GOOS=linux go build -o main .

# Final stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependency
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/main .
COPY views/ views/

# Run as non-root user
RUN useradd -r appuser
USER appuser

EXPOSE 3000 

CMD ["./main"] 