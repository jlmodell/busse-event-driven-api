# first (build) stage
FROM golang:1.18 as builder
WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 go build -v -o app .

# final (target) stage
FROM alpine:3.10
WORKDIR /root/
COPY --from=builder /app ./
CMD ["./app"]