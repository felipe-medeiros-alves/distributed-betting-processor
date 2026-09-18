FROM golang:1.23-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /processor ./cmd/processor
RUN CGO_ENABLED=0 go build -o /migrate ./cmd/migrate

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /processor /app/processor
COPY --from=build /migrate /app/migrate
COPY migrations /app/migrations
EXPOSE 8080
ENTRYPOINT ["/app/processor"]
