FROM golang:1.26-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends libopus-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -tags nolibopusfile -o /gateway .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libopus0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /gateway /usr/local/bin/gateway
EXPOSE 8788/tcp 7881/udp
ENTRYPOINT ["gateway"]
