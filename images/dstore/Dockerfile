# The dstore node image: the dstore binary plus the operator's entrypoint.
ARG DSTORE_VERSION=main
FROM golang:1.26-alpine AS build
ARG DSTORE_VERSION
RUN apk add --no-cache git
ENV CGO_ENABLED=0
RUN go install github.com/amber-store/dstore/cmd/dstore@${DSTORE_VERSION}

FROM alpine:3.20
LABEL org.opencontainers.image.source=https://github.com/amber-store/dstore-operator
RUN apk add --no-cache ca-certificates
COPY --from=build /go/bin/dstore /usr/local/bin/dstore
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
VOLUME ["/data"]
EXPOSE 4433/udp
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
