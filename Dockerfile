FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /manager ./cmd/manager

FROM gcr.io/distroless/static:nonroot
LABEL org.opencontainers.image.source=https://github.com/amber-store/dstore-operator
COPY --from=build /manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
