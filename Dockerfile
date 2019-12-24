FROM golang:alpine AS build

ENV GO111MODULE=on

SHELL ["/bin/sh", "-x", "-c"]

COPY ./ /go/src/s3-sftp-proxy/

WORKDIR /go/src/s3-sftp-proxy/

RUN go build


FROM alpine:3.10

COPY --from=build /go/src/s3-sftp-proxy/s3-sftp-proxy /usr/local/bin

ENTRYPOINT ["/usr/local/bin/s3-sftp-proxy"]

CMD ["--config", "/etc/s3-sftp-proxy.conf"]
