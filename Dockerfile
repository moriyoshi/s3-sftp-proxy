FROM golang:alpine AS build

SHELL ["/bin/sh", "-x", "-c"]

COPY ./ /go/src/s3-sftp-proxy/

RUN install -d /go/bin \
    && wget https://raw.githubusercontent.com/golang/dep/master/install.sh -O /tmp/dep-install.sh \
    && sh /tmp/dep-install.sh

WORKDIR /go/src/s3-sftp-proxy/

RUN dep ensure

RUN go build


FROM alpine:3.10

COPY --from=build /go/src/s3-sftp-proxy/s3-sftp-proxy /usr/local/bin

ENTRYPOINT ["/usr/local/bin/s3-sftp-proxy"]

CMD ["--config", "/etc/s3-sftp-proxy.conf"]
