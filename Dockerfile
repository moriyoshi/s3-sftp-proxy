FROM golang:1.13-alpine as builder
RUN apk add --no-cache coreutils bash git openssh
WORKDIR /app
COPY ./ /app
RUN go version \
    && go build ./ \
    && chmod +x s3-sftp-proxy

FROM golang:1.13-alpine
RUN apk add --no-cache openssh \
    && addgroup -S s3sftpproxy && \
    adduser -S s3sftpproxy -G s3sftpproxy
USER s3sftpproxy:s3sftpproxy
WORKDIR /app
COPY --from=builder /app/s3-sftp-proxy /s3-sftp-proxy
ENTRYPOINT [ "/s3-sftp-proxy" ]