FROM golang:latest AS builder

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update; apt-get -y install ca-certificates upx

COPY . /go/src/github.com/fioprotocol/fio.etl
WORKDIR /go/src/github.com/fioprotocol/fio.etl

RUN go build -ldflags "-s -w" -o /fioetl cmd/fioetl/main.go && upx -9 /fioetl

FROM scratch

COPY --from=builder /fioetl /fioetl
COPY auth.yml /

# this gets libssl1.1, openssl, and libc6 which we need for TLS, not tiny, but saves about 1 GiB in final image size
COPY --from=builder /etc/ca-certificates /etc/ca-certificates
COPY --from=builder /etc/ssl /etc/ssl
COPY --from=builder /usr/share/ca-certificates /usr/share/ca-certificates
COPY --from=builder /usr/lib /usr/lib
COPY --from=builder /lib /lib
COPY --from=builder /lib64 /lib64

USER 65535
CMD ["/fioetl"]

