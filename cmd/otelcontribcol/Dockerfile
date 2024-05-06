FROM golang:1.20 AS build

WORKDIR /src
ADD . /src

RUN cd cmd/otelcontribcol && CGO_ENABLED=0 go build -o /otelcontribcol

FROM alpine:latest as prep
RUN apk --update add ca-certificates

RUN mkdir -p /tmp

FROM scratch

ARG USER_UID=10001
USER ${USER_UID}

COPY --from=prep /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /otelcontribcol /otelcontribcol
EXPOSE 4317 55680 55679
ENTRYPOINT ["/otelcontribcol"]
CMD ["--config", "/etc/otel/config.yaml"]
