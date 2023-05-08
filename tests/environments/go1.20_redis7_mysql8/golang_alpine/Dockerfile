FROM golang:alpine

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux

RUN apk add --no-cache git

WORKDIR /opt/project

COPY component ./component
COPY controllers ./controllers
COPY models ./models
COPY go.mod go.sum main.go ./

RUN go mod tidy
RUN go build

CMD ["./go-rush-producer"]