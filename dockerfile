From golang:1.22.1

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build 

ENTRYPOINT ["./go_parser","samples/sample_1"]

