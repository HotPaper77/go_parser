#docker build . -t go_parser
docker run --name letsgo --memory="20m" --cpuset-cpus="0-1" -v "$PWD/samples/sample_1":/app/samples/sample_1  go_parser