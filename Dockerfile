FROM golang:latest

WORKDIR /app
COPY . /app
RUN go build .

EXPOSE 8000

CMD [ "go", "run", "." ]