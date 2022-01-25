FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update --quiet --yes \
  && apt-get install --quiet --yes clang libmysqlclient-dev

WORKDIR /app

COPY example.c ./
RUN clang -o example example.c `mysql_config --cflags --libs`

ENTRYPOINT ./example
