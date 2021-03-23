FROM swift:5.3

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update --quiet --yes \
  && apt-get install --quiet --yes libmysqlclient-dev

WORKDIR /app

COPY . .
RUN swift build
ENTRYPOINT swift run 
