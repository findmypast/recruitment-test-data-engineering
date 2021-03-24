FROM swift:5.3

RUN export DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true \
  && apt-get update --quiet --yes \
  && apt-get dist-upgrade --quiet --yes \
  && apt-get install --quiet --yes libmysqlclient-dev

WORKDIR /app

COPY Package.swift .
RUN swift package resolve

COPY . .
RUN swift build

ENTRYPOINT swift run 
