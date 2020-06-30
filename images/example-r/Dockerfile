FROM r-base:4.0.2

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update --quiet --yes \
  && apt-get install --quiet --yes libcurl4-openssl-dev libmysqlclient-dev libssl-dev libxml2-dev
  
RUN R -e "install.packages('tidyverse', dependencies=TRUE, repos='http://cran.rstudio.com/')"

COPY example.R ./
CMD ["./example.R"]
