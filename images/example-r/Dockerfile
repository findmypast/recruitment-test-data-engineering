FROM r-base:4.0.2

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update --quiet --yes \
  && apt-get install --quiet --yes r-cran-rmysql r-cran-tidyverse
  
COPY example.R ./
CMD ["./example.R"]
