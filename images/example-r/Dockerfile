FROM rocker/tidyverse:4.0.4

WORKDIR /app

RUN R -e "install.packages('RMySQL', dependencies=TRUE, repos='http://cran.rstudio.com/')" 

COPY example.R ./
CMD ["./example.R"]
