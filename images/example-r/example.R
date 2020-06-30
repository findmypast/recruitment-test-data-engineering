#!/usr/bin/env Rscript

library(tidyverse)
library(jsonlite)

example_data <- read_csv('/data/example.csv')

write_json(example_data, '/data/example_r.json', pretty = TRUE, auto_unbox = TRUE)
