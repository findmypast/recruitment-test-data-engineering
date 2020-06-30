#!/usr/bin/env Rscript

library(tidyverse)
library(jsonlite)

# connect to the database
# ???

# read the CSV data file into the table
example_data <- read_csv('/data/example.csv')

# debug output
print(example_data)

# output the table to a JSON file
write_json(example_data, '/data/example_r.json', pretty = FALSE, auto_unbox = TRUE)
