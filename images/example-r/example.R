#!/usr/bin/env Rscript

library(tidyverse)
library(jsonlite)
library(DBI)

# connect to the database
database <- dbConnect(RMySQL::MySQL(), user = 'codetest', password = 'swordfish', dbname = 'codetest', host = 'database')
dbListTables(database)

# read the CSV data file into the table
example_data <- read_csv('/data/example.csv')
# TODO: write the data into the database
dbSendQuery(database, "insert into examples(name) values('Testie')")

# output the table to a JSON file
res <- dbSendQuery(database, "select * from examples")
output = dbFetch(res)
dbClearResult(res)

write_json(output, '/data/example_r.json', pretty = FALSE, auto_unbox = TRUE)

