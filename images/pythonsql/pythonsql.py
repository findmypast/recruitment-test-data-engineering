#!/usr/bin/env python

import csv
import json
import sqlalchemy

# connect to the database
engine = sqlalchemy.create_engine("mysql://codetest:swordfish@database/codetest")
connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)

# make an ORM object to refer to the table
people = sqlalchemy.schema.Table('people', metadata, autoload=True, autoload_with=engine)

# read the CSV data file into the table
with open('/data/people.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  for row in reader: 
    connection.execute(people.insert().values(name = row[0]))

# output the table to a JSON file
with open('/data/people_python.json', 'w') as json_file:
  rows = connection.execute(sqlalchemy.sql.select([people])).fetchall()
  rows = [{'id': row[0], 'name': row[1]} for row in rows]
  json.dump(rows, json_file, separators=(',', ':'))


# make an ORM object to refer to the table
places = sqlalchemy.schema.Table('places', metadata, autoload=True, autoload_with=engine)

# read the CSV data file into the table
with open('/data/places.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  for row in reader: 
    connection.execute(places.insert().values(name = row[0]))

# output the table to a JSON file
with open('/data/places_python.json', 'w') as json_file:
  rows = connection.execute(sqlalchemy.sql.select([places])).fetchall()
  rows = [{'id': row[0], 'name': row[1]} for row in rows]
  json.dump(rows, json_file, separators=(',', ':'))
