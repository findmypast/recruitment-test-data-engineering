#!/usr/bin/env python

import csv
import json
import sqlalchemy
from sqlalchemy import Column, DateTime, String, Integer, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Place(Base):
    __tablename__ = "places"
    id = Column(Integer, primary_key=True)
    city = Column(String(80))
    county = Column(String(80))
    country = Column(String(80))

class Person(Base):
    __tablename__ = "people"
    id = Column(Integer, primary_key=True)
    given_name = Column(String(80))
    family_name = Column(String(80))
    date_of_birth = Column(String(80))
    place_of_birth = Column(String(80))

def create_place(row):
    return Place(city = row[0], county = row[1], country = row[2])

def create_person(row):
    return Person(given_name = row[0], family_name = row[1], date_of_birth = row[2], place_of_birth = row[3])

def load_data(csv_filename, load_func):
    object_list = []

    with open(csv_filename) as csv_file:
      number_of_csv_lines = 0
      reader = csv.reader(csv_file)
      next(reader)
      for row in reader:
          object_list.append(load_func(row))
          number_of_csv_lines = number_of_csv_lines + 1

      session = sqlalchemy.orm.Session(bind=engine)
      session.bulk_save_objects(object_list)
      session.commit()
      return number_of_csv_lines

def compare_load_data(no_of_csv_files, table):
    print ('No of csv lines : ', no_of_csv_files)

    session = sqlalchemy.orm.Session(bind=engine);
    count = session.query(table).count()
    print ('No of db lines  : ', count)

# connect to the database
engine = sqlalchemy.create_engine("mysql://codetest:swordfish@database/codetest")
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)

# make an ORM object to refer to the table
Places = sqlalchemy.schema.Table('places', metadata, autoload=True, autoload_with=engine)
People = sqlalchemy.schema.Table('people', metadata, autoload=True, autoload_with=engine)

# read the places CSV data file into the table
print ('Loading places')
no_csv_places = load_data('/data/places.csv', create_place)
compare_load_data(no_csv_places, Place)

print ('Loading people')
no_csv_people = load_data('/data/people.csv', create_person)
compare_load_data(no_csv_people, Person)

# output the table to a JSON file
#with open('/data/example_python.json', 'w') as json_file:
#  rows = connection.execute(sqlalchemy.sql.select([Example])).fetchall()
#  rows = [{'id': row[0], 'name': row[1]} for row in rows]
#  json.dump(rows, json_file, separators=(',', ':'))
