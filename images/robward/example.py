#!/usr/bin/env python

import csv
import json
import sqlalchemy
from sqlalchemy import Column, Date, String, Integer, func, exists, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
import contextlib

Base = declarative_base()
ARRAY_SIZE = 1000

class Place(Base):
    __tablename__ = "places"
    id = Column(Integer, primary_key=True)
    city = Column(String(80))
    county = Column(String(80))
    country = Column(String(80))

class PersonStage(Base):
    __tablename__ = "people_stage"
    id = Column(Integer, primary_key=True)
    given_name = Column(String(80))
    family_name = Column(String(80))
    date_of_birth = Column(Date())
    place_of_birth = Column(String(80))
    place_of_birth_id = Column(Integer)

class Person(Base):
    __tablename__ = "people"
    id = Column(Integer, primary_key=True)
    given_name = Column(String(80))
    family_name = Column(String(80))
    date_of_birth = Column(Date())
    place_of_birth_id = Column(Integer, ForeignKey('places.id'))

class RowCountDoesNotMatchException(Exception):
    pass

class MissingPlaceException(Exception):
    pass

class PeopleRowCountDoesNotMatchException(Exception):
    pass

def create_place(row):
    return Place(city = row[0], county = row[1], country = row[2])

def create_personstage(row):
    return PersonStage(given_name = row[0], family_name = row[1], date_of_birth = row[2], place_of_birth = row[3])

def create_person_list(rows):
    person_list = []
    for row in rows:
        person_list.append(Person(given_name = row[0], family_name = row[1], date_of_birth = row[2], place_of_birth_id = row[3]))
    return person_list

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

def compare_load_data(no_of_csv_rows, table):
    print ('No of csv rows : ', no_of_csv_rows)

    session = sqlalchemy.orm.Session(bind=engine);
    db_row_count = session.query(table).count()
    print ('No of db rows  : ', db_row_count)

    if no_of_csv_rows == db_row_count:
        print('No of rows match')
    else:
        raise RowCountDoesNotMatchException()

def check_all_places_exist():
    session = sqlalchemy.orm.Session(bind=engine)
    query = session.query(Person).\
    filter(
        ~exists().where(
                    PersonStage.place_of_birth == Place.city
        )
    )
    missing_places = session.execute(query.statement)

    if missing_places is None:
        print('place_of_birth records that do not match places:')
        for row in missing_places:
            print ("Row {0}, Palce {1}".format(row.id, row.place_of_birth))
        raise MissingPlaceException()
    print ('All places exist')

def update_place_of_birth(places_table, people_stage_table, people_table):
    session = sqlalchemy.orm.Session(bind=engine)
    total = 0

    try:
        # Suppress data being written to console by fetchmany
        with contextlib.redirect_stdout(None):
            source_query = session.query(people_stage_table.c.given_name.label('given_name'), people_stage_table.c.family_name.label('family_name'), people_stage_table.c.date_of_birth.label('date_of_birth'),places_table.c.id.label('place_of_birth_id')).join(places_table, places_table.c.city == people_stage_table.c.place_of_birth);

            results = session.execute(source_query.statement)
            while True:
                recs = results.fetchmany(ARRAY_SIZE)
                print(recs)
                _total = len(recs)
                total += _total
                if _total > 0:
                    session.bulk_save_objects(create_person_list(recs))
                    session.commit()
                if _total < ARRAY_SIZE:
                    break
        # done
        print(f'{total} records copied')
    except sqlalchemy.exc.NoSuchTableError as e1:
        pass

    print ('Places of birth linked to Places table')
    return total

def validate_people_table(people_stage_table, people_table):
    session = sqlalchemy.orm.Session(bind=engine);
    people_stage_row_count = session.query(people_stage_table).count()
    print ('No of people_stage rows : ', people_stage_row_count)

    people_row_count = session.query(people_table).count()

    print ('No of people rows       : ', people_row_count)

    if people_stage_row_count != people_row_count:
        raise PeopleRowCountDoesNotMatchException()

def output_summary(people_table, places_table):
    # output the table to a JSON file
    with open('/data/summary_output.json', 'w') as json_file:
        session = sqlalchemy.orm.Session(bind=engine)

        source_query = session.query(places_table.c.country.label('country'), func.count(places_table.c.country).label('count')).join(people_table, places_table.c.id == people_table.c.place_of_birth_id).group_by(places_table.c.country);

        result = connection.execute(source_query.statement).fetchall()
        rows = [{row[0] : row[1]} for row in result]
        json.dump(rows, json_file, separators=(',', ':'))

# connect to the database
engine = sqlalchemy.create_engine("mysql://codetest:swordfish@database/codetest")
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)

# make an ORM object to refer to the table
Places = sqlalchemy.schema.Table('places', metadata, autoload=True, autoload_with=engine)
PeopleStage = sqlalchemy.schema.Table('people_stage', metadata, autoload=True, autoload_with=engine)
People = sqlalchemy.schema.Table('people', metadata, autoload=True, autoload_with=engine)

# read the places CSV data file into the table
print ('Loading places')
no_csv_places = load_data('/data/places.csv', create_place)
compare_load_data(no_csv_places, Place)

print ()
print ('Loading people')
no_csv_people = load_data('/data/people.csv', create_personstage)
compare_load_data(no_csv_people, PersonStage)

print ()
print ('Check all places of birth exist in places')
check_all_places_exist()

print ()
print ('Set place_of_birth foreign key')
update_place_of_birth(Places, PeopleStage, People)

print ()
print ('Validate people table is correct')
validate_people_table(PeopleStage, People)

print ()
print ('Output summary')
output_summary(People, Places)
