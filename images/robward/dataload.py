#!/usr/bin/env python
"""Module to import places and people csv files into database tables"""
import contextlib
import csv
import json
from sqlalchemy import (
    create_engine,
    Column,
    Date,
    String,
    Integer,
    func,
    exists,
    ForeignKey,
)
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.schema import MetaData, Table

BATCH_SIZE = 5000


class DataLoad:
    Base = declarative_base()

    class Place(Base):
        """Class to represent a Place in the places table"""

        __tablename__ = "places"
        id = Column(Integer, primary_key=True)
        city = Column(String(80))
        county = Column(String(80))
        country = Column(String(80))

    class PersonStage(Base):
        """Class to represent a Person in the people_stage table"""

        __tablename__ = "people_stage"
        id = Column(Integer, primary_key=True)
        given_name = Column(String(80))
        family_name = Column(String(80))
        date_of_birth = Column(Date())
        place_of_birth = Column(String(80))

    class Person(Base):
        """Class to represent a Person in the people table"""

        __tablename__ = "people"
        id = Column(Integer, primary_key=True)
        given_name = Column(String(80))
        family_name = Column(String(80))
        date_of_birth = Column(Date())
        place_of_birth_id = Column(Integer, ForeignKey("places.id"))

    class RowCountDoesNotMatchException(Exception):
        """Exception raised when row count does not match between csv file and table"""

        pass

    class MissingPlaceException(Exception):
        """Exception raised place_of_birth does not exist in places table"""

        pass

    class PeopleRowCountDoesNotMatchException(Exception):
        """Exception raised number of rows in people_stage and people does not match"""

        pass

    class UnexpectedPlaceTotalsException(Exception):
        """Exception raised when the total number of place counts does not match the total number of people"""

        pass

    # connect to the database
    engine = create_engine("mysql://codetest:swordfish@database/codetest")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    connection = engine.connect()

    metadata = MetaData(engine)

    # make an ORM object to refer to the table
    Places = Table("places", metadata, autoload=True, autoload_with=engine)
    PeopleStage = Table("people_stage", metadata, autoload=True, autoload_with=engine)
    People = Table("people", metadata, autoload=True, autoload_with=engine)

    def create_place(self, row):
        """Create a Place object from array"""
        return self.Place(city=row[0], county=row[1], country=row[2])

    def create_personstage(self, row):
        """Create a PersonStage object from array"""
        return self.PersonStage(
            given_name=row[0],
            family_name=row[1],
            date_of_birth=row[2],
            place_of_birth=row[3],
        )

    def create_person_list(self, rows):
        """Create a list of Person objects from array"""
        person_list = []
        for row in rows:
            person_list.append(
                self.Person(
                    given_name=row[0],
                    family_name=row[1],
                    date_of_birth=row[2],
                    place_of_birth_id=row[3],
                )
            )
        return person_list

    def load_data(self, csv_filename, load_func):
        """Load data from csv file into a table. The function is supplied to create the object from the data"""

        with open(csv_filename, encoding="utf-8") as csv_file:
            number_of_csv_lines = 0
            reader = csv.reader(csv_file)
            next(reader)
            object_list = []

            batch_count = 0
            for row in reader:
                object_list.append(load_func(row))
                batch_count = batch_count + 1
                if batch_count >= BATCH_SIZE:
                    session = Session(bind=self.engine)
                    session.bulk_save_objects(object_list)
                    session.commit()
                    object_list = []
                    number_of_csv_lines = number_of_csv_lines + batch_count
                    batch_count = 0
                    print(f"Rows loaded : {number_of_csv_lines}")

            if batch_count > 0:
                session = Session(bind=self.engine)
                session.bulk_save_objects(object_list)
                session.commit()
                number_of_csv_lines = number_of_csv_lines + batch_count

            return number_of_csv_lines

    def verify_data_load(self, no_of_csv_rows, table):
        """Compare the number of rows in the csv file and the row count"""
        print("No of csv rows : ", no_of_csv_rows)

        session = Session(bind=self.engine)
        db_row_count = session.query(table).count()
        session.close()
        print("No of db rows  : ", db_row_count)

        if no_of_csv_rows == db_row_count:
            print("No of rows match")
        else:
            raise self.RowCountDoesNotMatchException()

    def verify_all_places_exist(self):
        """Verify that all places of birth in the people_stage table exist in the places table"""
        session = Session(bind=self.engine)
        query = session.query(self.PersonStage).filter(
            ~exists().where(self.PersonStage.place_of_birth == self.Place.city)
        )

        missing_places = session.execute(query.statement).all()

        if len(missing_places) > 0:
            print("place_of_birth records that do not match places:")
            for row in missing_places:
                print(f"Row {row[0].id}, Place of birth {row[0].place_of_birth}")
            raise self.MissingPlaceException()
        session.close()
        print("All places exist")

    def update_place_of_birth(self, places_table, people_stage_table):
        """Populate the people table from the people_stage table with the place_of_birth_id foreign key set"""
        session = Session(bind=self.engine)
        total = 0

        try:
            # Suppress data being written to console by fetchmany
            with contextlib.redirect_stdout(None):
                source_query = session.query(
                    people_stage_table.c.given_name.label("given_name"),
                    people_stage_table.c.family_name.label("family_name"),
                    people_stage_table.c.date_of_birth.label("date_of_birth"),
                    places_table.c.id.label("place_of_birth_id"),
                ).join(
                    places_table,
                    places_table.c.city == people_stage_table.c.place_of_birth,
                )

                results = session.execute(source_query.statement)
                while True:
                    recs = results.fetchmany(BATCH_SIZE)
                    _total = len(recs)
                    total += _total
                    if _total > 0:
                        session.bulk_save_objects(self.create_person_list(recs))
                        session.commit()
                    if _total < BATCH_SIZE:
                        break
                session.close()

            # done
            print(f"{total} records copied")
        except NoSuchTableError as e1:
            pass

        print("Places of birth linked to Places table")
        return total

    def verify_people_table(self, people_stage_table, people_table):
        """Verify that people table matches the people_stage table"""
        session = Session(bind=self.engine)
        people_stage_row_count = session.query(people_stage_table).count()
        print("No of people_stage rows : ", people_stage_row_count)

        people_row_count = session.query(people_table).count()
        session.close()

        print("No of people rows       : ", people_row_count)

        if people_stage_row_count != people_row_count:
            raise self.PeopleRowCountDoesNotMatchException()

    def drop_staging_table(self, staging_tables):
        """Drop staging tables when they are no longer needed"""
        for stage_table in staging_tables:
            print(f"Dropping {stage_table.name}")
            stage_table.drop(self.engine)

    def output_summary(self, people_table, places_table, expected_total):
        """Output the table to a JSON file"""
        with open("/data/summary_output.json", "w", encoding="utf-8") as json_file:
            session = Session(bind=self.engine)

            source_query = (
                session.query(
                    places_table.c.country.label("country"),
                    func.count(places_table.c.country).label("count"),
                )
                .join(
                    people_table, places_table.c.id == people_table.c.place_of_birth_id
                )
                .group_by(places_table.c.country)
            )

            result = self.connection.execute(source_query.statement).fetchall()
            session.close()

            total_count = 0
            rows = []
            for row in result:
                rows.append({row[0]: row[1]})
                total_count = total_count + row[1]

            if expected_total != total_count:
                raise self.UnexpectedPlaceTotalsException()

            json.dump(rows, json_file, separators=(",", ":"))
            print(rows)

    def ingest(self):
        # read the places CSV data file into the table
        print("Loading places")
        no_csv_places = self.load_data("/data/places.csv", self.create_place)
        self.verify_data_load(no_csv_places, self.Place)

        print()
        print("Loading people")
        no_csv_people = self.load_data("/data/people.csv", self.create_personstage)
        self.verify_data_load(no_csv_people, self.PersonStage)

        print()
        print("Verify all places of birth exist in places")
        self.verify_all_places_exist()

        print()
        print("Set place_of_birth foreign key")
        self.update_place_of_birth(self.Places, self.PeopleStage)

        print()
        print("Verify people table is correct")
        self.verify_people_table(self.PeopleStage, self.People)

        print()
        print("Drop staging tables")
        self.drop_staging_table([self.PeopleStage])

        print()
        print("Output summary")
        self.output_summary(self.People, self.Places, no_csv_people)
