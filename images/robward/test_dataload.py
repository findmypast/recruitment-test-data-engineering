#!/usr/bin/env python
"""Tests to check the error cases of the DataLoad module"""
import pytest
from dataload import DataLoad


def test_missing_rows_on_load():
    """Check that an error is reported if the number of rows in the csv file differs from the number of rows in the table"""
    obj = DataLoad()

    no_csv_places = obj.load_data("/data/places.csv", obj.create_place)

    # Remove a row so the numbers are different
    obj.connection.execute("DELETE FROM places WHERE id=10")

    with pytest.raises(obj.RowCountDoesNotMatchException):
        obj.verify_data_load(no_csv_places, obj.Place)


def test_unknown_place():
    """Check correct exception is raised if a place of birth is not in the places table"""
    obj = DataLoad()

    no_csv_places = obj.load_data("/data/places.csv", obj.create_place)
    no_csv_people = obj.load_data(
        "/data/people_unknown_place.csv", obj.create_personstage
    )

    with pytest.raises(obj.MissingPlaceException):
        obj.verify_all_places_exist()


def test_people_copy_worked():
    """Check that the the copying of the data from the people_stage to the people table worked"""
    obj = DataLoad()

    no_csv_places = obj.load_data("/data/places.csv", obj.create_place)
    no_csv_people = obj.load_data("/data/people.csv", obj.create_personstage)

    # Remove a row so the numbers are different
    obj.connection.execute("DELETE FROM people WHERE id=2203")

    with pytest.raises(obj.PeopleRowCountDoesNotMatchException):
        obj.verify_people_table(obj.PeopleStage, obj.People)


def test_consistent_output():
    """Check that if the total number of places matches the expected number"""
    obj = DataLoad()

    no_csv_places = obj.load_data("/data/places.csv", obj.create_place)
    no_csv_people = obj.load_data("/data/people.csv", obj.create_personstage)

    incorrect_number_of_rows = no_csv_people - 34

    with pytest.raises(obj.UnexpectedPlaceTotalsException):
        obj.output_summary(obj.People, obj.Places, incorrect_number_of_rows)
