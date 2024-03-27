from sqlalchemy import text
import json
from model import Session


def write_json():
    # output the table to a JSON file
    with open('/data/summary_output.json', 'w') as json_file:
        with Session() as session:
            sql = text('SELECT places.country, COUNT(people.id) AS citizens '
                       'FROM people '
                       'INNER JOIN  places ON places.city = people.place_of_birth '
                       'GROUP BY places.country')
            result = session.execute(sql).fetchall()
            data =[{country: citizen} for country, citizen in result]
            json_data = json.dumps(data)
            json_file.write(json_data)
            print("Finished writing file")


write_json()