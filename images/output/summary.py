from sqlalchemy import text
from sqlalchemy import create_engine
import json
from sqlalchemy.orm import sessionmaker

engine = create_engine("mysql://root:swordfish@database:3306/codetest")
Session = sessionmaker(bind=engine)

def write_json():
    # output the table to a JSON file
    with open('/data/summary_output.json', 'w') as json_file:
        with Session() as session:
            sql = text('SELECT places.country, COUNT(people.id) AS citizens '
                       ' FROM people '
                       ' INNER JOIN  places ON places.city = people.place_of_birth '
                       'GROUP BY places.country')
            result = session.execute(sql).fetchall()
            print(f"the result of the query is {result}")
            data =[{country: citizen} for country, citizen in result]
            json_data = json.dumps(data)
            json_file.write(json_data)
            print("Finished writing file")

write_json()