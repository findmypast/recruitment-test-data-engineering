import pandas
from model import engine, Base, Places, People


Base.metadata.create_all(engine)


def load_data(file_name, table_name):
    df = pandas.read_csv(file_name)
    df.to_sql(con=engine, name=table_name, if_exists="append", index=False)


print("Loading places")
load_data("/data/places.csv", Places.__tablename__)
print("Loading people")
load_data("/data/people.csv", People.__tablename__)
