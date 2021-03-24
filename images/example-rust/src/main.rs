use mysql::PooledConn;
use mysql::Pool;
use mysql::params;
use std::error::Error;
use mysql::prelude::*;
use csv::Reader;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Example {
  name: String
} 


fn insert_example_csv_to_db(mut conn: PooledConn) -> Result<(), Box<dyn Error>> {
  let mut reader = Reader::from_path("../../data/example.csv")?;
  let mut records = Vec::new();
  for result in reader.deserialize() {
      let record: Example = result?;
      println!("{:?}", record);
      records.push(record);
  }


  // Doesn't work for some reason :(
  conn.exec_batch(
    r"INSERT INTO examples (name)
      VALUES (:name)",
    records.iter().map(|record| params! {
        "name" => &record.name,
    })
  )?;

  conn.query_drop(r"INSERT INTO examples(name) values ('test')");
  Ok(())
}

fn connect_to_db() -> PooledConn {
  let url = "mysql://codetest:swordfish@localhost:3306/codetest";
  let pool = Pool::new(url).expect("can't create pool ");
  

  let conn = pool.get_conn().expect("can't create connection");
  return conn
}


fn main() {
  let mut conn = connect_to_db();
  // Drop/Create tables
  conn.query_drop(r"DROP TABLE examples");
  conn.query_drop(
    r"CREATE TABLE examples (
        id int not null,
        name varchar(80) default null
    )").expect("can not do query");


  insert_example_csv_to_db(conn);

  println!("script completed")
}
