use mysql::PooledConn;
use mysql::Pool;
use mysql::prelude::*;
use mysql::params;
use csv::Reader;
use mysql::Error;

use serde::Deserialize;
#[derive(Debug, Deserialize)]
struct Example {
  name: String
} 

#[derive(Debug, Deserialize)]
struct ExampleRead {
  id: i32,
  name: String
}


fn read_csv() -> Vec<Example> {
  let mut records = Vec::new();
  let mut reader = Reader::from_path("../../data/example.csv").unwrap();
  let iter = reader.records();

  for result in iter {
    let record = result.unwrap();
    let row: Example = record.deserialize(None).unwrap();
    records.push(row)
  }
  return records
}
fn connect_to_db() -> PooledConn {
  let url = "mysql://codetest:swordfish@localhost:3306/codetest";
  let pool = Pool::new(url).expect("can't create pool ");
  

  let conn = pool.get_conn().expect("can't create connection");
  return conn
}


fn main() -> Result<(), Error> {
  let mut conn = connect_to_db();
  // Drop/Create tables
  conn.query_drop(r"DROP TABLE examples").unwrap();
  conn.query_drop(
    r"CREATE TABLE examples (
        id int not null auto_increment,
        name varchar(80) default null,
        primary key(id)
    )").expect("can not do query");


  let records = read_csv();

// Doesn't work for some reason :(
  conn.exec_batch(
    r"INSERT INTO examples (name)
      VALUES (:name)",
    records.iter().map(|record| params! {
        "name" => &record.name,
    })
  ).unwrap();

  let selected_records = conn
    .query_map(
        "SELECT id, name from examples",
        |(id, name)| {
            ExampleRead { id, name }
        },
    )?;


  println!("{:?}",records);
  println!("{:?}",selected_records);
  println!("Yay!");
  Ok(())
}
