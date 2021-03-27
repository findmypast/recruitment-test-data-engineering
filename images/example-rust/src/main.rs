use mysql::{PooledConn, Pool};
use mysql::prelude::*;
use mysql::params;
use csv::Reader;
use serde::{Deserialize, Serialize};
use serde_json::{Result};
use std::fs::{OpenOptions};
use std::io::Write;


#[derive(Debug, Deserialize, Serialize)]
struct Example {
  name: String
} 

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ExampleRead {
  id: i32,
  name: String
}


fn read_csv() -> Vec<Example> {
  let mut records = Vec::new();
  let mut reader = Reader::from_path("/data/example.csv").unwrap();
  let iter = reader.records();

  // It didn't like deserializing StringRecord and applying it to a struct with an Option<i32>
  // DOn't know why 'yet', to get around this a seperate struct was created in order to read in the files.
  for result in iter {
    let record = result.unwrap();
    let row: Example = record.deserialize(None).unwrap();
    records.push(row)
  }
  return records
}

fn connect_to_db() -> PooledConn {
  let url = "mysql://codetest:swordfish@database:3306/codetest";
  let pool = Pool::new(url).expect("can't create pool ");
  

  let conn = pool.get_conn().expect("can't create connection");
  return conn
}

fn write_to_json(selected_records: Vec<ExampleRead>) -> std::result::Result<(), serde_json::Error> {
  let mut file = OpenOptions::new()
                  .write(true)
                  .append(true)
                  .create(true)
                  .open("/data/example-rust.json")
                  .expect("Could not open/create file");
  let iter = selected_records.iter();
  let iter_length =  iter.len();
  let mut count = 0;

  file.write_all("[\n".as_bytes()).expect("write failed");
  for record in iter {
    count = count + 1;
    // Serialize it to a JSON string
    let record_json = serde_json::to_string(&record)?;
    file.write_all(record_json.as_bytes()).expect("writing record failed");

    if iter_length == count {
    } else {
      file.write_all(",\n".as_bytes()).expect("writing comma failed");
    }
  }
  file.write_all("\n]".as_bytes()).expect("write failed");
  println!("Successfully wrote a file");
  Ok(())
}

fn main() -> Result<()> {
  let mut conn = connect_to_db();
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
    ).unwrap();

  write_to_json(selected_records)?;

  println!("Script Completed!");
  Ok(())
}
