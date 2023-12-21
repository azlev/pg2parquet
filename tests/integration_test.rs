use libpq::{connection::Connection, Status};

use pg2parquet::postgres::replication_protocol;

pub fn setup() {}

#[test]
fn insert() {
    let client =
        Connection::new("host=127.0.0.1 port=5433 user=postgres password=postgres").unwrap();
    let res = client
        .exec("create table teste (id int generated always as identity primary key, t text);");

    if res.status() != Status::CommandOk {
        panic!("Failed to setup tests");
    }
    assert!(true)
}
