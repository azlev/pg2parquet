use libpq::connection::Connection;
use libpq::Status;

use std::process::exit;

use pg2parquet::config;
use pg2parquet::postgres::replication_protocol;

fn main() {
    let config = config::build_config().unwrap();

    let client = Connection::new(&config.conninfo).unwrap();

    let replication_command = &format!("START_REPLICATION SLOT {configslot} LOGICAL 000/000 (proto_version '3', publication_names '{configpublication}')",
        configslot = config.slot,
        configpublication = config.publication);
    eprintln!("DEBUG: {}", replication_command);
    let res = client.exec(replication_command);

    if res.status() != Status::CopyBoth {
        eprintln!("{}", client.error_message().unwrap());
        exit(1);
    }

    loop {
        let buffer = client.copy_data(false).expect("Error while reading data");

        match buffer[0] {
            replication_protocol::PRIMARY_KEEPALIVE_ID => {
                let ka = replication_protocol::parse_keepalive(&buffer);
                println!("keepalive, LSN: {}, {}, {}", ka.0, ka.1, ka.2);
                if ka.2 == true {
                    println!("reply requested!");
                    let reply = replication_protocol::create_keepalive();
                    client
                        .put_copy_data(&reply)
                        .expect("Sending keeplive reply failed.");
                    client.flush().expect("Error in flush.");
                    println!("reply sent!");
                }
            }
            replication_protocol::XLOG_DATA_ID => {
                println!("XLOG_DATA_ID: {}", buffer[0]);
            }
            _ => eprintln!("Unrecognized message: {}", buffer[0]),
        }
    }
}