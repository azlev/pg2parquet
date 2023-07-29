use std::env;
use std::fmt;

pub struct ConfigError {
    error_message: String,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.error_message)
    }
}

impl fmt::Debug for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.error_message)
    }
}

pub struct Config {
    pub publication: String,
    pub slot: String,
    pub output: String,
    pub conninfo: String,
}

pub fn build_config() -> Result<Config, ConfigError> {
    let publication = match env::var("PG2PARQUET_PUBLICATION") {
        Ok(val) => val.clone(),
        Err(_e) => {
            return Result::Err(ConfigError {
                error_message: String::from("PG2PARQUET_PUBLICATION is empty"),
            })
        }
    };

    let slot = match env::var("PG2PARQUET_SLOT") {
        Ok(val) => val.clone(),
        Err(_e) => {
            return Result::Err(ConfigError {
                error_message: String::from("PG2PARQUET_SLOT is empty"),
            })
        }
    };

    let output = match env::var("PG2PARQUET_OUTPUT") {
        Ok(val) => val.clone(),
        Err(_e) => {
            eprintln!("WARN: PG2PARQUET_OUTPUT is empty, using current directory");
            String::from(".")
        }
    };

    let conninfo = match env::var("PG2PARQUET_CONNINFO") {
        Ok(val) => val.clone(),
        Err(_e) => {
            eprintln!(
                "WARN: PG2PARQUET_CONNINFO is empty, using local connection and current user"
            );
            String::from("")
        }
    };

    Result::Ok(Config {
        publication: publication,
        slot: slot,
        output: output,
        conninfo: conninfo,
    })
}

#[cfg(test)]
mod tests {
    use crate::config::build_config;
    use std::env;

    #[test]
    fn simple() {
        env::set_var("PG2PARQUET_PUBLICATION", "pg2parquet");
        env::set_var("PG2PARQUET_SLOT", "pg2parquet");
        env::set_var("PG2PARQUET_OUTPUT", ".");
        env::set_var("PG2PARQUET_conninfo", "host=localhost");

        let config = build_config();
    }
}
