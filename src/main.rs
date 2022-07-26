use std::{env::args, io::stdout, path::PathBuf};

use anyhow::{anyhow, Context, Error, Result};
use csv::Writer;

use crate::engine::Engine;

#[macro_use]
extern crate log;

mod client;
mod engine;
mod transaction;

pub type MaybeError = Option<Error>;

fn main() -> Result<()> {
    env_logger::init();
    info!("Toy Payment Engine");

    let input = input_file_from_args()?;
    info!("Input: {}", input.display());

    let engine = Engine::new(input).with_context(|| "invalid input")?;
    let clients = engine
        .process()
        .with_context(|| "processing input failed")?;
    info!("Process finished");

    let mut writer = Writer::from_writer(stdout());
    for client in clients.values() {
        writer.serialize(client)?;
    }
    writer.flush()?;

    info!("Result printed");

    Ok(())
}

fn input_file_from_args() -> Result<PathBuf> {
    let mut args = args();
    args.nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("Valid path to CSV file must be provided as a first argument"))
}
