use std::{
    env::args,
    io::{stdout, Write},
    path::PathBuf,
};

use anyhow::{anyhow, Context, Result};

use crate::engine::Engine;

#[macro_use]
extern crate log;

mod client;
mod engine;
mod transaction;

fn main() -> Result<()> {
    env_logger::init();
    info!("Toy Payment Engine");

    let input = input_file_from_args()?;
    info!("Input: {}", input.display());

    let mut engine = Engine::default();
    let output = engine
        .process(&input)
        .with_context(|| "processing input failed")?;
    info!("Process finished");

    stdout().write_all(&output)?;
    info!("Result printed");

    Ok(())
}

fn input_file_from_args() -> Result<PathBuf> {
    let mut args = args();
    args.nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("Valid path to CSV file must be provided as a first argument"))
}
