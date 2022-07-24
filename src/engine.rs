use std::{
    collections::{hash_map::Entry, HashMap},
    io::stdout,
    path::Path,
};

use anyhow::{anyhow, Context, Result};
use csv::{ReaderBuilder, Trim, Writer};

use crate::{
    client::Client,
    transaction::{Kind, Transaction},
};

#[derive(Debug, Default)]
pub struct Engine {
    clients: HashMap<u16, Client>,
    processed_transactions: HashMap<u32, Transaction>,
}

impl Engine {
    pub fn process(&mut self, input: &Path) -> Result<Vec<u8>> {
        let mut reader = ReaderBuilder::new()
            .trim(Trim::All)
            .from_path(input)
            .with_context(|| format!("can't initialize reader from path {}", input.display()))?;

        for transaction in reader.deserialize::<Transaction>().flatten() {
            info!("{transaction:?}");
            self.process_transaction(transaction)?;
        }

        let mut writer = Writer::from_writer(stdout());
        for client in self.clients.values() {
            writer.serialize(client)?;
        }

        Ok(vec![])
    }

    fn add_transaction(&mut self, transaction: Transaction) -> Result<()> {
        let key = transaction.id;
        if let Entry::Vacant(e) = self.processed_transactions.entry(key) {
            e.insert(transaction);
            Ok(())
        } else {
            Err(anyhow!("duplicate tx index: {}", key))
        }
    }

    fn process_transaction(&mut self, transaction: Transaction) -> Result<()> {
        let client = self
            .clients
            .entry(transaction.client)
            .or_insert_with(|| Client::new(transaction.client));
        match transaction.kind {
            Kind::Deposit => {
                let amount = transaction.get_amount()?;
                client.deposit(amount)?;
                self.add_transaction(transaction)?;
            }
            Kind::Withdrawal => {
                let amount = transaction.get_amount()?;
                client.withdrawal(amount)?;
                self.add_transaction(transaction)?;
            }
            Kind::Dispute => {
                if let Some(tx) = self.processed_transactions.get_mut(&transaction.id) {
                    if tx.client == transaction.client {
                        if tx.disputed {
                            warn!("tx #{}: already disputed", tx.id);
                        } else {
                            let amount = tx.get_amount()?;
                            match tx.kind {
                                Kind::Deposit => {
                                    client.dispute_deposit(amount)?;
                                    tx.disputed = true
                                }
                                _ => warn!("tx #{}: only deposit tx can be disputed", tx.id),
                            }
                        }
                    } else {
                        warn!(
                            "transactions clients mismatch: {} try to dispute {}",
                            tx.client, transaction.client
                        );
                    }
                } else {
                    warn!("unknown disputed tx: {}", transaction.id);
                }
            }
            Kind::Resolve => {
                if let Some(tx) = self.processed_transactions.get_mut(&transaction.id) {
                    if tx.client == transaction.client {
                        if tx.disputed {
                            let amount = tx.get_amount()?;
                            match tx.kind {
                                Kind::Deposit => {
                                    client
                                        .resolve_deposit(amount)
                                        .with_context(|| format!("disputed: {tx:?}"))?;
                                    tx.disputed = false;
                                }
                                _ => warn!("tx #{}: only deposit tx can be resolved", tx.id),
                            }
                        } else {
                            warn!("tx is not disputed: {}", tx.id);
                        }
                    } else {
                        warn!(
                            "can't resolve other clients: {} try to dispute {}",
                            tx.client, transaction.client
                        );
                    }
                } else {
                    warn!("unknown resolved tx: {}", transaction.id);
                }
            }
            Kind::Chargeback => {
                if let Some(tx) = self.processed_transactions.get(&transaction.id) {
                    if tx.client == transaction.client {
                        if tx.disputed {
                            let amount = tx.get_amount()?;
                            client.chargeback(amount)?;
                        } else {
                            warn!("tx #{}: is not disputed", tx.id);
                        }
                    }
                } else {
                    warn!("unknown chargeback tx: {}", transaction.id);
                }
            }
        }
        Ok(())
    }
}
