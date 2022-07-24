use anyhow::{anyhow, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub kind: Kind,
    pub client: u16,
    #[serde(rename = "tx")]
    pub id: u32,
    pub amount: Option<f64>,
    #[serde(skip)]
    pub disputed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl Transaction {
    pub fn get_amount(&self) -> Result<f64> {
        self.amount
            .ok_or_else(|| anyhow!("tx #{}: missing amount field", self.id))
    }
}
