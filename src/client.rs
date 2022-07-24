use anyhow::{anyhow, Context, Result};
use serde::Serialize;

#[derive(Debug, Serialize, Default)]
pub struct Client {
    id: u16,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

impl Client {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    fn check_lock(&self) -> Result<()> {
        if self.locked {
            Err(anyhow!("Client #{}: is locked", self.id))
        } else {
            Ok(())
        }
    }

    fn can_reduce_balance(&self, amount: f64) -> Result<()> {
        if self.available < amount || self.total < amount {
            Err(anyhow!("Client #{}: insufficient funds", self.id))
        } else {
            Ok(())
        }
    }

    fn can_reduce_held(&self, amount: f64) -> Result<()> {
        if self.held < amount {
            debug!("held: {}, amount: {amount}", self.held);
            Err(anyhow!("Client #{}: insufficient funds held", self.id))
        } else {
            Ok(())
        }
    }

    pub fn deposit(&mut self, amount: f64) -> Result<()> {
        self.check_lock()?;
        self.available += amount;
        self.total += amount;
        Ok(())
    }

    pub fn withdrawal(&mut self, amount: f64) -> Result<()> {
        self.check_lock()?;
        self.can_reduce_balance(amount)?;
        self.available -= amount;
        self.total -= amount;
        Ok(())
    }

    pub fn dispute_deposit(&mut self, amount: f64) -> Result<()> {
        self.check_lock()?;
        self.can_reduce_balance(amount)?;
        self.available -= amount;
        self.held += amount;
        Ok(())
    }

    pub fn resolve_deposit(&mut self, amount: f64) -> Result<()> {
        self.check_lock()?;
        self.can_reduce_held(amount)
            .with_context(|| "can't reduce held funds to resolve")?;
        self.available += amount;
        self.held -= amount;
        Ok(())
    }

    pub fn chargeback(&mut self, amount: f64) -> Result<()> {
        self.check_lock()?;
        self.can_reduce_held(amount)
            .with_context(|| "can't reduce held funds for chargeback")?;
        self.held -= amount;
        self.total -= amount;
        self.locked = true;
        Ok(())
    }
}
