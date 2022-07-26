use std::{
    collections::{hash_map::Entry, HashMap},
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Error};
use csv::{ByteRecord, Position, Reader, ReaderBuilder, Trim};

use crate::{
    client::Client,
    transaction::{Kind, Transaction},
};

type TxId = u32;

#[derive(Debug)]
pub struct Engine {
    input: PathBuf,
    clients: HashMap<u16, Client>,
    processed_transactions: HashMap<TxId, Position>,
    disputed_transactions: HashMap<TxId, Transaction>,

    reader: Reader<File>,
}

impl Engine {
    pub fn new(input: PathBuf) -> Result<Self, Error> {
        let reader = Self::reader(&input)?;
        let engine = Self {
            input,
            reader,
            clients: HashMap::default(),
            processed_transactions: HashMap::default(),
            disputed_transactions: HashMap::default(),
        };
        Ok(engine)
    }

    pub fn reader(path: &Path) -> Result<Reader<File>, Error> {
        ReaderBuilder::new()
            .trim(Trim::All)
            .from_path(path)
            .with_context(|| format!("can't initialize reader from path {}", path.display()))
    }

    pub fn process(mut self) -> Result<HashMap<u16, Client>, Error> {
        let mut reader = Self::reader(&self.input)?;
        let mut raw_record = ByteRecord::new();
        let headers = reader.byte_headers()?.clone();

        while reader.read_byte_record(&mut raw_record)? {
            if let Ok(transaction) = raw_record.deserialize::<Transaction>(Some(&headers)) {
                info!("{transaction:?}");
                let position = raw_record
                    .position()
                    .expect("record has not position")
                    .clone();
                if let Err(Some(e)) = self.process_transaction(&transaction, position) {
                    return Err(e);
                }
            }
        }

        Ok(self.clients)
    }

    fn process_transaction(
        &mut self,
        transaction: &Transaction,
        position: Position,
    ) -> Result<(), Option<Error>> {
        let f = match transaction.kind {
            Kind::Deposit => Self::deposit,
            Kind::Withdrawal => Self::withdrawal,
            Kind::Dispute => Self::dispute,
            Kind::Resolve => Self::resolve,
            Kind::Chargeback => Self::chargeback,
        };
        f(self, transaction, position)
    }

    fn deposit(
        &mut self,
        transaction: &Transaction,
        position: Position,
    ) -> Result<(), Option<Error>> {
        let amount = transaction.get_amount()?;
        self.client(transaction.client).deposit(amount)?;
        self.add_transaction(transaction.id, position)?;
        Ok(())
    }

    fn withdrawal(
        &mut self,
        transaction: &Transaction,
        position: Position,
    ) -> Result<(), Option<Error>> {
        let amount = transaction.get_amount()?;
        self.client(transaction.client).withdrawal(amount)?;
        self.add_transaction(transaction.id, position)?;
        Ok(())
    }

    fn dispute(
        &mut self,
        transaction: &Transaction,
        position: Position,
    ) -> Result<(), Option<Error>> {
        let tx = self.load_transaction(transaction.id).map_err(|_| None)?;
        if tx.client != transaction.client {
            warn!("tx clients mismatch: at {}", position.line());
            return Ok(());
        }
        if self.disputed_transactions.contains_key(&tx.id) {
            warn!("tx #{}: already disputed", tx.id);
            return Ok(());
        }
        if !matches!(tx.kind, Kind::Deposit) {
            warn!("tx #{}: only deposit tx can be disputed", tx.id);
            return Ok(());
        }
        let amount = tx.get_amount()?;
        self.client(transaction.client).dispute_deposit(amount)?;
        debug!("added disputed tx: #{}", tx.id);
        self.disputed_transactions.insert(tx.id, tx);
        Ok(())
    }

    fn resolve(
        &mut self,
        transaction: &Transaction,
        position: Position,
    ) -> Result<(), Option<Error>> {
        let tx = self
            .disputed_transactions
            .get(&transaction.id)
            .cloned()
            .ok_or(None)?;
        if tx.client != transaction.client {
            warn!("tx clients mismatch: at {}", position.line());
            return Ok(());
        }
        if !matches!(tx.kind, Kind::Deposit) {
            unreachable!("only deposit tx can be disputed");
        }
        let amount = tx.get_amount()?;
        self.client(transaction.client).resolve_deposit(amount)?;
        self.disputed_transactions.remove(&tx.id);
        Ok(())
    }

    fn chargeback(
        &mut self,
        transaction: &Transaction,
        position: Position,
    ) -> Result<(), Option<Error>> {
        let tx = self
            .disputed_transactions
            .get(&transaction.id)
            .cloned()
            .ok_or(None)?;
        if tx.client != transaction.client {
            warn!("tx clients mismatch: at {}", position.line());
            return Ok(());
        }

        let amount = tx.get_amount()?;
        self.client(transaction.client).chargeback(amount)?;
        self.disputed_transactions.remove(&tx.id);

        Ok(())
    }

    fn add_transaction(&mut self, id: u32, position: Position) -> Result<(), Error> {
        if let Entry::Vacant(e) = self.processed_transactions.entry(id) {
            e.insert(position);
            Ok(())
        } else {
            Err(anyhow!("duplicate tx index: {}", id))
        }
    }

    fn get_position(&self, id: u32) -> Option<&Position> {
        self.processed_transactions.get(&id)
    }

    fn load_transaction(&mut self, id: u32) -> Result<Transaction, Error> {
        let position = self
            .get_position(id)
            .ok_or_else(|| anyhow!("id not found: {}", id))?
            .clone();
        self.reader.seek(position)?;
        let mut raw_record = ByteRecord::new();
        let headers = self.reader.byte_headers()?.clone();
        self.reader.read_byte_record(&mut raw_record)?;
        let transaction = raw_record.deserialize(Some(&headers))?;
        Ok(transaction)
    }

    fn client(&mut self, client_id: u16) -> &mut Client {
        self.clients
            .entry(client_id)
            .or_insert_with(|| Client::new(client_id))
    }
}
