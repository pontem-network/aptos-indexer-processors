// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use anyhow::{anyhow, bail};
use aptos_protos::transaction::v1::{
    transaction::{TransactionType, TxnData},
    Transaction,
};
use aptos_protos::transaction::v1::{Event, UserTransaction};
use async_trait::async_trait;
use diesel_async::RunQueryDsl;
use once_cell::sync::Lazy;
use tracing::{debug, error};

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::schema::ls_transactions::hash as hash_field;
use crate::utils::database::PgPoolConnection;
use crate::{schema, schema::ls_transactions, utils::database::PgDbPool};

const USER_TX: i32 = TransactionType::User as i32;
const LS_ADDRESSES: [&str; 2] = [
    "0x0163df34fccbf003ce219d3f1d9e70d140b60622cb9dd47599c25fb2f797ba6e",
    "0x61d2c22a6cb7831bee0f48363b0eec92369357aece0d1142062f7d5d85c7bef8",
];
const LS_EVENTS: [&str; 8] = [
    "PoolCreatedEvent",
    "LiquidityAddedEvent",
    "LiquidityRemovedEvent",
    "SwapEvent",
    "FlashloanEvent",
    "OracleUpdatedEvent",
    "UpdateFeeEvent",
    "UpdateDAOFeeEvent",
];
pub const MAX_TX_CHUNCK: usize = 100;

static LS_TYPE_STR: Lazy<Vec<String>> = Lazy::new(ls_gen_type_pref);

pub struct LsProcessor {
    connection_pool: PgDbPool,
}

impl LsProcessor {
    pub fn new(connection_pool: PgDbPool) -> Self {
        Self { connection_pool }
    }
}

impl Debug for LsProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "DefaultTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

#[async_trait]
impl ProcessorTrait for LsProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::LsProcessor.into()
    }

    async fn process_transactions(
        &self,
        mut transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let last_transaction_timstamp = transactions.last().and_then(|t| t.timestamp.clone());

        debug!(?start_version, ?end_version, "process_transactions",);

        transactions = transactions
            .into_iter()
            .filter_map(only_user_tx)
            .filter_map(only_success)
            .filter_map(only_ls_tx)
            .collect();

        if transactions.is_empty() {
            let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

            return Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs: 0_f64,
                last_transaction_timstamp,
            });
        }

        let txns: Vec<LsTransaction> = transactions
            .iter()
            .filter_map(|tx| match tx.try_into() {
                Ok(t) => Some(t),
                Err(err) => {
                    error!("{err}");
                    None
                },
            })
            .collect();
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let mut conn: PgPoolConnection = self.connection_pool.get().await?;
        let mut fails = Vec::default();
        for tx_ch in txns.chunks(MAX_TX_CHUNCK) {
            if let Err(err) = diesel::insert_into(schema::ls_transactions::table)
                .values(tx_ch)
                .on_conflict(hash_field)
                .do_nothing()
                .execute(&mut conn)
                .await
            {
                error!("{err}");
                fails.push(err.to_string());
            }
        }

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();

        if fails.is_empty() {
            Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
                last_transaction_timstamp: transactions.last().unwrap().timestamp.clone(),
            })
        } else {
            let error = fails.join("\n");
            error!(
                start_version = start_version,
                end_version = end_version,
                processor_name = self.name(),
                ?error,
                "[Parser] Error inserting transactions to db",
            );
            bail!(error)
        }
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

fn only_user_tx(tx: Transaction) -> Option<Transaction> {
    matches!(tx.r#type, USER_TX).then_some(tx)
}

fn only_success(tx: Transaction) -> Option<Transaction> {
    tx.info.as_ref()?.success.then_some(tx)
}

fn only_ls_tx(tx: Transaction) -> Option<Transaction> {
    let usr_tx = unwrap_usr_tx(&tx)?;
    usr_tx.events.iter().any(ls_filter_events).then_some(tx)
}

// // When new pool created.
// PoolCreatedEvent
// // When liquidity added to the pool.
// LiquidityAddedEvent
// // When liquidity removed from the pool.
// LiquidityRemovedEvent
// // When swap happened.
// SwapEvent
// // When flashloan event happened.
// FlashloanEvent
// // When oracle updated (i don't think we need to catch it).
// OracleUpdatedEvent
// // When fee of pool updated.
// UpdateFeeEvent
// // When DAO fee updated for the pool.
// UpdateDAOFeeEvent
// = = =
// The logic is the same, yet the addresses different, modules deployed at:
// 0x0163df34fccbf003ce219d3f1d9e70d140b60622cb9dd47599c25fb2f797ba6e
//
// Resource account:
// 0x61d2c22a6cb7831bee0f48363b0eec92369357aece0d1142062f7d5d85c7bef8
fn ls_filter_events(ev: &Event) -> bool {
    LS_TYPE_STR.iter().any(|pref| ev.type_str.starts_with(pref))
}

fn ls_gen_type_pref() -> Vec<String> {
    let r = LS_ADDRESSES
        .iter()
        .map(clr_hex_address)
        .flat_map(|address| {
            let ad_mod = format!("{address}::liquidity_pool");
            LS_EVENTS.map(|st| format!("{ad_mod}::{st}"))
        })
        .collect::<Vec<_>>();
    dbg!(&r);
    r
}

#[inline]
fn clr_hex_address(address: &&str) -> String {
    format!(
        "0x{}",
        address.trim_start_matches("0x").trim_start_matches('0')
    )
}

#[derive(Insertable, Debug)]
#[diesel(table_name = ls_transactions)]
pub struct LsTransaction {
    pub hash: String,
    pub sender: String,
    pub transaction: serde_json::Value,
    pub timestamp: i64,
}

impl TryFrom<&Transaction> for LsTransaction {
    type Error = anyhow::Error;

    fn try_from(tx: &Transaction) -> Result<Self, Self::Error> {
        let timestamp = tx
            .timestamp
            .as_ref()
            .ok_or(anyhow!("The timestamp is not specified in the transaction"))?
            .seconds;

        let mut hash = hex::encode(
            &tx.info
                .as_ref()
                .ok_or(anyhow!("The INFO is not specified in the transaction"))?
                .hash,
        )
        .trim_start_matches('0')
        .to_string();
        hash = format!("{hash:0>32}");

        let mut sender = unwrap_usr_tx(tx)
            .and_then(|usr| usr.request.as_ref())
            .map(|tx| &tx.sender)
            .ok_or(anyhow!("The SENDER is not specified in the transaction"))?
            .trim_start_matches("0x")
            .trim_start_matches('0')
            .to_string();
        sender = format!("{sender:0>32}");

        let transaction = serde_json::to_value(tx)?;

        Ok(LsTransaction {
            sender,
            hash,
            transaction,
            timestamp,
        })
    }
}

#[inline]
fn unwrap_usr_tx(tx: &Transaction) -> Option<&UserTransaction> {
    match tx.txn_data.as_ref()? {
        TxnData::User(user_tx, ..) => Some(user_tx),
        _ => None,
    }
}
