use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{error, instrument};

use aptos_protos::transaction::v1::Transaction;

pub mod db;
mod events;
mod mv;

use crate::processors::ls_processor::db::InsertToDb;
use crate::{
    processors::{
        ls_processor::mv::clr_hex_address, ProcessingResult, ProcessorName, ProcessorTrait,
    },
    utils::database::{PgDbPool, PgPoolConnection},
};

use self::events::LsEvent;

pub struct LsProcessor {
    connection_pool: PgDbPool,
    ls_config: LsConfigs,
}

impl LsProcessor {
    pub fn new(connection_pool: PgDbPool, mut ls_config: LsConfigs) -> Self {
        ls_config
            .address
            .iter_mut()
            .for_each(|(_version_ls, address)| *address = clr_hex_address(address));

        Self {
            connection_pool,
            ls_config,
        }
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

    #[instrument(level = "debug", skip(self, transactions))]
    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();
        let last_transaction_timstamp = transactions.last().and_then(|t| t.timestamp.clone());

        let events: Vec<LsEvent> = LsEvent::try_from_txs(&self.ls_config.address, &transactions)?;

        if events.is_empty() {
            let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();

            return Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs: 0_f64,
                last_transaction_timstamp,
            });
        }

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();
        let mut conn: PgPoolConnection = self.connection_pool.get().await?;

        events.insert_to_db(&mut conn).await.map_err(|err| {
            error!(
                start_version = start_version,
                end_version = end_version,
                processor_name = self.name(),
                ?err,
                "[Parser] Error inserting transactions to db",
            );
            err
        })?;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();

        Ok(ProcessingResult {
            start_version,
            end_version,
            processing_duration_in_secs,
            db_insertion_duration_in_secs,
            last_transaction_timstamp: transactions.last().unwrap().timestamp.clone(),
        })
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LsConfigs {
    // The logic is the same, yet the addresses different, modules deployed at:
    // 0x0163df34fccbf003ce219d3f1d9e70d140b60622cb9dd47599c25fb2f797ba6e
    //
    // Resource account:
    // 0x61d2c22a6cb7831bee0f48363b0eec92369357aece0d1142062f7d5d85c7bef8
    //
    // Vec<(VERSION_LS,ADDRESS)>
    address: Vec<(String, String)>,
}
