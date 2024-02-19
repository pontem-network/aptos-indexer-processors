// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::transaction_payload::Payload;
use aptos_protos::transaction::v1::Event;
use aptos_protos::transaction::v1::{
    transaction::{TransactionType, TxnData},
    Transaction,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use field_count::FieldCount;
use once_cell::sync::Lazy;
use std::fmt::Debug;
use tracing::{debug, error, info};

use super::{ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    models::default_models::{
        block_metadata_transactions::BlockMetadataTransactionModel,
        move_modules::MoveModule,
        move_resources::MoveResource,
        move_tables::{CurrentTableItem, TableItem, TableMetadata},
        transactions::TransactionModel,
        write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    schema,
    utils::database::{execute_in_chunks, PgDbPool},
};

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

async fn insert_to_db(
    conn: PgDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    txns: Vec<TransactionModel>,
    block_metadata_transactions: Vec<BlockMetadataTransactionModel>,
    wscs: Vec<WriteSetChangeModel>,
    (move_modules, move_resources, table_items, current_table_items, table_metadata): (
        Vec<MoveModule>,
        Vec<MoveResource>,
        Vec<TableItem>,
        Vec<CurrentTableItem>,
        Vec<TableMetadata>,
    ),
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    execute_in_chunks(
        conn.clone(),
        insert_transactions_query,
        txns,
        TransactionModel::field_count(),
    )
    .await?;
    execute_in_chunks(
        conn.clone(),
        insert_block_metadata_transactions_query,
        block_metadata_transactions,
        BlockMetadataTransactionModel::field_count(),
    )
    .await?;
    execute_in_chunks(
        conn.clone(),
        insert_write_set_changes_query,
        wscs,
        WriteSetChangeModel::field_count(),
    )
    .await?;
    execute_in_chunks(
        conn.clone(),
        insert_move_modules_query,
        move_modules,
        MoveModule::field_count(),
    )
    .await?;
    execute_in_chunks(
        conn.clone(),
        insert_move_resources_query,
        move_resources,
        MoveResource::field_count(),
    )
    .await?;
    execute_in_chunks(
        conn.clone(),
        insert_table_items_query,
        table_items,
        TableItem::field_count(),
    )
    .await?;
    execute_in_chunks(
        conn.clone(),
        insert_current_table_items_query,
        current_table_items,
        CurrentTableItem::field_count(),
    )
    .await?;
    execute_in_chunks(
        conn.clone(),
        insert_table_metadata_query,
        table_metadata,
        TableMetadata::field_count(),
    )
    .await?;

    Ok(())
}

fn insert_transactions_query(
    items_to_insert: Vec<TransactionModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::transactions::dsl::*;

    (
        diesel::insert_into(schema::transactions::table)
            .values(items_to_insert)
            .on_conflict(version)
            .do_update()
            .set((
                inserted_at.eq(excluded(inserted_at)),
                payload_type.eq(excluded(payload_type)),
            )),
        None,
    )
}

fn insert_block_metadata_transactions_query(
    items_to_insert: Vec<BlockMetadataTransactionModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::block_metadata_transactions::dsl::*;

    (
        diesel::insert_into(schema::block_metadata_transactions::table)
            .values(items_to_insert)
            .on_conflict(version)
            .do_nothing(),
        None,
    )
}

fn insert_write_set_changes_query(
    items_to_insert: Vec<WriteSetChangeModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::write_set_changes::dsl::*;

    (
        diesel::insert_into(schema::write_set_changes::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, index))
            .do_nothing(),
        None,
    )
}

fn insert_move_modules_query(
    items_to_insert: Vec<MoveModule>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::move_modules::dsl::*;

    (
        diesel::insert_into(schema::move_modules::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_move_resources_query(
    items_to_insert: Vec<MoveResource>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::move_resources::dsl::*;

    (
        diesel::insert_into(schema::move_resources::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_table_items_query(
    items_to_insert: Vec<TableItem>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::table_items::dsl::*;

    (
        diesel::insert_into(schema::table_items::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_current_table_items_query(
    items_to_insert: Vec<CurrentTableItem>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_table_items::dsl::*;

    (
        diesel::insert_into(schema::current_table_items::table)
            .values(items_to_insert)
            .on_conflict((table_handle, key_hash))
            .do_update()
            .set((
                key.eq(excluded(key)),
                decoded_key.eq(excluded(decoded_key)),
                decoded_value.eq(excluded(decoded_value)),
                is_deleted.eq(excluded(is_deleted)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_table_items.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_table_metadata_query(
    items_to_insert: Vec<TableMetadata>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::table_metadatas::dsl::*;

    (
        diesel::insert_into(schema::table_metadatas::table)
            .values(items_to_insert)
            .on_conflict(handle)
            .do_nothing(),
        None,
    )
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

        todo!("ls transactions");

        let (txns, block_metadata_txns, write_set_changes, wsc_details) =
            TransactionModel::from_transactions(&transactions);

        dbg!(&txns.first());

        let mut block_metadata_transactions = vec![];
        for block_metadata_txn in block_metadata_txns {
            block_metadata_transactions.push(block_metadata_txn.clone());
        }
        let mut move_modules = vec![];
        let mut move_resources = vec![];
        let mut table_items = vec![];
        let mut current_table_items = AHashMap::new();
        let mut table_metadata = AHashMap::new();
        for detail in wsc_details {
            match detail {
                WriteSetChangeDetail::Module(module) => move_modules.push(module.clone()),
                WriteSetChangeDetail::Resource(resource) => move_resources.push(resource.clone()),
                WriteSetChangeDetail::Table(item, current_item, metadata) => {
                    table_items.push(item.clone());
                    current_table_items.insert(
                        (
                            current_item.table_handle.clone(),
                            current_item.key_hash.clone(),
                        ),
                        current_item.clone(),
                    );
                    if let Some(meta) = metadata {
                        table_metadata.insert(meta.handle.clone(), meta.clone());
                    }
                },
            }
        }

        // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
        let mut current_table_items = current_table_items
            .into_values()
            .collect::<Vec<CurrentTableItem>>();
        let mut table_metadata = table_metadata.into_values().collect::<Vec<TableMetadata>>();
        // Sort by PK
        current_table_items
            .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
        table_metadata.sort_by(|a, b| a.handle.cmp(&b.handle));

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            txns,
            block_metadata_transactions,
            write_set_changes,
            (
                move_modules,
                move_resources,
                table_items,
                current_table_items,
                table_metadata,
            ),
        )
        .await;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        match tx_result {
            Ok(_) => Ok(ProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
                last_transaction_timstamp: transactions.last().unwrap().timestamp.clone(),
            }),
            Err(e) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error inserting transactions to db",
                );
                bail!(e)
            },
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
    let usr_tx = match tx.txn_data.as_ref()? {
        TxnData::User(user_tx, ..) => Some(user_tx),
        _ => None,
    }?;

    usr_tx
        .events
        .iter()
        .filter(ls_filter_events)
        .next()
        .is_some()
        .then_some(tx)
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
fn ls_filter_events(ev: &&Event) -> bool {
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
