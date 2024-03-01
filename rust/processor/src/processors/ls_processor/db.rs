use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use aptos_protos::transaction::v1::{Event, Transaction};
use bigdecimal::BigDecimal;
use diesel::Insertable;
use diesel::{deserialize::Queryable, ExpressionMethods, Selectable};
use diesel_async::RunQueryDsl;
use serde::Deserialize;
use tonic::async_trait;
use tracing::info;

use crate::{
    processors::ls_processor::mv::{
        filter_ls_events, EventLs, MoveStructTagLs, TransactionInfo, TransactionLs,
    },
    schema::{self, ls_events, ls_pools},
    utils::database::PgPoolConnection,
};

// Write 100 values at a time to the table
const TB_CHUNKS_SIZE: usize = 100;

#[derive(Debug)]
pub(crate) enum LsDB {
    Pools(TableLsPool),
    Events(TableLsEvent),
}

impl LsDB {
    pub(crate) fn try_from_tx(
        addresses: &[(String, String)],
        tx: &Transaction,
    ) -> Result<Vec<LsDB>> {
        filter_ls_events(addresses, tx)
            .ok_or(anyhow!("It is not a user transaction"))?
            .map(|ev| LsDB::try_from_ev_tx(ev, tx))
            .collect::<Result<Vec<_>>>()
    }

    fn try_from_ev_tx((version, ev): (&String, &Event), tx: &Transaction) -> Result<LsDB> {
        let mv_st = ev.move_struct().ok_or(anyhow!("expected Move Struct"))?;
        let event_type = LsEventType::from_str(&mv_st.name)?;

        match event_type {
            // When new pool created.
            LsEventType::PoolCreatedEvent => {
                let pool_type = mv_st.pool_type()?;

                Ok(LsDB::Pools(TableLsPool {
                    id: pool_type.hash(),
                    version_ls: version.clone(),
                    x_name: pool_type.x_name,
                    y_name: pool_type.y_name,
                    curve: pool_type.curve,
                    x_val: BigDecimal::from(0),
                    y_val: BigDecimal::from(0),
                    fee: 0,
                    last_event: 0,
                }))
            },

            LsEventType::LiquidityAddedEvent
            | LsEventType::OracleUpdatedEvent
            | LsEventType::SwapEvent
            | LsEventType::LiquidityRemovedEvent
            | LsEventType::FlashloanEvent
            | LsEventType::UpdateFeeEvent
            | LsEventType::UpdateDAOFeeEvent => {
                let pool_type = mv_st.pool_type()?;
                let TransactionInfo {
                    version,
                    tx_hash,
                    timestamp,
                    sender,
                } = tx.info().ok_or(anyhow!(
                    "Not all data could be extracted from the transaction"
                ))?;

                let even_type = ev.data_value()?;
                let data: ObjEventType = serde_json::from_value(even_type.clone())
                    .map_err(|err| anyhow!("{err:?}\n{even_type:?}"))?;

                let (x_val, y_val, fee) = data.get_val()?;

                Ok(LsDB::Events(TableLsEvent {
                    id: ev.key()? + "_" + &ev.sequence_number.to_string(),
                    pool_id: pool_type.hash(),
                    tp: event_type,
                    even_type,
                    timestamp,
                    tx_hash,
                    sender,
                    version,
                    x_val: x_val.map(|v| v.into()),
                    y_val: y_val.map(|v| v.into()),
                    fee,
                    sq: None,
                }))
            },
        }
    }
}

#[async_trait]
pub(crate) trait InsertToDb {
    async fn insert_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()>;
}

#[async_trait]
impl InsertToDb for Vec<LsDB> {
    async fn insert_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        let (pools, events): (Vec<_>, Vec<_>) = self
            .into_iter()
            .partition(|row| matches!(row, LsDB::Pools(..)));

        let pools = pools
            .into_iter()
            .filter_map(|ls_db| match ls_db {
                LsDB::Pools(pools) => Some(pools),
                _ => None,
            })
            .collect::<Vec<_>>();
        pools.insert_to_db(conn).await?;

        let events = events
            .into_iter()
            .filter_map(|ls_db| match ls_db {
                LsDB::Events(events) => Some(events),
                _ => None,
            })
            .collect::<Vec<_>>();
        events.insert_to_db(conn).await?;

        Ok(())
    }
}

#[derive(Selectable, Queryable, Insertable, Debug, Clone)]
#[diesel(table_name = ls_pools)]
pub struct TableLsPool {
    pub id: String,
    pub version_ls: String,
    pub x_name: String,
    pub y_name: String,
    pub curve: String,
    pub x_val: BigDecimal,
    pub y_val: BigDecimal,
    pub fee: i64,
    pub last_event: i64,
}

impl TableLsPool {
    pub async fn update_to_db(&self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        diesel::insert_into(schema::ls_pools::table)
            .values(self)
            .on_conflict(schema::ls_pools::id)
            .do_update()
            .set((
                schema::ls_pools::x_val.eq(&self.x_val),
                schema::ls_pools::y_val.eq(&self.y_val),
                schema::ls_pools::fee.eq(&self.fee),
            ))
            .execute(conn)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl InsertToDb for Vec<TableLsPool> {
    async fn insert_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        let count = self.len();

        for rows in self.chunks(TB_CHUNKS_SIZE) {
            diesel::insert_into(schema::ls_pools::table)
                .values(rows)
                .on_conflict(schema::ls_pools::id)
                .do_nothing()
                .execute(conn)
                .await?;
        }
        info!("{count} TableLsPool added");

        Ok(())
    }
}

#[derive(Selectable, Queryable, Insertable, Debug)]
#[diesel(table_name = ls_events)]
pub struct TableLsEvent {
    pub id: String,
    pub pool_id: String,
    pub tp: LsEventType,
    pub version: i64,
    pub tx_hash: String,
    pub sender: String,
    pub even_type: serde_json::Value,
    pub timestamp: i64,
    pub x_val: Option<BigDecimal>,
    pub y_val: Option<BigDecimal>,
    pub fee: Option<i64>,
    pub sq: Option<i64>,
}

impl TableLsEvent {
    pub async fn update_to_db(&self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        diesel::insert_into(schema::ls_events::table)
            .values(self)
            .on_conflict(schema::ls_events::id)
            .do_update()
            .set((
                schema::ls_events::x_val.eq(&self.x_val),
                schema::ls_events::y_val.eq(&self.y_val),
                schema::ls_events::fee.eq(&self.fee),
            ))
            .execute(conn)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl InsertToDb for Vec<TableLsEvent> {
    async fn insert_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        let count = self.len();
        for rows in self.chunks(TB_CHUNKS_SIZE) {
            diesel::insert_into(schema::ls_events::table)
                .values(rows)
                .on_conflict(schema::ls_events::id)
                .do_nothing()
                .execute(conn)
                .await?;
        }
        info!("{count} TableLsEvent added");

        Ok(())
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(
    Clone, Copy, Debug, diesel_derive_enum::DbEnum, diesel::query_builder::QueryId, PartialEq, Eq,
)]
#[ExistingTypePath = "crate::schema::sql_types::EventType"]
pub enum LsEventType {
    // When new pool created.
    PoolCreatedEvent,
    // When liquidity added to the pool.
    LiquidityAddedEvent,
    // When liquidity removed from the pool.
    LiquidityRemovedEvent,
    // When swap happened.
    SwapEvent,
    // When flashloan event happened.
    FlashloanEvent,
    // When oracle updated (i don't think we need to catch it).
    OracleUpdatedEvent,
    // When fee of pool updated.
    UpdateFeeEvent,
    // When DAO fee updated for the pool.
    UpdateDAOFeeEvent,
}

impl FromStr for LsEventType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        let result = match s {
            // When new pool created.
            "PoolCreatedEvent" => LsEventType::PoolCreatedEvent,
            // When liquidity added to the pool.
            "LiquidityAddedEvent" => LsEventType::LiquidityAddedEvent,
            // When liquidity removed from the pool.
            "LiquidityRemovedEvent" => LsEventType::LiquidityRemovedEvent,
            // When swap happened.
            "SwapEvent" => LsEventType::SwapEvent,
            // When flashloan event happened.
            "FlashloanEvent" => LsEventType::FlashloanEvent,
            // When oracle updated (i don't think we need to catch it).
            "OracleUpdatedEvent" => LsEventType::OracleUpdatedEvent,
            // When fee of pool updated.
            "UpdateFeeEvent" => LsEventType::UpdateFeeEvent,
            // When DAO fee updated for the pool.
            "UpdateDAOFeeEvent" => LsEventType::UpdateDAOFeeEvent,
            _ => bail!("Unknown event"),
        };
        Ok(result)
    }
}

pub(crate) fn sha256_from_str(s: &str) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(s);
    format!("{:x}", hasher.finalize())
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
pub enum ObjEventType {
    Added {
        added_x_val: String,
        added_y_val: String,
        lp_tokens_received: String,
    },
    Swap {
        x_in: String,
        y_in: String,
        x_out: String,
        y_out: String,
    },
    Return {
        returned_x_val: String,
        returned_y_val: String,
        lp_tokens_burned: String,
    },
    Last {
        last_price_x_cumulative: String,
        last_price_y_cumulative: String,
    },
    NewFee {
        new_fee: String,
    },
}

impl ObjEventType {
    /// (x_val, y_val, fee)
    pub fn get_val(&self) -> Result<(Option<i128>, Option<i128>, Option<i64>)> {
        let (mut x, mut y, mut fee): (Option<i128>, Option<i128>, Option<i64>) = (None, None, None);

        match self {
            ObjEventType::Added {
                added_x_val,
                added_y_val,
                ..
            } => {
                x = Some(added_x_val.parse::<i128>()?);
                y = Some(added_y_val.parse::<i128>()?);
            },
            ObjEventType::Swap {
                x_in,
                y_in,
                x_out,
                y_out,
            } => {
                x = Some(x_in.parse::<i128>()? - x_out.parse::<i128>()?);
                y = Some(y_in.parse::<i128>()? - y_out.parse::<i128>()?);
            },
            ObjEventType::Return {
                returned_x_val,
                returned_y_val,
                ..
            } => {
                x = Some(-returned_x_val.parse::<i128>()?);
                y = Some(-returned_y_val.parse::<i128>()?);
            },
            ObjEventType::Last { .. } => {},
            ObjEventType::NewFee { new_fee } => {
                fee = Some(new_fee.parse()?);
            },
        }

        Ok((x, y, fee))
    }
}
