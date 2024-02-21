use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use aptos_protos::transaction::v1::{Event, Transaction};
use diesel_async::RunQueryDsl;
use tracing::warn;

use crate::{
    processors::ls_processor::mv::{
        filter_ls_events, EventLs, MoveStructTagLs, TransactionInfo, TransactionLs,
    },
    schema::{self, ls_events, ls_pools},
    utils::database::PgPoolConnection,
};

#[derive(Debug)]
pub(crate) enum LsDB {
    Pools(TableLsPools),
    Events(TableLsEvents, Option<()>),
}

impl LsDB {
    pub(crate) fn try_from_tx(addresses: &[String], tx: &Transaction) -> Result<Vec<LsDB>> {
        filter_ls_events(addresses, tx)
            .ok_or(anyhow!("It is not a user transaction"))?
            .map(|ev| LsDB::try_from_ev_tx(ev, tx))
            .collect::<Result<Vec<_>>>()
    }

    fn try_from_ev_tx(ev: &Event, tx: &Transaction) -> Result<LsDB> {
        let mv_st = ev.move_struct().ok_or(anyhow!("expected Move Struct"))?;
        let event_type = LsEventType::from_str(&mv_st.name)?;

        dbg!(&ev);
        dbg!(mv_st);

        match event_type {
            // When new pool created.
            LsEventType::PoolCreatedEvent => {
                let pool_type = mv_st.pool_type()?;

                Ok(LsDB::Pools(TableLsPools {
                    id: pool_type.hash(),
                    x_name: pool_type.x_name,
                    y_name: pool_type.y_name,
                    curve: pool_type.curve,
                    x_val: 0,
                    y_val: 0,
                    fee: 0,
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
                let data = ev.data_value()?;

                dbg!(&data);

                match event_type {
                    // When liquidity added to the pool.
                    LsEventType::LiquidityAddedEvent => {
                        // @todo
                        let [x_val, y_val] = ["added_x_val", "added_y_val"].map(|index| {
                            data.get(index)
                                .and_then(|v| v.as_str())
                                .ok_or(anyhow!(
                                    "The value `LiquidityAddedEvent::{index}` was not found"
                                ))
                                .and_then(|value| value.parse::<u64>().map_err(anyhow::Error::from))
                        });
                    },
                    // When oracle updated (i don't think we need to catch it).
                    LsEventType::OracleUpdatedEvent => {
                        // @todo
                        let [last_price_x_cumulative, last_price_y_cumulative] =
                            ["last_price_x_cumulative", "last_price_y_cumulative"].map(|index| {
                                data.get(index)
                                    .and_then(|v| v.as_str())
                                    .ok_or(anyhow!(
                                        "The value `OracleUpdatedEvent::{index}` was not found"
                                    ))
                                    .and_then(|value| {
                                        value.parse::<u128>().map_err(anyhow::Error::from)
                                    })
                            });
                    },
                    // When swap happened.
                    LsEventType::SwapEvent => {
                        // @todo
                        let [x_in, x_out, y_in, y_out] =
                            ["x_in", "x_out", "y_in", "y_out"].map(|index| {
                                data.get(index)
                                    .and_then(|v| v.as_str())
                                    .ok_or(anyhow!("The value `SwapEvent::{index}` was not found"))
                                    .and_then(|value| {
                                        value.parse::<u64>().map_err(anyhow::Error::from)
                                    })
                            });
                    },
                    // When liquidity removed from the pool.
                    LsEventType::LiquidityRemovedEvent => {
                        // @todo
                        let [lp_tokens_burned, returned_x_val, returned_y_val] =
                            ["lp_tokens_burned", "returned_x_val", "returned_y_val"].map(|index| {
                                data.get(index)
                                    .and_then(|v| v.as_str())
                                    .ok_or(anyhow!(
                                        "The value `LiquidityRemovedEvent::{index}` was not found"
                                    ))
                                    .and_then(|value| {
                                        value.parse::<u64>().map_err(anyhow::Error::from)
                                    })
                            });
                    },
                    // When flashloan event happened.
                    LsEventType::FlashloanEvent => {
                        // @todo
                        let [x_in, x_out, y_in, y_out] =
                            ["x_in", "x_out", "y_in", "y_out"].map(|index| {
                                data.get(index)
                                    .and_then(|v| v.as_str())
                                    .ok_or(anyhow!(
                                        "The value `FlashloanEvent::{index}` was not found"
                                    ))
                                    .and_then(|value| {
                                        value.parse::<u64>().map_err(anyhow::Error::from)
                                    })
                            });
                    },
                    // When fee of pool updated.
                    LsEventType::UpdateFeeEvent => {
                        // @todo
                        let new_fee = data
                            .get("new_fee")
                            .and_then(|v| v.as_str())
                            .ok_or(anyhow!("The value `UpdateFeeEvent::new_fee` was not found"))
                            .and_then(|value| value.parse::<u64>().map_err(anyhow::Error::from))?;
                    },
                    // When DAO fee updated for the pool.
                    LsEventType::UpdateDAOFeeEvent => {
                        // @todo
                        let new_fee = data
                            .get("new_fee")
                            .and_then(|v| v.as_str())
                            .ok_or(anyhow!(
                                "The value `UpdateDAOFeeEvent::new_fee` was not found"
                            ))
                            .and_then(|value| value.parse::<u64>().map_err(anyhow::Error::from))?;
                    },
                    _ => unimplemented!(),
                }

                Ok(LsDB::Events(
                    TableLsEvents {
                        id: ev.key()? + "_" + &ev.sequence_number.to_string(),
                        pool_id: pool_type.hash(),
                        tp: event_type,
                        even_type: data,
                        timestamp,
                        tx_hash,
                        sender,
                        version,
                    },
                    // @todo update lq
                    None,
                ))
            },
        }
    }

    pub(crate) async fn run(&self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        match self {
            LsDB::Pools(pl) => pl.insert(conn).await,
            LsDB::Events(ev, val) => {
                if ev.insert(conn).await? {
                    dbg!(&val);
                    warn!("update liq");
                }
                Ok(())
            },
        }
    }
}

#[derive(Insertable, Debug)]
#[diesel(table_name = ls_pools)]
pub(crate) struct TableLsPools {
    id: String,
    x_name: String,
    y_name: String,
    curve: String,
    x_val: i64,
    y_val: i64,
    fee: i64,
}

impl TableLsPools {
    pub(crate) async fn insert(&self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        diesel::insert_into(schema::ls_pools::table)
            .values(self)
            .on_conflict(schema::ls_pools::id)
            .do_nothing()
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Insertable, Debug)]
#[diesel(table_name = ls_events)]
pub(crate) struct TableLsEvents {
    id: String,
    pool_id: String,
    tp: LsEventType,
    version: i64,
    tx_hash: String,
    sender: String,
    even_type: serde_json::Value,
    timestamp: i64,
}

impl TableLsEvents {
    pub(crate) async fn insert(&self, conn: &mut PgPoolConnection<'_>) -> Result<bool> {
        dbg!(self);

        let result = diesel::insert_into(schema::ls_events::table)
            .values(self)
            .on_conflict(schema::ls_events::id)
            .do_nothing()
            .execute(conn)
            .await?;

        Ok(result != 0)
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, diesel_derive_enum::DbEnum)]
#[ExistingTypePath = "crate::schema::sql_types::EventType"]
pub(crate) enum LsEventType {
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
