use std::{collections::HashMap, str::FromStr};

use anyhow::{anyhow, bail, Result};
use aptos_protos::transaction::v1::{Event, Transaction};
use bigdecimal::BigDecimal;
use diesel::{
    query_dsl::methods::{FilterDsl, SelectDsl},
    ExpressionMethods,
};
use diesel_async::RunQueryDsl;
use tonic::async_trait;
use tracing::debug;

use crate::{
    processors::ls_processor::mv::{
        filter_ls_events, EventLs, MoveStructTagLs, TransactionInfo, TransactionLs,
    },
    schema::{self, ls_events, ls_pools},
    utils::{
        database::PgPoolConnection,
        util::{bigdecimal_to_u64, u64_to_bigdecimal},
    },
};

// Write 100 values at a time to the table
const TB_CHUNKS_SIZE: usize = 100;

#[derive(Debug)]
pub(crate) enum LsDB {
    Pools(TableLsPool),
    Events(TableLsEvent),
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

        match event_type {
            // When new pool created.
            LsEventType::PoolCreatedEvent => {
                let pool_type = mv_st.pool_type()?;

                Ok(LsDB::Pools(TableLsPool {
                    id: pool_type.hash(),
                    x_name: pool_type.x_name,
                    y_name: pool_type.y_name,
                    curve: pool_type.curve,
                    x_val: u64_to_bigdecimal(0),
                    y_val: u64_to_bigdecimal(0),
                    fee: 0,
                    last_tx_version: 0,
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

                Ok(LsDB::Events(TableLsEvent {
                    id: ev.key()? + "_" + &ev.sequence_number.to_string(),
                    pool_id: pool_type.hash(),
                    tp: event_type,
                    even_type: data,
                    timestamp,
                    tx_hash,
                    sender,
                    version,
                }))
            },
        }
    }

    pub(crate) fn pool_id(&self) -> &String {
        match self {
            LsDB::Pools(p) => &p.id,
            LsDB::Events(p) => &p.pool_id,
        }
    }
}

#[async_trait]
pub(crate) trait WriteToDb {
    async fn write_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()>;
}

#[async_trait]
impl WriteToDb for Vec<LsDB> {
    async fn write_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
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
        pools.write_to_db(conn).await?;

        let events = events
            .into_iter()
            .filter_map(|ls_db| match ls_db {
                LsDB::Events(events) => Some(events),
                _ => None,
            })
            .collect::<Vec<_>>();
        events.write_to_db(conn).await?;

        Ok(())
    }
}

#[derive(Insertable, Debug)]
#[diesel(table_name = ls_pools)]
pub(crate) struct TableLsPool {
    id: String,
    x_name: String,
    y_name: String,
    curve: String,
    x_val: BigDecimal,
    y_val: BigDecimal,
    fee: i64,
    last_tx_version: i64,
}

#[async_trait]
impl WriteToDb for Vec<TableLsPool> {
    async fn write_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        for rows in self.chunks(TB_CHUNKS_SIZE) {
            diesel::insert_into(schema::ls_pools::table)
                .values(rows)
                .on_conflict(schema::ls_pools::id)
                .do_nothing()
                .execute(conn)
                .await?;
        }

        Ok(())
    }
}

#[derive(Insertable, Debug)]
#[diesel(table_name = ls_events)]
pub(crate) struct TableLsEvent {
    id: String,
    pool_id: String,
    tp: LsEventType,
    version: i64,
    tx_hash: String,
    sender: String,
    even_type: serde_json::Value,
    timestamp: i64,
}

#[async_trait]
impl WriteToDb for Vec<TableLsEvent> {
    async fn write_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        for rows in self.chunks(TB_CHUNKS_SIZE) {
            diesel::insert_into(schema::ls_events::table)
                .values(rows)
                .on_conflict(schema::ls_events::id)
                .do_nothing()
                .execute(conn)
                .await?;
        }

        let up_pool: Vec<UpdatePool> = self
            .iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>>>()?;
        up_pool.write_to_db(conn).await
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

#[derive(AsChangeset)]
#[diesel(table_name = ls_pools)]
struct TableLsPoolUpdate {
    fee: Option<i64>,
    x_val: Option<BigDecimal>,
    y_val: Option<BigDecimal>,
    last_tx_version: i64,
}

#[derive(Debug)]
pub(crate) enum UpdatePool {
    // Pools.x_val += x_val
    // Pools.y_val += y_val
    Val {
        pool_id: String,
        version: i64,
        x_val: i128,
        y_val: i128,
    },
    // Pools.fee = new_fee
    Fee {
        pool_id: String,
        version: i64,
        fee: u64,
    },
    None,
}

impl UpdatePool {
    fn pool_id(&self) -> Option<&String> {
        let result = match self {
            UpdatePool::Fee { pool_id, .. } | UpdatePool::Val { pool_id, .. } => pool_id,
            UpdatePool::None => None?,
        };
        Some(result)
    }

    fn version(&self) -> Option<i64> {
        let result = match self {
            UpdatePool::Fee { version, .. } | UpdatePool::Val { version, .. } => version,
            UpdatePool::None => None?,
        };
        Some(*result)
    }
}

#[async_trait]
impl WriteToDb for Vec<UpdatePool> {
    async fn write_to_db(mut self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        use crate::schema::ls_pools;

        let pool_ids = self
            .iter()
            .filter_map(|up| up.pool_id())
            .collect::<Vec<_>>();

        if pool_ids.is_empty() {
            return Ok(());
        };

        let pools_in_db = ls_pools::table
            .select((
                ls_pools::columns::id,
                ls_pools::columns::last_tx_version,
                ls_pools::columns::x_val,
                ls_pools::columns::y_val,
            ))
            .filter(ls_pools::columns::id.eq_any(pool_ids))
            .load::<(String, i64, BigDecimal, BigDecimal)>(conn)
            .await?
            .into_iter()
            .map(|(pool_id, last_tx_version, x_val, y_val)| {
                (
                    pool_id,
                    (
                        last_tx_version,
                        bigdecimal_to_u64(&x_val) as i128,
                        bigdecimal_to_u64(&y_val) as i128,
                    ),
                )
            })
            .collect::<HashMap<_, _>>();

        // HashMap<pool_id, (x_val, y_val, fee)>
        let mut rows: HashMap<String, (i64, Option<i128>, Option<i128>, Option<u64>)> =
            HashMap::new();

        for item in self {
            let Some(pool_id) = item.pool_id() else {
                continue;
            };
            let Some(version_in_item) = item.version() else {
                continue;
            };
            let Some((version_in_db, _, _)) = pools_in_db.get(pool_id) else {
                bail!("{pool_id} was not found in the database")
            };

            if *version_in_db >= version_in_item {
                debug!("Old data {item:?}\n{version_in_db} >= {version_in_item}");
                continue;
            }

            let (version, x_val, y_val, fee) = rows.entry(pool_id.clone()).or_default();
            *version = version_in_item;

            match item {
                UpdatePool::Fee { fee: new_fee, .. } => *fee = Some(new_fee),
                UpdatePool::Val {
                    x_val: new_x_val,
                    y_val: new_y_val,
                    ..
                } => {
                    *x_val = Some(x_val.map_or_else(|| new_x_val, |old| old + new_x_val));
                    *y_val = Some(y_val.map_or_else(|| new_y_val, |old| old + new_y_val));
                },
                _ => continue,
            }
        }

        for (pool_id, (last_tx_version, x_val, y_val, fee)) in rows {
            let Some((_, old_x_val, old_y_val)) = pools_in_db.get(&pool_id) else {
                bail!("{pool_id} was not found in the database")
            };

            let set_values = TableLsPoolUpdate {
                x_val: x_val.map(|val| {
                    let val = old_x_val + val;
                    if val < 0 {
                        panic!("{pool_id}::x_val The value cannot be negative");
                    }
                    if val >= u64::MAX as i128 {
                        panic!("{pool_id}::x_val exceeds the maximum allowed value");
                    }

                    u64_to_bigdecimal(val as u64)
                }),
                y_val: y_val.map(|val| {
                    let val = old_y_val + val;
                    if val < 0 {
                        panic!("{pool_id}::y_val The value cannot be negative");
                    }
                    if val >= u64::MAX as i128 {
                        panic!("{pool_id}::y_val  exceeds the maximum allowed value");
                    }

                    u64_to_bigdecimal(val as u64)
                }),
                fee: match fee {
                    Some(val) => Some(i64::try_from(val)?),
                    None => None,
                },
                last_tx_version,
            };

            diesel::update(ls_pools::table.filter(ls_pools::columns::id.eq(pool_id.clone())))
                .set(set_values)
                .execute(conn)
                .await?;
        }

        Ok(())
    }
}

impl TryFrom<&TableLsEvent> for UpdatePool {
    type Error = anyhow::Error;

    fn try_from(event: &TableLsEvent) -> std::prelude::v1::Result<Self, Self::Error> {
        let op = match event.tp {
            // When liquidity added to the pool.
            LsEventType::LiquidityAddedEvent => {
                let [added_x_val, added_y_val] = ["added_x_val", "added_y_val"].map(|index| {
                    event
                        .even_type
                        .get(index)
                        .and_then(|v| v.as_str())
                        .ok_or(anyhow!(
                            "The value `LiquidityAddedEvent::{index}` was not found"
                        ))
                        .and_then(|value| value.parse::<i128>().map_err(anyhow::Error::from))
                });

                // Pools.x_val += added_x_val
                // Pools.y_val += added_y_val
                UpdatePool::Val {
                    pool_id: event.pool_id.clone(),
                    version: event.version,
                    x_val: added_x_val?,
                    y_val: added_y_val?,
                }
            },
            // When oracle updated (i don't think we need to catch it).
            LsEventType::OracleUpdatedEvent => {
                let [_last_price_x_cumulative, _last_price_y_cumulative] =
                    ["last_price_x_cumulative", "last_price_y_cumulative"].map(|index| {
                        event
                            .even_type
                            .get(index)
                            .and_then(|v| v.as_str())
                            .ok_or(anyhow!(
                                "The value `OracleUpdatedEvent::{index}` was not found"
                            ))
                            .and_then(|value| value.parse::<u128>().map_err(anyhow::Error::from))
                    });

                UpdatePool::None
            },
            // When swap happened.
            LsEventType::SwapEvent => {
                let [x_in, x_out, y_in, y_out] = ["x_in", "x_out", "y_in", "y_out"].map(|index| {
                    event
                        .even_type
                        .get(index)
                        .and_then(|v| v.as_str())
                        .ok_or(anyhow!("The value `SwapEvent::{index}` was not found"))
                        .and_then(|value| value.parse::<i128>().map_err(anyhow::Error::from))
                });

                // Pools.x_val += x_in - x_out
                // Pools.y_val += y_in - y_out
                UpdatePool::Val {
                    pool_id: event.pool_id.clone(),
                    version: event.version,
                    x_val: x_in? - x_out?,
                    y_val: y_in? - y_out?,
                }
            },
            // When liquidity removed from the pool.
            LsEventType::LiquidityRemovedEvent => {
                let [_lp_tokens_burned, returned_x_val, returned_y_val] =
                    ["lp_tokens_burned", "returned_x_val", "returned_y_val"].map(|index| {
                        event
                            .even_type
                            .get(index)
                            .and_then(|v| v.as_str())
                            .ok_or(anyhow!(
                                "The value `LiquidityRemovedEvent::{index}` was not found"
                            ))
                            .and_then(|value| value.parse::<i128>().map_err(anyhow::Error::from))
                    });

                // Pools.x_val -= returned_x_val
                // Pools.y_val -= returned_y_val
                UpdatePool::Val {
                    pool_id: event.pool_id.clone(),
                    version: event.version,
                    x_val: -returned_x_val?,
                    y_val: -returned_y_val?,
                }
            },
            // When flashloan event happened.
            LsEventType::FlashloanEvent => {
                let [x_in, x_out, y_in, y_out] = ["x_in", "x_out", "y_in", "y_out"].map(|index| {
                    event
                        .even_type
                        .get(index)
                        .and_then(|v| v.as_str())
                        .ok_or(anyhow!("The value `FlashloanEvent::{index}` was not found"))
                        .and_then(|value| value.parse::<i128>().map_err(anyhow::Error::from))
                });

                // Pools.x_val += x_in - x_out
                // Pools.y_val += y_in - y_out
                UpdatePool::Val {
                    pool_id: event.pool_id.clone(),
                    version: event.version,
                    x_val: x_in? - x_out?,
                    y_val: y_in? - y_out?,
                }
            },
            // When fee of pool updated.
            LsEventType::UpdateFeeEvent => {
                let fee = event
                    .even_type
                    .get("new_fee")
                    .and_then(|v| v.as_str())
                    .ok_or(anyhow!("The value `UpdateFeeEvent::new_fee` was not found"))
                    .and_then(|value| value.parse::<u64>().map_err(anyhow::Error::from))?;

                // Pools.fee = new_fee
                UpdatePool::Fee {
                    pool_id: event.pool_id.clone(),
                    version: event.version,
                    fee,
                }
            },
            // When DAO fee updated for the pool.
            LsEventType::UpdateDAOFeeEvent => {
                let fee = event
                    .even_type
                    .get("new_fee")
                    .and_then(|v| v.as_str())
                    .ok_or(anyhow!(
                        "The value `UpdateDAOFeeEvent::new_fee` was not found"
                    ))
                    .and_then(|value| value.parse::<u64>().map_err(anyhow::Error::from))?;

                // Pools.fee = new_fee
                UpdatePool::Fee {
                    pool_id: event.pool_id.clone(),
                    version: event.version,
                    fee,
                }
            },
            _ => unimplemented!(),
        };

        Ok(op)
    }
}
