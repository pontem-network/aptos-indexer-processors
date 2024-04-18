use std::{collections::HashMap, str::FromStr};

use anyhow::{bail, Result};
use bigdecimal::BigDecimal;
use diesel::{
    deserialize::Queryable, query_dsl::methods::FilterDsl, ExpressionMethods, Insertable,
    Selectable,
};
use diesel_async::RunQueryDsl;
use tonic::async_trait;
use tracing::info;

use crate::{
    schema::{self, ls_events, ls_pools},
    utils::database::PgPoolConnection,
};

use super::mv::PoolType;

// Write 100 values at a time to the table
const TB_CHUNKS_SIZE: usize = 100;

#[async_trait]
pub(crate) trait InsertToDb {
    async fn insert_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()>;
}

#[derive(Selectable, Queryable, QueryId, AsChangeset, Insertable, Debug, Clone)]
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
    pub dao_fee: i64,
    pub last_version: i64,
}

impl From<&PoolType> for TableLsPool {
    fn from(pool_type: &PoolType) -> Self {
        TableLsPool {
            id: pool_type.hash(),
            version_ls: "".to_string(),
            x_name: pool_type.x_name.clone(),
            y_name: pool_type.y_name.clone(),
            curve: pool_type.curve.clone(),
            x_val: BigDecimal::from(0),
            y_val: BigDecimal::from(0),
            fee: 0,
            dao_fee: 0,
            last_version: 0,
        }
    }
}

impl From<PoolType> for TableLsPool {
    fn from(pool_type: PoolType) -> Self {
        TableLsPool {
            id: pool_type.hash(),
            version_ls: "".to_string(),
            x_name: pool_type.x_name,
            y_name: pool_type.y_name,
            curve: pool_type.curve,
            x_val: BigDecimal::from(0),
            y_val: BigDecimal::from(0),
            fee: 0,
            dao_fee: 0,
            last_version: 0,
        }
    }
}

#[async_trait]
impl InsertToDb for Vec<TableLsPool> {
    async fn insert_to_db(mut self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        self.group();

        let mut updated_count = 0;
        let count = self.len();

        for pool in &self {
            updated_count += diesel::insert_into(schema::ls_pools::table)
                .values(pool)
                .on_conflict(schema::ls_pools::id)
                .do_update()
                .set((
                    schema::ls_pools::x_val.eq(&pool.x_val),
                    schema::ls_pools::y_val.eq(&pool.y_val),
                    schema::ls_pools::fee.eq(&pool.fee),
                    schema::ls_pools::dao_fee.eq(&pool.dao_fee),
                    schema::ls_pools::last_version.eq(&pool.last_version),
                ))
                .filter(schema::ls_pools::last_version.lt(&pool.last_version))
                .execute(conn)
                .await?;
        }
        info!("{count}:{updated_count} TableLsPool added/updated");

        Ok(())
    }
}

trait GroupPool {
    fn group(&mut self);
}

impl GroupPool for Vec<TableLsPool> {
    fn group(&mut self) {
        // Selecting the latest values
        let mut m = self
            .iter()
            .map(|v| (&v.id, &v.last_version))
            .collect::<Vec<_>>();
        m.sort_by(|(_, a), (_, b)| a.cmp(b));
        let max: HashMap<String, i64> = m
            .into_iter()
            .collect::<HashMap<_, _>>()
            .into_iter()
            .map(|(id, version)| (id.clone(), *version))
            .collect();

        self.retain(|pool| {
            let max_version = match max.get(&pool.id) {
                Some(varsion) => varsion,
                None => return true,
            };
            &pool.last_version == max_version
        });

        // Remove duplicates
        let unique = self
            .iter()
            .map(|pool| (&pool.id, pool))
            .collect::<HashMap<_, _>>();

        *self = unique.into_values().cloned().collect();
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
    pub event: serde_json::Value,
    pub timestamp: i64,
    pub x_val: Option<BigDecimal>,
    pub y_val: Option<BigDecimal>,
    pub fee: Option<i64>,
    pub dao_fee: Option<i64>,
    pub sq: Option<i64>,
}

#[async_trait]
impl InsertToDb for Vec<TableLsEvent> {
    async fn insert_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        for rows in self.chunks(TB_CHUNKS_SIZE) {
            let count = diesel::insert_into(schema::ls_events::table)
                .values(rows)
                .on_conflict(schema::ls_events::id)
                .do_nothing()
                .execute(conn)
                .await?;

            info!("{from}:{count} TableLsEvent added", from = rows.len());
        }

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

    CoinDepositedEvent,
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
            "CoinDepositedEvent" => LsEventType::CoinDepositedEvent,
            _ => bail!("Unknown event"),
        };
        Ok(result)
    }
}
