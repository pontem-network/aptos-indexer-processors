use anyhow::anyhow;
use anyhow::{Context, Result};
use bigdecimal::{BigDecimal, Zero};
use clap::Parser;
use diesel::dsl::count;
use processor::processors::ls_processor::db::{LsEventType, ObjEventType};
use tonic::async_trait;
use tracing::info;

use diesel::QueryDsl;
use diesel::{BoolExpressionMethods, ExpressionMethods};
use diesel_async::RunQueryDsl;

use processor::{
    processors::ls_processor::db::TableLsEvent,
    schema::ls_events,
    utils::database::{new_db_pool, PgPoolConnection},
    IndexerGrpcProcessorConfig,
};
use server_framework::{load, setup_logging, GenericConfig, ServerArgs};

#[tokio::main]
async fn main() -> Result<()> {
    // Set up the server.
    setup_logging();

    info!("parsing arguments");
    let args = ServerArgs::parse();

    info!("loading configurations");
    let config = load::<GenericConfig<IndexerGrpcProcessorConfig>>(&args.config_path)?;

    info!("creating a database connection");
    let conn_pool = new_db_pool(
        &config.server_config.postgres_connection_string,
        config.server_config.db_pool_size,
    )
    .await
    .context("Failed to create connection pool")?;

    let mut conn: PgPoolConnection = conn_pool.get().await?;

    const LIMIT: i64 = 1000;

    for tp in [NotIndexed::Fee, NotIndexed::Val] {
        let count = tp.count(&mut conn).await?;

        let end_page = (count as f64 / LIMIT as f64).ceil() as i64;

        for page in 0..end_page {
            let offset = page * LIMIT;

            println!("{count} | {offset}");

            let events = tp.get(&mut conn, offset, LIMIT).await?;
            events.update(&mut conn).await?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum NotIndexed {
    Val,
    Fee,
}

impl NotIndexed {
    async fn count(self, conn: &mut PgPoolConnection<'_>) -> Result<i64> {
        let query = ls_events::table.select(count(ls_events::id));
        let count = match self {
            NotIndexed::Fee => {
                query
                    .filter(
                        ls_events::fee.eq(0).and(
                            ls_events::tp
                                .eq(LsEventType::UpdateFeeEvent)
                                .or(ls_events::tp.eq(LsEventType::UpdateDAOFeeEvent)),
                        ),
                    )
                    .first(conn)
                    .await?
            },
            NotIndexed::Val => {
                query
                    .filter(
                        ls_events::x_val
                            .eq(BigDecimal::zero())
                            .and(ls_events::y_val.eq(BigDecimal::zero()))
                            .and(
                                ls_events::tp
                                    .eq(LsEventType::LiquidityAddedEvent)
                                    .or(ls_events::tp.eq(LsEventType::SwapEvent))
                                    .or(ls_events::tp.eq(LsEventType::LiquidityRemovedEvent))
                                    .or(ls_events::tp.eq(LsEventType::FlashloanEvent)),
                            ),
                    )
                    .first(conn)
                    .await?
            },
        };

        Ok(count)
    }

    async fn get(
        &self,
        conn: &mut PgPoolConnection<'_>,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<TableLsEvent>> {
        let list = match self {
            NotIndexed::Fee => {
                ls_events::table
                    .filter(
                        ls_events::fee.eq(0).and(
                            ls_events::tp
                                .eq(LsEventType::UpdateFeeEvent)
                                .or(ls_events::tp.eq(LsEventType::UpdateDAOFeeEvent)),
                        ),
                    )
                    .order((ls_events::version.asc(), ls_events::id.asc()))
                    .limit(limit)
                    .offset(offset)
                    .load(conn)
                    .await?
            },
            NotIndexed::Val => {
                ls_events::table
                    .filter(
                        ls_events::x_val
                            .eq(BigDecimal::zero())
                            .and(ls_events::y_val.eq(BigDecimal::zero()))
                            .and(
                                ls_events::tp
                                    .eq(LsEventType::LiquidityAddedEvent)
                                    .or(ls_events::tp.eq(LsEventType::SwapEvent))
                                    .or(ls_events::tp.eq(LsEventType::LiquidityRemovedEvent))
                                    .or(ls_events::tp.eq(LsEventType::FlashloanEvent)),
                            ),
                    )
                    .order((ls_events::version.asc(), ls_events::id.asc()))
                    .limit(limit)
                    .offset(offset)
                    .load(conn)
                    .await?
            },
        };

        Ok(list)
    }
}

#[async_trait]
trait UpdateEvent {
    async fn update(mut self, conn: &mut PgPoolConnection<'_>) -> Result<()>;
}

#[async_trait]
impl UpdateEvent for Vec<TableLsEvent> {
    async fn update(mut self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        for event in self {
            event.update(conn).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl UpdateEvent for TableLsEvent {
    async fn update(mut self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        let data: ObjEventType = serde_json::from_value(self.even_type.clone())
            .map_err(|err| anyhow!("{err:?}\n{:?}", self.even_type))?;

        let (mut x, mut y, mut fee): (i128, i128, i64) = (0, 0, 0);

        match data {
            ObjEventType::Added {
                added_x_val,
                added_y_val,
                ..
            } => {
                x = added_x_val.parse::<i128>()?;
                y = added_y_val.parse::<i128>()?;
            },
            ObjEventType::Swap {
                x_in,
                y_in,
                x_out,
                y_out,
            } => {
                x = x_in.parse::<i128>()? - x_out.parse::<i128>()?;
                y = y_in.parse::<i128>()? - y_out.parse::<i128>()?;
            },
            ObjEventType::Return {
                returned_x_val,
                returned_y_val,
                ..
            } => {
                x = returned_x_val.parse::<i128>()?;
                y = returned_y_val.parse::<i128>()?;
            },
            ObjEventType::Last { .. } => {},
            ObjEventType::NewFee { new_fee } => {
                fee = new_fee.parse()?;
            },
        }

        self.x_val = x.into();
        self.y_val = y.into();
        self.fee = fee;

        self.update_to_db(conn).await
    }
}
