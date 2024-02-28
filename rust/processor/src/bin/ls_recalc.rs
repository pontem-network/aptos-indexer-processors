use anyhow::{Context, Result};
use bigdecimal::{BigDecimal, Zero};
use clap::Parser;
use diesel::dsl::{count, sum};
use serde::Deserialize;
use tracing::{debug, info};

use diesel::QueryDsl;
use diesel::{BoolExpressionMethods, ExpressionMethods};
use diesel_async::RunQueryDsl;

use processor::{
    processors::ls_processor::db::TableLsPool,
    schema::{ls_events, ls_pools},
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

    info!("checking and fixing pools");

    let pools: Vec<TableLsPool> = ls_pools::table.load(&mut conn).await?;

    let all_count = pools.len();
    for (num, pool) in pools.into_iter().enumerate() {
        println!("{all_count}: {num}");
        fix(&mut conn, pool).await?;
    }

    Ok(())
}

async fn fix(conn: &mut PgPoolConnection<'_>, pool: TableLsPool) -> Result<()> {
    let count: i64 = ls_events::table
        .select(count(ls_events::id))
        .filter(ls_events::pool_id.eq(&pool.id))
        .first(conn)
        .await?;

    info!("{} Rows: {count}", &pool.id);

    let fee: Option<i64> = ls_events::table
        .select(ls_events::fee)
        .filter(
            ls_events::pool_id
                .eq(&pool.id)
                .and(ls_events::fee.is_not_null())
                .and(ls_events::fee.ne(0)),
        )
        .order(ls_events::version.desc())
        .first(conn)
        .await
        .ok()
        .and_then(|v| v);

    debug!("{fee:?}");

    let (x_val, y_val): (Option<BigDecimal>, Option<BigDecimal>) = ls_events::table
        .select((sum(ls_events::x_val), sum(ls_events::y_val)))
        .filter(
            ls_events::pool_id.eq(&pool.id).and(
                ls_events::x_val
                    .is_not_null()
                    .and(ls_events::x_val.ne(BigDecimal::zero()))
                    .or(ls_events::y_val
                        .is_not_null()
                        .and(ls_events::y_val.ne(BigDecimal::zero()))),
            ),
        )
        .first(conn)
        .await?;

    debug!("{x_val:?}");
    debug!("{y_val:?}");

    info!("Saving new values {}", pool.id);
    diesel::update(ls_pools::table.filter(ls_pools::id.eq(&pool.id)))
        .set((
            ls_pools::x_val.eq(x_val.unwrap_or_default()),
            ls_pools::y_val.eq(y_val.unwrap_or_default()),
            ls_pools::fee.eq(fee.unwrap_or_default()),
        ))
        .execute(conn)
        .await?;

    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
enum ObjEventType {
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
