use anyhow::{anyhow, ensure};
use anyhow::{Context, Result};
use bigdecimal::{BigDecimal, ToPrimitive};
use clap::Parser;
use diesel::dsl::count;
use serde::Deserialize;
use tracing::{debug, error, info};

use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::RunQueryDsl;

use processor::{
    processors::ls_processor::db::{TableLsEvent, TableLsPool},
    schema::{ls_events, ls_pools},
    utils::database::{new_db_pool, PgPoolConnection},
    IndexerGrpcProcessorConfig,
};
use server_framework::{load, setup_logging, GenericConfig, ServerArgs};

#[derive(Debug, Parser)]
struct Cmd {
    #[clap(short, long)]
    fix: bool,

    #[clap(flatten)]
    server_args: ServerArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set up the server.
    setup_logging();

    info!("parsing arguments");
    let args = Cmd::parse();

    info!("loading configurations");
    let config = load::<GenericConfig<IndexerGrpcProcessorConfig>>(&args.server_args.config_path)?;

    info!("creating a database connection");
    let conn_pool = new_db_pool(
        &config.server_config.postgres_connection_string,
        config.server_config.db_pool_size,
    )
    .await
    .context("Failed to create connection pool")?;

    let mut conn: PgPoolConnection = conn_pool.get().await?;

    info!("checking and fixing pools");

    let mut pools: Vec<TableLsPool> = ls_pools::table.load(&mut conn).await?;

    for pool in &mut pools {
        fix(&mut conn, pool, args.fix).await?;
    }

    Ok(())
}

async fn fix(conn: &mut PgPoolConnection<'_>, pool: &mut TableLsPool, fix: bool) -> Result<()> {
    debug!("CHECKING {} ...", &pool.id);

    const LIMIT: i64 = 5000;

    let count: i64 = ls_events::table
        .select(count(ls_events::id))
        .filter(ls_events::pool_id.eq(&pool.id))
        .first(conn)
        .await?;
    let (mut x, mut y, mut fee) = (0, 0, 0);
    for page in 0.. {
        let events: Vec<TableLsEvent> = ls_events::table
            .filter(ls_events::pool_id.eq(&pool.id))
            .order((ls_events::version.asc(), ls_events::id.asc()))
            .limit(LIMIT)
            .offset(page * LIMIT)
            .load(conn)
            .await?;
        println!("{count}: {}", events.len() + (LIMIT * page) as usize);

        let (n_x, n_y, n_fee) = calc_events(pool, &events)?;
        x += n_x;
        y += n_y;
        fee = n_fee;

        if events.len() != LIMIT as usize {
            break;
        }
    }

    let expected_x: i128 = pool
        .x_val
        .to_i128()
        .ok_or(anyhow!("pool.x_val.to_i128() empty"))?;
    let expected_y: i128 = pool
        .y_val
        .to_i128()
        .ok_or(anyhow!("pool.y_val.to_i128() empty"))?;
    let expected_fee = pool.fee;

    if x != expected_x || y != expected_y || fee != expected_fee {
        let error = format!(
            "({x}!={expected_x} || {y}!={expected_y} || {fee} != {expected_fee}) pool_id: {}",
            &pool.id
        );
        error!("{error}");

        pool.x_val = BigDecimal::from(x);
        pool.y_val = BigDecimal::from(y);
        pool.fee = fee;

        ensure!(fix, "{error}");

        info!("Saving new values {}", pool.id);
        pool.save(conn).await?;
    }

    info!("SUCCESS: {}", &pool.id);

    Ok(())
}

// x_val, y_val, fee
fn calc_events(pool: &TableLsPool, events: &Vec<TableLsEvent>) -> Result<(i128, i128, i64)> {
    let (mut x, mut y, mut fee): (i128, i128, i64) = (0, 0, 0);

    for event in events {
        if pool.last_tx_version < event.version {
            continue;
        }

        let data: ObjEventType = serde_json::from_value(event.even_type.clone())
            .map_err(|err| anyhow!("{err:?}\n{:?}", event.even_type))?;

        match data {
            ObjEventType::Added {
                added_x_val,
                added_y_val,
                ..
            } => {
                x += added_x_val.parse::<i128>()?;
                y += added_y_val.parse::<i128>()?;
            },
            ObjEventType::Swap {
                x_in,
                y_in,
                x_out,
                y_out,
            } => {
                x += x_in.parse::<i128>().unwrap() - x_out.parse::<i128>().unwrap();
                y += y_in.parse::<i128>().unwrap() - y_out.parse::<i128>().unwrap();
            },
            ObjEventType::Return {
                returned_x_val,
                returned_y_val,
                ..
            } => {
                x -= returned_x_val.parse::<i128>().unwrap();
                y -= returned_y_val.parse::<i128>().unwrap();
            },
            ObjEventType::Last { .. } => {
                continue;
            },
            ObjEventType::NewFee { new_fee } => {
                fee = new_fee.parse().unwrap();
            },
        }
    }

    Ok((x, y, fee))
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
