use std::str::FromStr;

use anyhow::{anyhow, Result};
use aptos_protos::transaction::v1::{Event, Transaction};
use bigdecimal::BigDecimal;
use serde::Deserialize;
use tonic::async_trait;

use crate::{
    processors::ls_processor::{
        db::{InsertToDb, LsEventType, TableLsEvent, TableLsPool},
        mv::{filter_ls_events, EventLs, MoveStructTagLs, TransactionLs, TxInfoForLs},
    },
    utils::database::PgPoolConnection,
};

#[derive(Debug)]
pub(crate) enum LsEvent {
    Pools(TableLsPool),
    Events(TableLsEvent),
}

impl LsEvent {
    pub(crate) fn try_from_txs(
        addresses: &[(String, String)],
        transactions: &[Transaction],
    ) -> Result<Vec<LsEvent>> {
        Ok(transactions
            .iter()
            .map(|tx| LsEvent::try_from_tx(addresses, tx))
            .collect::<Result<Vec<Vec<_>>>>()?
            .into_iter()
            .flatten()
            .collect())
    }

    fn try_from_tx(
        addresses: &[(String, String)],
        transaction: &Transaction,
    ) -> Result<Vec<LsEvent>> {
        let it = match filter_ls_events(addresses, transaction) {
            Some(it) => it,
            None => return Ok(Vec::default()),
        };

        it.map(|ev| LsEvent::try_from_ev_tx(ev, transaction))
            .collect::<Result<Vec<_>>>()
    }

    fn try_from_ev_tx((version, ev): (&String, &Event), tx: &Transaction) -> Result<LsEvent> {
        let mv_st = ev.move_struct().ok_or(anyhow!("expected Move Struct"))?;
        let event_type = LsEventType::from_str(&mv_st.name)?;

        match event_type {
            // When new pool created.
            LsEventType::PoolCreatedEvent => {
                let pool_type = mv_st.pool_type()?;

                Ok(LsEvent::Pools(TableLsPool {
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
                let TxInfoForLs {
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

                Ok(LsEvent::Events(TableLsEvent {
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
impl InsertToDb for Vec<LsEvent> {
    async fn insert_to_db(self, conn: &mut PgPoolConnection<'_>) -> Result<()> {
        let (pools, events): (Vec<_>, Vec<_>) = self
            .into_iter()
            .partition(|row| matches!(row, LsEvent::Pools(..)));

        let pools = pools
            .into_iter()
            .filter_map(|ls_db| match ls_db {
                LsEvent::Pools(pools) => Some(pools),
                _ => None,
            })
            .collect::<Vec<_>>();
        pools.insert_to_db(conn).await?;

        let events = events
            .into_iter()
            .filter_map(|ls_db| match ls_db {
                LsEvent::Events(events) => Some(events),
                _ => None,
            })
            .collect::<Vec<_>>();
        events.insert_to_db(conn).await?;

        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
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
    pub(crate) fn get_val(&self) -> Result<(Option<i128>, Option<i128>, Option<i64>)> {
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