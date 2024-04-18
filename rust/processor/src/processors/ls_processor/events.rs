use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use aptos_protos::transaction::v1::{Event, Transaction};

use serde::Deserialize;
use tonic::async_trait;

use crate::{
    processors::ls_processor::{
        db::{InsertToDb, LsEventType, TableLsEvent, TableLsPool},
        info::PoolResourceFromTx,
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

        let result = it
            .map(|ev| LsEvent::try_from_ev_tx(ev, transaction))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok(result)
    }

    fn try_from_ev_tx(
        (version_ls, ev_ls): (&String, &Event),
        tx: &Transaction,
    ) -> Result<Vec<LsEvent>> {
        let mv_st = ev_ls.move_struct().ok_or(anyhow!("expected Move Struct"))?;
        let pool_type = mv_st.pool_type()?;

        // Searching for new values for the pool
        let pool_row = tx
            .pool_row_from_resources(
                mv_st,
                version_ls,
                tx.version.try_into().context("tx version")?,
            )?
            .map(LsEvent::Pools)
            .map(|v| vec![v]);

        let event_type = LsEventType::from_str(&mv_st.name)?;

        match event_type {
            // When new pool created.
            LsEventType::PoolCreatedEvent => Ok(pool_row.unwrap_or_else(|| {
                vec![LsEvent::Pools(TableLsPool {
                    version_ls: version_ls.clone(),
                    ..(&pool_type).into()
                })]
            })),

            LsEventType::LiquidityAddedEvent
            | LsEventType::OracleUpdatedEvent
            | LsEventType::SwapEvent
            | LsEventType::LiquidityRemovedEvent
            | LsEventType::FlashloanEvent
            | LsEventType::UpdateFeeEvent
            | LsEventType::UpdateDAOFeeEvent
            | LsEventType::CoinDepositedEvent => {
                let TxInfoForLs {
                    version,
                    tx_hash,
                    timestamp,
                    sender,
                } = tx.info().ok_or(anyhow!(
                    "Not all data could be extracted from the transaction"
                ))?;

                let even_type = ev_ls.data_value()?;
                let mut data: ObjEventType = serde_json::from_value(even_type.clone())
                    .map_err(|err| anyhow!("{err:?}\n{even_type:?}"))?;

                data = match event_type {
                    LsEventType::UpdateFeeEvent => ObjEventType::UpdateFee {
                        new_fee: data.fee().with_context(|| {
                            format!("fee not found. event_type: {event_type:?}. data: {data:?}")
                        })?,
                    },
                    LsEventType::UpdateDAOFeeEvent => ObjEventType::UpdateDaoFee {
                        new_fee: data.fee().with_context(|| {
                            format!("dao fee not found. event_type: {event_type:?}. data: {data:?}")
                        })?,
                    },
                    _ => data,
                };

                let EventVal {
                    x_val,
                    y_val,
                    fee,
                    dao_fee,
                } = data.get_val()?;

                let event_row = LsEvent::Events(TableLsEvent {
                    id: ev_ls.key()? + "_" + &ev_ls.sequence_number.to_string(),
                    pool_id: pool_type.hash(),
                    tp: event_type,
                    event: even_type,
                    timestamp,
                    tx_hash,
                    sender,
                    version,
                    x_val: x_val.map(|v| v.into()),
                    y_val: y_val.map(|v| v.into()),
                    fee,
                    dao_fee,
                    sq: None,
                });

                let result = match pool_row {
                    Some(mut result) => {
                        result.push(event_row);
                        result
                    },
                    None => vec![event_row],
                };
                Ok(result)
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
    UpdateFee {
        new_fee: String,
    },
    UpdateDaoFee {
        new_fee: String,
    },
    CoinDepositedEvent {
        x_val: String,
        y_val: String,
    },
}

impl ObjEventType {
    pub(crate) fn get_val(&self) -> Result<EventVal> {
        let mut result = EventVal::default();

        match self {
            ObjEventType::Added {
                added_x_val,
                added_y_val,
                ..
            } => {
                result.x_val = Some(added_x_val.parse::<i128>()?);
                result.y_val = Some(added_y_val.parse::<i128>()?);
            },
            ObjEventType::Swap {
                x_in,
                y_in,
                x_out,
                y_out,
            } => {
                result.x_val = Some(x_in.parse::<i128>()? - x_out.parse::<i128>()?);
                result.y_val = Some(y_in.parse::<i128>()? - y_out.parse::<i128>()?);
            },
            ObjEventType::Return {
                returned_x_val,
                returned_y_val,
                ..
            } => {
                result.x_val = Some(-returned_x_val.parse::<i128>()?);
                result.y_val = Some(-returned_y_val.parse::<i128>()?);
            },
            ObjEventType::CoinDepositedEvent { x_val, y_val, .. } => {
                result.x_val = Some(-x_val.parse::<i128>()?);
                result.y_val = Some(-y_val.parse::<i128>()?);
            },
            ObjEventType::Last { .. } => {},
            ObjEventType::UpdateFee { new_fee } => {
                result.fee = Some(new_fee.parse()?);
            },
            ObjEventType::UpdateDaoFee { new_fee } => {
                result.dao_fee = Some(new_fee.parse()?);
            },
        }

        Ok(result)
    }

    /// @return: fee | dao_fee
    pub(crate) fn fee(&self) -> Option<String> {
        match self {
            ObjEventType::UpdateFee { new_fee } | ObjEventType::UpdateDaoFee { new_fee } => {
                Some(new_fee.clone())
            },
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct EventVal {
    x_val: Option<i128>,
    y_val: Option<i128>,
    fee: Option<i64>,
    dao_fee: Option<i64>,
}
