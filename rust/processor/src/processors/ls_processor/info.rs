use crate::processors::ls_processor::mv::MoveStructTagLs;

use super::db::TableLsPool;
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::{
    move_type::Content, write_set_change::Change, MoveStructTag, MoveType, Transaction,
};

use serde::Deserialize;

const LS_POOL_MODULE_NAME: &str = "liquidity_pool";
const LS_POOL_RESOURCE_NAME: &str = "LiquidityPool";
const COIN_INFO_ADDRESS: &str = "0x1";
const COIN_INFO_MODULE_NAME: &str = "coin";
const COIN_INFO_RESOURCE_NAME: &str = "CoinInfo";

pub(crate) trait PoolResourceFromTx {
    fn pool_resource_as_str(&self, address: &str, gen_tp: &[MoveType]) -> Option<&String>;

    fn pool_coin_info_as_str(&self, gen_tp: &[MoveType]) -> Option<&String>;

    fn pool_row_from_resources(
        &self,
        mv_st: &MoveStructTag,
        ls_version: &str,
        tx_version: i64,
    ) -> Result<Option<TableLsPool>>;
}

impl PoolResourceFromTx for &Transaction {
    fn pool_resource_as_str(&self, address: &str, gen_tp: &[MoveType]) -> Option<&String> {
        self.info
            .as_ref()?
            .changes
            .iter()
            .filter_map(|ch| ch.change.as_ref())
            .filter_map(|ch| match ch {
                Change::WriteResource(r) => Some(r),
                _ => None,
            })
            .filter_map(|ch| {
                let tp = ch.r#type.as_ref()?;
                (tp.address == address
                    && tp.module == LS_POOL_MODULE_NAME
                    && tp.name == LS_POOL_RESOURCE_NAME
                    && tp.generic_type_params == gen_tp)
                    .then_some(&ch.data)
            })
            .last()
    }

    fn pool_coin_info_as_str(&self, gen_tp: &[MoveType]) -> Option<&String> {
        self.info
            .as_ref()?
            .changes
            .iter()
            .filter_map(|ch| ch.change.as_ref())
            .filter_map(|ch| match ch {
                Change::WriteResource(r) => Some(r),
                _ => None,
            })
            .filter_map(|resource| {
                let type_resource = resource.r#type.as_ref()?;
                let gtp = &match type_resource
                    .generic_type_params
                    .first()?
                    .content
                    .as_ref()?
                {
                    Content::Struct(v) => Some(v),
                    _ => None,
                }?
                .generic_type_params;

                (type_resource.address == COIN_INFO_ADDRESS
                    && type_resource.module == COIN_INFO_MODULE_NAME
                    && type_resource.name == COIN_INFO_RESOURCE_NAME
                    && gtp == gen_tp)
                    .then_some(&resource.data)
            })
            .last()
    }

    /// Searching for new values for the pool
    fn pool_row_from_resources(
        &self,
        mv_st: &MoveStructTag,
        version_ls: &str,
        tx_version: i64,
    ) -> Result<Option<TableLsPool>> {
        let address = &mv_st.address;
        let gen_tp = &mv_st.generic_type_params;

        let pool_resource: PoolResource = match self.pool_resource_as_str(address, gen_tp) {
            Some(pool_resource_str) => serde_json::from_str(pool_resource_str)
                .with_context(|| format!("Data: {pool_resource_str}"))?,
            None => return Ok(None),
        };

        let coin_decimal = match self.pool_coin_info_as_str(gen_tp) {
            Some(str) => Some(
                serde_json::from_str::<PoolCoinInfo>(str)
                    .with_context(|| format!("Data: {str}"))?
                    .decimals,
            ),
            None => None,
        };

        let pool_row = TableLsPool {
            x_val: pool_resource.x_val()?.into(),
            y_val: pool_resource.y_val()?.into(),
            fee: pool_resource.fee()?,
            dao_fee: pool_resource.dao_fee()?,
            last_version: tx_version,
            version_ls: version_ls.to_string(),
            coin_decimal,
            ..mv_st.pool_type()?.into()
        };

        Ok(Some(pool_row))
    }
}

#[derive(Debug, Deserialize)]
struct PoolResource {
    coin_x_reserve: PoolResourceValue,
    coin_y_reserve: PoolResourceValue,
    dao_fee: String,
    fee: String,
}

impl PoolResource {
    fn fee(&self) -> Result<i64> {
        self.fee
            .parse()
            .with_context(|| format!("fee value: {}", &self.fee))
    }

    fn dao_fee(&self) -> Result<i64> {
        self.dao_fee
            .parse()
            .with_context(|| format!("dao_fee value: {}", &self.fee))
    }

    fn x_val(&self) -> Result<i128> {
        self.coin_x_reserve
            .value
            .parse()
            .with_context(|| format!("coin_x_reserve value: {}", &self.coin_x_reserve.value))
    }

    fn y_val(&self) -> Result<i128> {
        self.coin_y_reserve
            .value
            .parse()
            .with_context(|| format!("coin_y_reserve value: {}", &self.coin_y_reserve.value))
    }
}

#[derive(Debug, Deserialize)]
struct PoolResourceValue {
    value: String,
}

#[derive(Debug, Deserialize)]
struct PoolCoinInfo {
    decimals: i64,
}
