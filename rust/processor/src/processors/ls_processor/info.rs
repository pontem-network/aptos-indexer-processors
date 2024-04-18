use crate::processors::ls_processor::mv::MoveStructTagLs;

use super::db::TableLsPool;
use anyhow::{Context, Result};
use aptos_protos::transaction::v1::{
    write_set_change::Change, MoveStructTag, MoveType, Transaction,
};

use serde::Deserialize;

const MODULE_NAME: &str = "liquidity_pool";
const RESOURCE_NAME: &str = "LiquidityPool";

pub(crate) trait PoolResourceFromTx {
    fn pool_resource_data_as_str(&self, address: &str, gen_tp: &[MoveType]) -> Option<&String>;

    fn pool_row_from_resources(
        &self,
        mv_st: &MoveStructTag,
        ls_version: &str,
        tx_version: i64,
    ) -> Result<Option<TableLsPool>>;
}

impl PoolResourceFromTx for &Transaction {
    fn pool_resource_data_as_str(&self, address: &str, gen_tp: &[MoveType]) -> Option<&String> {
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
                    && tp.module == MODULE_NAME
                    && tp.name == RESOURCE_NAME
                    && tp.generic_type_params == gen_tp)
                    .then_some(&ch.data)
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

        let pool_resource_str = match self.pool_resource_data_as_str(address, gen_tp) {
            Some(resource) => resource,
            None => return Ok(None),
        };

        let pool_resource: PoolResource = serde_json::from_str(pool_resource_str)
            .with_context(|| format!("Data: {pool_resource_str}"))?;

        let pool_row = TableLsPool {
            x_val: pool_resource.x_val()?.into(),
            y_val: pool_resource.y_val()?.into(),
            fee: pool_resource.fee()?,
            dao_fee: pool_resource.dao_fee()?,
            last_version: tx_version,
            version_ls: version_ls.to_string(),
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
