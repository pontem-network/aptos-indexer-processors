use std::str::FromStr;

use anyhow::{anyhow, Result};
use aptos_protos::transaction::v1::{
    move_type::{Content, ReferenceType},
    transaction::{TransactionType, TxnData},
    Event, MoveStructTag, MoveType, Transaction, UserTransaction,
};

use super::db::{sha256_from_str, LsEventType};

pub(crate) const LS_MODULE: &str = "liquidity_pool";
const USER_TX: i32 = TransactionType::User as i32;

#[inline]
pub(crate) fn filter_user_tx(tx: Transaction) -> Option<Transaction> {
    matches!(tx.r#type, USER_TX).then_some(tx)
}

#[inline]
pub(crate) fn filter_success_tx(tx: Transaction) -> Option<Transaction> {
    tx.info.as_ref()?.success.then_some(tx)
}

pub(crate) fn filter_ls_events<'a>(
    addresses: &'a [(String, String)],
    tx: &'a Transaction,
) -> Option<impl Iterator<Item = (&'a String, &'a Event)>> {
    let itr = unwrap_usr_tx(tx)?.events.iter().filter_map(|ev| {
        let ms = match ev.move_struct() {
            Some(mt) => mt,
            None => return None,
        };

        (ms.module == LS_MODULE && LsEventType::from_str(&ms.name).is_ok())
            .then(|| {
                addresses
                    .iter()
                    .find(|(_version_ls, address)| address == &ms.address)
                    .map(|(version_ls, _)| (version_ls, ev))
            })
            .and_then(|v| v)
    });
    Some(itr)
}

pub(crate) fn filter_ls_tx(addresses: &[(String, String)], tx: Transaction) -> Option<Transaction> {
    let result = filter_ls_events(addresses, &tx)?.next().is_some();

    result.then_some(tx)
}

#[inline]
pub(crate) fn unwrap_usr_tx(tx: &Transaction) -> Option<&UserTransaction> {
    match tx.txn_data.as_ref()? {
        TxnData::User(user_tx, ..) => Some(user_tx),
        _ => None,
    }
}

pub(crate) fn move_type_to_string(mv: &MoveType) -> Option<String> {
    let content = mv.content.as_ref()?;

    let result = match content {
        Content::Vector(mv) => {
            format!("vector<{}>", move_type_to_string(mv).unwrap_or_default())
        },
        Content::Struct(MoveStructTag {
            address,
            module,
            name,
            generic_type_params,
        }) => {
            let mut gen_string = generic_type_params
                .iter()
                .filter_map(move_type_to_string)
                .collect::<Vec<String>>()
                .join(", ");

            gen_string = if gen_string.is_empty() {
                Default::default()
            } else {
                format!("<{}>", gen_string)
            };

            format!("{address}::{module}::{name}{gen_string}")
        },
        Content::GenericTypeParamIndex(index) => format!("T{index}"),
        Content::Reference(v) => {
            let ReferenceType { mutable, to } = v.as_ref();
            let s = to
                .as_ref()
                .and_then(|to| move_type_to_string(to))
                .unwrap_or_default();
            if *mutable {
                format!("&mut {s}")
            } else {
                format!("&{s}")
            }
        },
        Content::Unparsable(string) => format!("unparsable<{string}>"),
    };

    Some(result)
}

#[inline]
pub(crate) fn clr_hex_address(address: &str) -> String {
    format!(
        "0x{}",
        address.trim_start_matches("0x").trim_start_matches('0')
    )
}

pub(crate) trait EventLs {
    fn move_struct(&self) -> Option<&MoveStructTag>;
    fn data_value(&self) -> Result<serde_json::Value>;
    fn key(&self) -> Result<String>;
}

impl EventLs for Event {
    fn move_struct(&self) -> Option<&MoveStructTag> {
        match self.r#type.as_ref()?.content.as_ref()? {
            Content::Struct(st) => Some(st),
            _ => None,
        }
    }

    fn data_value(&self) -> Result<serde_json::Value> {
        serde_json::from_str(&self.data).map_err(|err| anyhow!("Couldn't parse the json. {err}"))
    }

    fn key(&self) -> Result<String> {
        self.key
            .as_ref()
            .map(|key| format!("{}_{}", key.account_address, key.creation_number))
            .ok_or(anyhow!("EventKey is not set"))
    }
}

pub(crate) trait MoveStructTagLs {
    fn generic_type_params_as_vec_str(&self) -> Vec<String>;

    fn pool_type(&self) -> Result<PoolType>;
}

impl MoveStructTagLs for MoveStructTag {
    fn generic_type_params_as_vec_str(&self) -> Vec<String> {
        self.generic_type_params
            .iter()
            .filter_map(move_type_to_string)
            .collect::<Vec<String>>()
    }

    fn pool_type(&self) -> Result<PoolType> {
        self.generic_type_params_as_vec_str().try_into()
    }
}

pub(crate) struct PoolType {
    pub x_name: String,
    pub y_name: String,
    pub curve: String,
}

impl PoolType {
    pub(crate) fn hash(&self) -> String {
        sha256_from_str(&format!("{}{}{}", self.x_name, self.y_name, self.curve))
    }
}

impl TryFrom<Vec<String>> for PoolType {
    type Error = anyhow::Error;

    fn try_from(value: Vec<String>) -> std::prelude::v1::Result<Self, Self::Error> {
        let [x_name, y_name, curve]: [String; 3] = value
            .try_into()
            .map_err(|val| anyhow!("3 generic types were expected. {val:?}"))?;

        Ok(PoolType {
            x_name,
            y_name,
            curve,
        })
    }
}

pub(crate) trait TransactionLs {
    fn info(&self) -> Option<TransactionInfo>;
}

impl TransactionLs for Transaction {
    fn info(&self) -> Option<TransactionInfo> {
        let version = self.version.try_into().unwrap();
        let info = self.info.as_ref()?;
        let tx_hash = hex::encode(&info.hash);
        let timestamp = self.timestamp.as_ref()?.seconds;
        let sender = clr_hex_address(&unwrap_usr_tx(self)?.request.as_ref()?.sender);

        Some(TransactionInfo {
            version,
            tx_hash,
            timestamp,
            sender,
        })
    }
}

pub(crate) struct TransactionInfo {
    pub(crate) version: i64,
    pub(crate) tx_hash: String,
    pub(crate) timestamp: i64,
    pub(crate) sender: String,
}
