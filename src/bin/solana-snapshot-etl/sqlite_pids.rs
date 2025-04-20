use log::info;
use rusqlite::{params, Connection};
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::{append_vec_iter, AppendVecIterator};
use std::path::{Path, PathBuf};
use std::rc::Rc;

pub struct RawProgramAccountDumper {
    db: Connection,
    program_id: [u8; 32],
}

impl RawProgramAccountDumper {
    pub fn new(path: PathBuf, program_id: [u8; 32]) -> rusqlite::Result<Self> {
        let db = Connection::open(&path)?;
        db.pragma_update(None, "synchronous", false)?;
        db.pragma_update(None, "journal_mode", "off")?;
        db.pragma_update(None, "locking_mode", "exclusive")?;

        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS raw_program_accounts (
                pubkey TEXT PRIMARY KEY,
                lamports INTEGER NOT NULL,
                owner TEXT NOT NULL,
                data BLOB NOT NULL
            );",
        )?;

        Ok(Self { db, program_id })
    }

    pub fn insert_all(
        &mut self,
        iter: impl Iterator<Item = solana_snapshot_etl::Result<AppendVec>>,
    ) -> rusqlite::Result<usize> {
        let mut count = 0;
        let tx = self.db.transaction()?;

        for append_vec in iter {
            let append_vec = Rc::new(append_vec?);
            for meta in append_vec_iter(Rc::clone(&append_vec)) {
                if let Some(account) = meta.access() {
                    if account.meta.owner != self.program_id {
                        continue;
                    }

                    let pubkey = bs58::encode(account.meta.pubkey).into_string();
                    let owner = bs58::encode(account.meta.owner).into_string();

                    tx.execute(
                        "INSERT OR IGNORE INTO raw_program_accounts (pubkey, lamports, owner, data)
                         VALUES (?1, ?2, ?3, ?4);",
                        params![
                            pubkey,
                            account.meta.lamports as i64,
                            owner,
                            &account.data
                        ],
                    )?;

                    count += 1;
                }
            }
        }

        tx.commit()?;
        info!("Done inserting {} accounts into raw_program_accounts", count);
        Ok(count)
    }
}
