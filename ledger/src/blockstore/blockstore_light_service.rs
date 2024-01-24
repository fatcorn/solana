use std::sync::Arc;
use std::thread;
use std::thread::{Builder, JoinHandle};
use crossbeam_channel::Receiver;
use solana_measure::measure;
use solana_measure::measure::Measure;
use solana_sdk::clock::Epoch;
use solana_sdk::epoch_schedule::EpochSchedule;
use crate::blockstore::Blockstore;

/// Default delete before 2 epoch slots
const MAX_VIRTUAL_SLOT_ALIVE_EPOCH: u64 = 2;

pub struct BlockstoreLightService {
    t_blockstore_light: JoinHandle<()>,
}

impl BlockstoreLightService {
    pub fn new(
        blockstore: Arc<Blockstore>,
        max_virtual_slot_alive_epoch: Option<u64>,
        current_epoch: Receiver<Epoch>,
        schedule: EpochSchedule
    ) -> Self {
        let max_virtual_slot_alive_epoch = if max_virtual_slot_alive_epoch.is_some() {
            max_virtual_slot_alive_epoch.unwrap()
        } else {
            MAX_VIRTUAL_SLOT_ALIVE_EPOCH
        };

        let hdl = Builder::new()
            .name("solBlstLighSec".to_string())
            .spawn(move || {
                for current_epoch in current_epoch.iter() {
                    if current_epoch <= max_virtual_slot_alive_epoch {
                        info!("Skip the initial epoch {} to light", current_epoch);
                        continue;
                    }
                    let target_epoch = current_epoch - max_virtual_slot_alive_epoch;
                    let start_slot = schedule.get_first_slot_in_epoch(target_epoch);
                    let end_slot = schedule.get_last_slot_in_epoch(target_epoch);
                    let mut blockstore_light_time = Measure::start("blockstore-light-ms");
                    blockstore.purge_and_compact_slots(start_slot, end_slot);
                    blockstore_light_time.stop();
                    datapoint_info!(
                        "blockstore-light",
                        (
                            "blockstore-light-ms",
                            blockstore_light_time.as_ms() as i64,
                            i64
                        ),
                        ("start-slot", start_slot, i64),
                        ("end-slot", end_slot, i64),
                    );
                }
            }).unwrap();
        Self{ t_blockstore_light: hdl}
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_blockstore_light.join()
    }
}