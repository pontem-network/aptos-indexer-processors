// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::Parser;
use processor::IndexerGrpcProcessorConfig;
use server_framework::{setup_logging, ServerArgs};
use tracing::debug;
use tracing_subscriber::EnvFilter;

const RUNTIME_WORKER_MULTIPLIER: usize = 2;

fn main() -> Result<()> {
    debug!("main");

    inic_logger();
    // setup_logging();

    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus * RUNTIME_WORKER_MULTIPLIER).max(16);
    println!(
        "[Processor] Starting processor tokio runtime: num_cpus={}, worker_threads={}",
        num_cpus, worker_threads
    );

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .disable_lifo_slot()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap()
        .block_on(async {
            let args = ServerArgs::parse();
            args.run::<IndexerGrpcProcessorConfig>(tokio::runtime::Handle::current())
                .await
        })
}

fn inic_logger() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info")))
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_target(false)
            .with_thread_names(true)
            .finish(),
    )
    .expect("failed to set global default subscriber");
}
