//! Recall-at-budget harness for the page-backed vector indexes. Builds N
//! deterministic random vectors, then reports recall@k against brute-force
//! ground truth as the per-query budget sweeps:
//!   - `hnsw`: budget = `ef_search` (via `search_with_ef`)
//!   - `ivfflat`: budget = `probes` (via `search_with_probes`)
//!
//! Usage:
//!   `vector_recall_sweep hnsw    <rows> <dims> <m>     <queries> <b1,b2,...>`
//!   `vector_recall_sweep ivfflat <rows> <dims> <lists> <queries> <b1,b2,...>`

use std::collections::BTreeSet;
use std::env;

use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};
use ultrasql_storage::access_method::{HnswMetric, PageBackedHnswIndex, PageBackedIvfFlatIndex};

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn vector(seed: u64, dims: usize) -> Vec<f32> {
    let mut state = seed;
    (0..dims)
        .map(|_| {
            let bits = (splitmix(&mut state) >> 40) as u32;
            (bits as f32) / f32::from(u16::MAX) - 8388.0
        })
        .collect()
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn tid(row: usize) -> TupleId {
    let block = u32::try_from(row / 1000).unwrap();
    let slot = u16::try_from(row % 1000).unwrap();
    TupleId::new(
        PageId::new(RelationId::new(99001), BlockNumber::new(block)),
        slot,
    )
}

fn row_of(tid: TupleId) -> usize {
    (tid.page.block.raw() as usize) * 1000 + tid.slot as usize
}

/// `(probe, budget) -> result tids` for whichever index this run built.
type SearchFn = Box<dyn Fn(&[f32], usize) -> Vec<TupleId>>;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mode = args[1].clone();
    let rows: usize = args[2].parse().unwrap();
    let dims: usize = args[3].parse().unwrap();
    let build_param: usize = args[4].parse().unwrap(); // hnsw: m, ivfflat: lists
    let queries: usize = args[5].parse().unwrap();
    let budgets: Vec<usize> = args[6].split(',').map(|s| s.parse().unwrap()).collect();
    let k = 10usize;
    let dims_u32 = u32::try_from(dims).unwrap();
    let rel = RelationId::new(99001);

    let data: Vec<Vec<f32>> = (0..rows).map(|r| vector(r as u64 + 1, dims)).collect();
    let query_vecs: Vec<Vec<f32>> = (0..queries)
        .map(|q| vector(0xdead_0000 + q as u64, dims))
        .collect();
    let truth: Vec<Vec<usize>> = query_vecs
        .iter()
        .map(|p| {
            let mut d: Vec<(f32, usize)> = data
                .iter()
                .enumerate()
                .map(|(r, v)| (l2(p, v), r))
                .collect();
            d.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            d.into_iter().take(k).map(|(_, r)| r).collect()
        })
        .collect();

    let build = std::time::Instant::now();
    let search: SearchFn = match mode.as_str() {
        "hnsw" => {
            let index = PageBackedHnswIndex::new(rel, dims_u32, HnswMetric::L2, build_param, 64)
                .expect("config");
            for (r, v) in data.iter().enumerate() {
                index.insert_vector(v, tid(r)).expect("insert");
            }
            eprintln!("build_ms={:.0}", build.elapsed().as_secs_f64() * 1000.0);
            Box::new(move |p, budget| {
                index
                    .search_with_ef(p, k, budget)
                    .expect("search")
                    .into_iter()
                    .map(|h| h.tid)
                    .collect()
            })
        }
        "ivfflat" => {
            let index = PageBackedIvfFlatIndex::new(rel, dims_u32, HnswMetric::L2, build_param, 1)
                .expect("config");
            let load: Vec<(Vec<f32>, TupleId)> = data
                .iter()
                .enumerate()
                .map(|(r, v)| (v.clone(), tid(r)))
                .collect();
            index.bulk_load(load).expect("bulk load");
            eprintln!("build_ms={:.0}", build.elapsed().as_secs_f64() * 1000.0);
            Box::new(move |p, budget| {
                index
                    .search_with_probes(p, k, budget)
                    .expect("search")
                    .into_iter()
                    .map(|h| h.tid)
                    .collect()
            })
        }
        other => panic!("unknown mode {other}; expected hnsw or ivfflat"),
    };

    let label = if mode == "ivfflat" { "probes" } else { "ef" };
    for budget in budgets {
        let mut recall = 0.0f64;
        for (qi, p) in query_vecs.iter().enumerate() {
            let got: BTreeSet<usize> = search(p, budget).into_iter().map(row_of).collect();
            let overlap = truth[qi].iter().filter(|r| got.contains(r)).count();
            recall += overlap as f64 / k as f64;
        }
        println!(
            "{label}={budget:>5}  recall@{k}={:.4}",
            recall / queries as f64
        );
    }
}
