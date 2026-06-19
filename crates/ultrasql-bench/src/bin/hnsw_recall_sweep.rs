//! Recall-at-ef harness for the page-backed HNSW index. Builds N deterministic
//! random vectors, then for each `ef` reports recall@k of `search_with_ef`
//! against brute-force ground truth — used to compare the single-layer vs
//! hierarchical build.
//!
//! Usage: `hnsw_recall_sweep <rows> <dims> <m> <queries> <ef1,ef2,...>`

use std::env;

use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};
use ultrasql_storage::access_method::{HnswMetric, PageBackedHnswIndex};

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
            (bits as f32) / (f32::from(u16::MAX)) - 8388.0
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

fn main() {
    let args: Vec<String> = env::args().collect();
    let rows: usize = args[1].parse().unwrap();
    let dims: usize = args[2].parse().unwrap();
    let m: usize = args[3].parse().unwrap();
    let queries: usize = args[4].parse().unwrap();
    let efs: Vec<usize> = args[5].split(',').map(|s| s.parse().unwrap()).collect();
    let k = 10usize;

    let dims_u32 = u32::try_from(dims).unwrap();
    let index = PageBackedHnswIndex::new(RelationId::new(99001), dims_u32, HnswMetric::L2, m, 64)
        .expect("config");
    let data: Vec<Vec<f32>> = (0..rows).map(|r| vector(r as u64 + 1, dims)).collect();
    let build = std::time::Instant::now();
    for (r, v) in data.iter().enumerate() {
        index.insert_vector(v, tid(r)).expect("insert");
    }
    eprintln!("build_ms={:.0}", build.elapsed().as_secs_f64() * 1000.0);

    let probes: Vec<Vec<f32>> = (0..queries)
        .map(|q| vector(0xdead_0000 + q as u64, dims))
        .collect();
    // Brute-force ground truth per query.
    let truth: Vec<Vec<usize>> = probes
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

    for ef in efs {
        let mut recall = 0.0f64;
        for (qi, p) in probes.iter().enumerate() {
            let hits = index.search_with_ef(p, k, ef).expect("search");
            let got: std::collections::BTreeSet<usize> = hits
                .iter()
                .map(|h| (h.tid.page.block.raw() as usize) * 1000 + h.tid.slot as usize)
                .collect();
            let want = &truth[qi];
            let overlap = want.iter().filter(|r| got.contains(r)).count();
            recall += overlap as f64 / k as f64;
        }
        println!("ef={ef:>4}  recall@{k}={:.4}", recall / queries as f64);
    }
}
