//! Microbenchmarks for the WAL record format and append buffer.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use ultrasql_core::{Lsn, Xid};
use ultrasql_wal::{RecordType, WalBuffer, WalRecord};

const PAYLOAD_SIZES: &[usize] = &[0_usize, 64, 256, 1024, 4096];

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal/encode");
    for &n in PAYLOAD_SIZES {
        let payload = vec![0xAAu8; n];
        let rec = WalRecord::new(
            RecordType::HeapInsert,
            Xid::new(42),
            Lsn::new(100),
            0,
            payload,
        );
        group.throughput(Throughput::Bytes(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let bytes = rec.encode();
                black_box(bytes);
            });
        });
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal/decode");
    for &n in PAYLOAD_SIZES {
        let payload = vec![0x55u8; n];
        let rec = WalRecord::new(
            RecordType::HeapInsert,
            Xid::new(42),
            Lsn::new(100),
            0,
            payload,
        );
        let encoded = rec.encode();
        group.throughput(Throughput::Bytes(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let (parsed, _) = WalRecord::decode(black_box(&encoded)).unwrap();
                black_box(parsed);
            });
        });
    }
    group.finish();
}

fn bench_buffer_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal/buffer_append");
    for &n in &[64_usize, 256, 1024] {
        let payload = vec![0xFFu8; n];
        let rec = WalRecord::new(RecordType::HeapInsert, Xid::new(1), Lsn::ZERO, 0, payload);
        group.throughput(Throughput::Bytes(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_with_setup(
                || WalBuffer::new(16 * 1024 * 1024, Lsn::ZERO),
                |buf| {
                    let lsn = buf.append(black_box(&rec)).unwrap();
                    black_box(lsn);
                },
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode, bench_buffer_append);
criterion_main!(benches);
