//! TPC-H data generation: `dbgen` wrapper with a deterministic synthetic fallback.
//!
//! When `dbgen` is available on `$PATH`, [`generate`] shells out to it at the
//! requested scale factor and writes `.tbl` files to `out_dir`. When `dbgen`
//! is unavailable, a minimal synthetic dataset is written instead — sufficient
//! to load the schema and exercise the query harness, but **not** a valid TPC-H
//! result and **not** suitable for published benchmark numbers.
//!
//! The synthetic generator is deterministic: given the same scale factor it
//! always produces the same output.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Table names in the order `dbgen` generates them.
pub const TABLE_NAMES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

/// Result of a data-generation run.
#[derive(Debug)]
pub struct GenResult {
    /// The directory that holds the `.tbl` files.
    pub out_dir: PathBuf,
    /// Whether `dbgen` was used (`true`) or the synthetic fallback (`false`).
    pub used_dbgen: bool,
}

/// Generates TPC-H `.tbl` files for the given scale factor.
///
/// If `dbgen` is found on `$PATH` it is invoked with `-s <scale>`. Otherwise
/// the synthetic fallback writes skeletal `.tbl` files into `out_dir`. The
/// caller is responsible for creating `out_dir` before calling this function.
pub fn generate(scale: u32, out_dir: &Path) -> Result<GenResult> {
    if which_dbgen().is_some() {
        run_dbgen(scale, out_dir)?;
        Ok(GenResult {
            out_dir: out_dir.to_owned(),
            used_dbgen: true,
        })
    } else {
        write_synthetic(scale, out_dir)?;
        Ok(GenResult {
            out_dir: out_dir.to_owned(),
            used_dbgen: false,
        })
    }
}

/// Returns the path to `dbgen` if it exists on `$PATH`, otherwise `None`.
fn which_dbgen() -> Option<PathBuf> {
    std::env::var_os("PATH")
        .unwrap_or_default()
        .to_string_lossy()
        .split(':')
        .find_map(|dir| {
            if dir.is_empty() {
                return None;
            }
            let candidate = Path::new(dir).join("dbgen");
            if candidate.exists() {
                Some(candidate)
            } else {
                None
            }
        })
}

/// Shells out to `dbgen -vf -s <scale>` in `out_dir`.
fn run_dbgen(scale: u32, out_dir: &Path) -> Result<()> {
    let dbgen = which_dbgen().expect("dbgen must exist — caller checked");
    let dbgen_dir = dbgen
        .parent()
        .context("dbgen path must have a parent directory")?;
    let out_dir = out_dir
        .canonicalize()
        .with_context(|| format!("canonicalize {}", out_dir.display()))?;
    let status = std::process::Command::new(&dbgen)
        .args(["-vf", "-s", &scale.to_string()])
        .env("DSS_CONFIG", dbgen_dir)
        .env("DSS_PATH", &out_dir)
        .current_dir(&out_dir)
        .status()
        .with_context(|| format!("spawn dbgen at {}", dbgen.display()))?;
    if !status.success() {
        anyhow::bail!("dbgen exited with status {:?}", status.code());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Synthetic fallback
// ---------------------------------------------------------------------------

/// Writes deterministic synthetic `.tbl` files.
///
/// The row counts are proportional to `scale` (minimum 1 row per table). The
/// data is pseudo-random but reproducible. This is **not** TPC-H-compliant —
/// it exists solely to allow `cargo test` and CI runs without a `dbgen` binary.
fn write_synthetic(scale: u32, out_dir: &Path) -> Result<()> {
    write_region(out_dir)?;
    write_nation(out_dir)?;
    write_supplier(scale, out_dir)?;
    write_customer(scale, out_dir)?;
    write_part(scale, out_dir)?;
    write_partsupp(scale, out_dir)?;
    write_orders(scale, out_dir)?;
    write_lineitem(scale, out_dir)?;
    Ok(())
}

fn tbl_path(out_dir: &Path, name: &str) -> PathBuf {
    out_dir.join(format!("{name}.tbl"))
}

fn write_region(out_dir: &Path) -> Result<()> {
    let data = "\
0|AFRICA|lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to \n\
1|AMERICA|hs use ironic, even requests. s\n\
2|ASIA|ges. thinly even pinto beans ca\n\
3|EUROPE|ly final courts cajole furiously final excuse\n\
4|MIDDLE EAST|uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl\n";
    std::fs::write(tbl_path(out_dir, "region"), data).context("write region.tbl")?;
    Ok(())
}

fn write_nation(out_dir: &Path) -> Result<()> {
    let data = "\
0|ALGERIA|0| haggle. carefully final deposits detect slyly agai\n\
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests abound\n\
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special \n\
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold\n\
4|EGYPT|4|y above the carefully unusual theodolites. final dunites are quickly across the furiously regular d\n\
5|ETHIOPIA|0|ven packages wake quickly. regu\n\
6|FRANCE|3|refully final requests. regular, ironi\n\
7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco\n\
8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun\n\
9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull\n\
10|IRAN|4|efully alongside of the slyly final dependencies. \n\
11|IRAQ|4|nic deposits boost atop the quickly final requests? quickly regula\n\
12|JAPAN|2|ously. final, express gifts cajole a\n\
13|JORDAN|4|ic deposits are blithely about the carefully regular pa\n\
14|KENYA|0| pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t\n\
15|MOROCCO|0|rns. blithely bold courts among the closely regular packages use furiously bold platelets?\n\
16|MOZAMBIQUE|0|s. ironic, unusual asymptotes wake blithely r\n\
17|PERU|1|platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun\n\
18|CHINA|2|c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos\n\
19|ROMANIA|3|ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account\n\
20|SAUDI ARABIA|4|ts. silent requests haggle. closely express packages sleep across the blithely\n\
21|VIETNAM|2|hely enticingly express accounts. even, final \n\
22|RUSSIA|3| requests against the platelets use never according to the quickly regular pint\n\
23|UNITED KINGDOM|3|eans boost carefully special requests. accounts are. carefull\n\
24|UNITED STATES|1|y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be\n";
    std::fs::write(tbl_path(out_dir, "nation"), data).context("write nation.tbl")?;
    Ok(())
}

fn write_supplier(scale: u32, out_dir: &Path) -> Result<()> {
    let n = (10_000 * scale).max(1);
    let mut buf = String::new();
    let mut rng = Xorshift64::new(0x5EED_0001);
    for i in 1..=n {
        let nation_key = rng.next_u64() % 25;
        let acctbal = format!("{:.2}", f64::from(i % 9999) - 999.99);
        let addr = fake_string(&mut rng, 15, 40);
        let phone = fake_phone(&mut rng, nation_key);
        let comment = fake_string(&mut rng, 20, 101);
        writeln!(
            buf,
            "{i}|Supplier#{i:>000009}|{addr}|{nation_key}|{phone}|{acctbal}|{comment}"
        )
        .expect("String write is infallible");
    }
    std::fs::write(tbl_path(out_dir, "supplier"), buf).context("write supplier.tbl")
}

fn write_customer(scale: u32, out_dir: &Path) -> Result<()> {
    let n = (150_000 * scale).max(1);
    let segments = [
        "AUTOMOBILE",
        "BUILDING",
        "FURNITURE",
        "HOUSEHOLD",
        "MACHINERY",
    ];
    let mut buf = String::new();
    let mut rng = Xorshift64::new(0x5EED_0002);
    for i in 1..=n {
        let nation_key = rng.next_u64() % 25;
        let seg = segments[(i as usize - 1) % segments.len()];
        let acctbal = format!("{:.2}", f64::from(i % 9999) - 999.99);
        let addr = fake_string(&mut rng, 10, 40);
        let phone = fake_phone(&mut rng, nation_key);
        let comment = fake_string(&mut rng, 20, 117);
        writeln!(
            buf,
            "{i}|Customer#{i:>000009}|{addr}|{nation_key}|{phone}|{acctbal}|{seg}|{comment}"
        )
        .expect("String write is infallible");
    }
    std::fs::write(tbl_path(out_dir, "customer"), buf).context("write customer.tbl")
}

fn write_part(scale: u32, out_dir: &Path) -> Result<()> {
    let n = (200_000 * scale).max(1);
    let mut buf = String::new();
    let mut rng = Xorshift64::new(0x5EED_0003);
    for i in 1..=n {
        let size = (rng.next_u64() % 50 + 1) as u32;
        let price = format!("{:.2}", 900.0 + f64::from(i % 20086) * 0.01);
        let name = fake_part_name(&mut rng);
        let mfgr = rng.next_u64() % 5 + 1;
        let brand = rng.next_u64() % 40 + 11;
        let ptype = fake_part_type(&mut rng);
        let container = fake_container(&mut rng);
        let comment = fake_string(&mut rng, 5, 23);
        writeln!(
            buf,
            "{i}|{name}|Manufacturer#{mfgr}|Brand#{brand}|{ptype}|{size}|{container}|{price}|{comment}"
        )
        .expect("String write is infallible");
    }
    std::fs::write(tbl_path(out_dir, "part"), buf).context("write part.tbl")
}

fn write_partsupp(scale: u32, out_dir: &Path) -> Result<()> {
    let n_parts = (200_000 * scale).max(1);
    let n_suppliers = (10_000 * scale).max(1);
    let mut buf = String::new();
    let mut rng = Xorshift64::new(0x5EED_0004);
    // Each part has exactly 4 suppliers.
    for p in 1..=n_parts {
        for s_offset in 0u64..4 {
            let suppkey = (rng.next_u64() % u64::from(n_suppliers)) + 1;
            // Avoid duplicate (partkey, suppkey) in the synthetic set by using offset.
            let suppkey = ((suppkey + s_offset - 1) % u64::from(n_suppliers)) + 1;
            let availqty = rng.next_u64() % 9999 + 1;
            let supplycost = format!(
                "{:.2}",
                f64::from((rng.next_u64() % 100_000) as u32) / 100.0 + 1.0
            );
            let comment = fake_string(&mut rng, 30, 199);
            writeln!(buf, "{p}|{suppkey}|{availqty}|{supplycost}|{comment}")
                .expect("String write is infallible");
        }
    }
    std::fs::write(tbl_path(out_dir, "partsupp"), buf).context("write partsupp.tbl")
}

fn write_orders(scale: u32, out_dir: &Path) -> Result<()> {
    let n = (1_500_000 * scale).max(1);
    let n_customers = (150_000 * scale).max(1);
    let statuses = ['O', 'F', 'P'];
    let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
    let mut buf = String::new();
    let mut rng = Xorshift64::new(0x5EED_0005);
    for i in 1..=n {
        // Non-customer orderkeys: multiples-of-4 within customer range.
        let orderkey = i * 4;
        let custkey = (rng.next_u64() % u64::from(n_customers)) + 1;
        let status = statuses[(i as usize - 1) % statuses.len()];
        let price = format!(
            "{:.2}",
            f64::from((rng.next_u64() % 50_000) as u32) + 1000.0
        );
        let date = fake_date(&mut rng, 1992, 1998);
        let priority = priorities[(i as usize - 1) % priorities.len()];
        let clerk = format!("Clerk#{:>000009}", rng.next_u64() % 1000 + 1);
        let comment = fake_string(&mut rng, 10, 79);
        writeln!(
            buf,
            "{orderkey}|{custkey}|{status}|{price}|{date}|{priority}|{clerk}|0|{comment}"
        )
        .expect("String write is infallible");
    }
    std::fs::write(tbl_path(out_dir, "orders"), buf).context("write orders.tbl")
}

fn write_lineitem(scale: u32, out_dir: &Path) -> Result<()> {
    let n_orders = (1_500_000 * scale).max(1);
    let n_parts = (200_000 * scale).max(1);
    let n_suppliers = (10_000 * scale).max(1);
    let shipmodes = ["AIR", "TRUCK", "SHIP", "RAIL", "MAIL", "FOB", "REG AIR"];
    let shipinstructs = [
        "DELIVER IN PERSON",
        "COLLECT COD",
        "TAKE BACK RETURN",
        "NONE",
    ];
    let mut buf = String::new();
    let mut rng = Xorshift64::new(0x5EED_0006);
    for o in 1..=n_orders {
        let orderkey = o * 4;
        let linecnt = (rng.next_u64() % 7 + 1) as u32;
        for linenumber in 1..=linecnt {
            let partkey = (rng.next_u64() % u64::from(n_parts)) + 1;
            let suppkey = (rng.next_u64() % u64::from(n_suppliers)) + 1;
            let quantity = format!("{:.2}", f64::from((rng.next_u64() % 50 + 1) as u32));
            let ep = format!(
                "{:.2}",
                f64::from((rng.next_u64() % 100_000) as u32) / 100.0 + 100.0
            );
            let discount = format!("{:.2}", f64::from((rng.next_u64() % 11) as u32) / 100.0);
            let tax = format!("{:.2}", f64::from((rng.next_u64() % 9) as u32) / 100.0);
            let returnflag = if rng.next_u64() % 3 == 0 { 'R' } else { 'N' };
            let linestatus = if rng.next_u64() % 2 == 0 { 'O' } else { 'F' };
            let shipdate = fake_date(&mut rng, 1992, 1998);
            let commitdate = fake_date(&mut rng, 1992, 1998);
            let receiptdate = fake_date(&mut rng, 1992, 1999);
            let shipmode = shipmodes[(rng.next_u64() as usize) % shipmodes.len()];
            let shipinstruct = shipinstructs[(rng.next_u64() as usize) % shipinstructs.len()];
            let comment = fake_string(&mut rng, 5, 44);
            writeln!(
                buf,
                "{orderkey}|{partkey}|{suppkey}|{linenumber}|{quantity}|{ep}|{discount}|{tax}|\
                 {returnflag}|{linestatus}|{shipdate}|{commitdate}|{receiptdate}|\
                 {shipinstruct}|{shipmode}|{comment}"
            )
            .expect("String write is infallible");
        }
    }
    std::fs::write(tbl_path(out_dir, "lineitem"), buf).context("write lineitem.tbl")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Minimal xorshift-64 PRNG for reproducible synthetic data.
struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    const fn new(seed: u64) -> Self {
        // Ensure non-zero state.
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    const fn next_u64(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }
}

fn fake_string(rng: &mut Xorshift64, min_len: usize, max_len: usize) -> String {
    let len = min_len + (rng.next_u64() as usize % (max_len - min_len + 1));
    (0..len)
        .map(|_| (b'a' + (rng.next_u64() % 26) as u8) as char)
        .collect()
}

fn fake_phone(rng: &mut Xorshift64, nation_key: u64) -> String {
    format!(
        "{}-{}-{}-{}",
        10 + nation_key % 40,
        100 + rng.next_u64() % 900,
        100 + rng.next_u64() % 900,
        1000 + rng.next_u64() % 9000,
    )
}

fn fake_date(rng: &mut Xorshift64, year_min: u32, year_max: u32) -> String {
    let year = year_min + (rng.next_u64() as u32 % (year_max - year_min + 1));
    let month = 1 + rng.next_u64() as u32 % 12;
    let day = 1 + rng.next_u64() as u32 % 28;
    format!("{year:04}-{month:02}-{day:02}")
}

fn fake_part_name(rng: &mut Xorshift64) -> String {
    const WORDS: &[&str] = &[
        "almond",
        "antique",
        "aquamarine",
        "azure",
        "beige",
        "bisque",
        "black",
        "blanched",
        "blue",
        "blush",
        "brown",
        "burlywood",
        "burnished",
        "chartreuse",
        "chiffon",
        "chocolate",
        "coral",
        "cornflower",
        "cornsilk",
        "cream",
        "cyan",
        "dark",
        "deep",
        "dim",
        "dodger",
        "drab",
        "firebrick",
        "floral",
        "forest",
        "frosted",
        "ghost",
        "goldenrod",
        "green",
        "grey",
        "honeydew",
        "hot",
        "indian",
        "ivory",
        "khaki",
        "lace",
        "lavender",
        "lawn",
        "lemon",
        "light",
        "lime",
        "linen",
        "magenta",
        "maroon",
        "medium",
        "midnight",
    ];
    const N: usize = 5;
    (0..N)
        .map(|_| WORDS[(rng.next_u64() as usize) % WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

fn fake_part_type(rng: &mut Xorshift64) -> String {
    const SYLLABLES: &[&str] = &["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"];
    const MATERIALS: &[&str] = &["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"];
    const SUBSTANCES: &[&str] = &["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"];
    format!(
        "{} {} {}",
        SYLLABLES[(rng.next_u64() as usize) % SYLLABLES.len()],
        MATERIALS[(rng.next_u64() as usize) % MATERIALS.len()],
        SUBSTANCES[(rng.next_u64() as usize) % SUBSTANCES.len()],
    )
}

fn fake_container(rng: &mut Xorshift64) -> String {
    const SIZES: &[&str] = &["SM", "MED", "LG", "JUMBO", "WRAP"];
    const KINDS: &[&str] = &["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"];
    format!(
        "{} {}",
        SIZES[(rng.next_u64() as usize) % SIZES.len()],
        KINDS[(rng.next_u64() as usize) % KINDS.len()],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_generates_all_tbl_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        generate(1, dir.path()).expect("generate");
        for name in TABLE_NAMES {
            let path = dir.path().join(format!("{name}.tbl"));
            assert!(path.exists(), "{name}.tbl not created");
            assert!(
                path.metadata().expect("metadata").len() > 0,
                "{name}.tbl is empty"
            );
        }
    }

    #[test]
    fn synthetic_is_deterministic() {
        let dir1 = tempfile::tempdir().expect("tempdir1");
        let dir2 = tempfile::tempdir().expect("tempdir2");
        write_synthetic(1, dir1.path()).expect("gen1");
        write_synthetic(1, dir2.path()).expect("gen2");
        for name in TABLE_NAMES {
            let a = std::fs::read(dir1.path().join(format!("{name}.tbl"))).expect("read a");
            let b = std::fs::read(dir2.path().join(format!("{name}.tbl"))).expect("read b");
            assert_eq!(a, b, "{name}.tbl is not deterministic");
        }
    }
}
