use std::process::Command;

#[test]
fn local_query_counts_csv_file_literal_without_server() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = dir.path().join("events.csv");
    std::fs::write(&file, "level,value\ninfo,1\nwarn,2\n").expect("write csv");
    let query = format!("SELECT count(*) FROM '{}'", file.display());
    let bin = std::env::var("CARGO_BIN_EXE_ultrasql-local").expect("ultrasql-local binary");

    let output = Command::new(bin)
        .args(["-q", &query])
        .output()
        .expect("run ultrasql-local");

    assert!(
        output.status.success(),
        "status={:?}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout utf8");
    assert!(stdout.contains("count"), "{stdout}");
    assert!(stdout.contains("| 2"), "{stdout}");
    assert!(stdout.contains("(1 row)"), "{stdout}");
}
