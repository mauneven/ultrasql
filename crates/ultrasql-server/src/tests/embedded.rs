use super::*;

#[test]
fn embedded_memory_executes_ddl_dml_and_select_in_one_session() {
    let mut db = EmbeddedDatabase::open_memory();

    let create = db
        .execute("CREATE TABLE embedded_users (id int4, name text)")
        .expect("create table");
    assert_eq!(create.command_tag, "CREATE TABLE");
    assert!(create.columns.is_empty());
    assert!(create.rows.is_empty());

    let insert = db
        .execute("INSERT INTO embedded_users VALUES (1, 'Ada'), (2, 'Grace')")
        .expect("insert rows");
    assert_eq!(insert.command_tag, "INSERT 0 2");
    assert!(insert.columns.is_empty());
    assert!(insert.rows.is_empty());

    let select = db
        .execute("SELECT id, name FROM embedded_users ORDER BY id")
        .expect("select rows");
    assert_eq!(select.command_tag, "SELECT 2");
    assert_eq!(
        select
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name"]
    );
    assert_eq!(
        select.rows,
        vec![
            vec![Some("1".to_owned()), Some("Ada".to_owned())],
            vec![Some("2".to_owned()), Some("Grace".to_owned())],
        ]
    );
}
