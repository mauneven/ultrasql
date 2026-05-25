use diesel::dsl::count_star;
use diesel::prelude::*;
use diesel::{insert_into, sql_query};

diesel::table! {
    users (id) {
        id -> Int4,
        name -> Text,
    }
}

diesel::table! {
    diesel_cert (id) {
        id -> Int4,
        label -> Text,
    }
}

#[derive(Debug, Insertable, PartialEq, Queryable, Selectable)]
#[diesel(table_name = diesel_cert)]
struct DieselCert {
    id: i32,
    label: String,
}

fn fail(context: &str, message: impl std::fmt::Display) -> ! {
    panic!("{context}: {message}");
}

fn assert_rows<T>(context: &str, actual: &[T], expected: &[T])
where
    T: std::fmt::Debug + PartialEq,
{
    if actual != expected {
        fail(context, format!("expected {expected:?}, got {actual:?}"));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: diesel-cert DATABASE_URL");
        std::process::exit(2);
    });

    let mut conn = PgConnection::establish(&dsn)?;

    let selected: Vec<(i32, String)> = users::table
        .filter(users::id.eq(3))
        .select((users::id, users::name))
        .load(&mut conn)?;
    assert_rows(
        "Diesel parameterized SELECT",
        &selected,
        &[(3, String::from("Linus"))],
    );

    sql_query("CREATE TABLE diesel_cert (id INT NOT NULL PRIMARY KEY, label TEXT NOT NULL)")
        .execute(&mut conn)?;
    insert_into(diesel_cert::table)
        .values(&[
            DieselCert {
                id: 1,
                label: String::from("alpha"),
            },
            DieselCert {
                id: 2,
                label: String::from("beta"),
            },
        ])
        .execute(&mut conn)?;

    let rows: Vec<DieselCert> = diesel_cert::table
        .order(diesel_cert::id.asc())
        .select(DieselCert::as_select())
        .load(&mut conn)?;
    assert_rows(
        "Diesel insert/query",
        &rows,
        &[
            DieselCert {
                id: 1,
                label: String::from("alpha"),
            },
            DieselCert {
                id: 2,
                label: String::from("beta"),
            },
        ],
    );

    let rollback = conn.transaction::<(), diesel::result::Error, _>(|tx| {
        insert_into(diesel_cert::table)
            .values(&DieselCert {
                id: 3,
                label: String::from("rollback"),
            })
            .execute(tx)?;
        Err(diesel::result::Error::RollbackTransaction)
    });
    if !matches!(rollback, Err(diesel::result::Error::RollbackTransaction)) {
        fail(
            "Diesel transaction rollback",
            format!("expected rollback error, got {rollback:?}"),
        );
    }

    let count: i64 = diesel_cert::table.select(count_star()).first(&mut conn)?;
    if count != 2 {
        fail(
            "Diesel transaction rollback",
            format!("expected 2, got {count}"),
        );
    }

    if sql_query("SELECT missing_column FROM diesel_cert")
        .execute(&mut conn)
        .is_ok()
    {
        fail(
            "Diesel failed transaction",
            "expected missing-column failure",
        );
    }
    let ids: Vec<i32> = diesel_cert::table
        .order(diesel_cert::id.asc())
        .select(diesel_cert::id)
        .load(&mut conn)?;
    assert_rows("Diesel recovery after error", &ids, &[1, 2]);

    Ok(())
}
