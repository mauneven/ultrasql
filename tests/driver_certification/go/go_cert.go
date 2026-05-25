package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func fail(context string, format string, args ...any) {
	panic(fmt.Sprintf("%s: %s", context, fmt.Sprintf(format, args...)))
}

func assertRows(context string, got [][2]any, want [][2]any) {
	if len(got) != len(want) {
		fail(context, "expected %d rows, got %d: %#v", len(want), len(got), got)
	}
	for i := range got {
		if got[i] != want[i] {
			fail(context, "row %d expected %#v, got %#v", i, want[i], got[i])
		}
	}
}

func assertOneInt(context string, got int, want int) {
	if got != want {
		fail(context, "expected %d, got %d", want, got)
	}
}

type GormCert struct {
	ID    int    `gorm:"column:id;primaryKey;autoIncrement:false"`
	Label string `gorm:"column:label;type:text;not null"`
}

func (GormCert) TableName() string {
	return "gorm_cert"
}

func certifyLibPQ(ctx context.Context, dsn string) {
	db, err := sql.Open("postgres", dsn+"&application_name=driver_cert_go_pq")
	if err != nil {
		fail("lib/pq open", "%v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		fail("lib/pq startup", "%v", err)
	}

	rows, err := db.QueryContext(ctx, "SELECT id, name FROM users WHERE id = $1", 2)
	if err != nil {
		fail("lib/pq parameterized SELECT", "%v", err)
	}
	var selected [][2]any
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			fail("lib/pq scan select", "%v", err)
		}
		selected = append(selected, [2]any{id, name})
	}
	if err := rows.Close(); err != nil {
		fail("lib/pq close select rows", "%v", err)
	}
	if err := rows.Err(); err != nil {
		fail("lib/pq select rows", "%v", err)
	}
	assertRows("lib/pq parameterized SELECT", selected, [][2]any{{2, "Grace"}})

	if _, err := db.ExecContext(ctx, "CREATE TABLE go_pq_cert (id INT NOT NULL, label TEXT)"); err != nil {
		fail("lib/pq create table", "%v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO go_pq_cert VALUES ($1, $2)", 1, "alpha"); err != nil {
		fail("lib/pq insert alpha", "%v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO go_pq_cert VALUES ($1, $2)", 2, "beta"); err != nil {
		fail("lib/pq insert beta", "%v", err)
	}
	rows, err = db.QueryContext(ctx, "SELECT id, label FROM go_pq_cert ORDER BY id")
	if err != nil {
		fail("lib/pq select inserted", "%v", err)
	}
	selected = selected[:0]
	for rows.Next() {
		var id int
		var label string
		if err := rows.Scan(&id, &label); err != nil {
			fail("lib/pq scan inserted", "%v", err)
		}
		selected = append(selected, [2]any{id, label})
	}
	if err := rows.Close(); err != nil {
		fail("lib/pq close inserted rows", "%v", err)
	}
	assertRows("lib/pq parameterized INSERT", selected, [][2]any{{1, "alpha"}, {2, "beta"}})

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		fail("lib/pq begin rollback", "%v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO go_pq_cert VALUES ($1, $2)", 3, "rollback"); err != nil {
		fail("lib/pq rollback insert", "%v", err)
	}
	if err := tx.Rollback(); err != nil {
		fail("lib/pq rollback", "%v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM go_pq_cert").Scan(&count); err != nil {
		fail("lib/pq rollback count", "%v", err)
	}
	assertOneInt("lib/pq explicit transaction rollback", count, 2)

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		fail("lib/pq begin failed transaction", "%v", err)
	}
	if _, err := tx.ExecContext(ctx, "SELECT missing_column FROM go_pq_cert"); err == nil {
		fail("lib/pq failed transaction", "expected missing-column failure")
	}
	if err := tx.Rollback(); err != nil {
		fail("lib/pq failed transaction rollback", "%v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM go_pq_cert").Scan(&count); err != nil {
		fail("lib/pq recovery count", "%v", err)
	}
	assertOneInt("lib/pq recovery after error", count, 2)
}

func certifyPGX(ctx context.Context, dsn string) {
	conn, err := pgx.Connect(ctx, dsn+"&application_name=driver_cert_go_pgx")
	if err != nil {
		fail("pgx startup", "%v", err)
	}
	defer conn.Close(ctx)

	var id int
	var name string
	if err := conn.QueryRow(ctx, "SELECT id, name FROM users WHERE id = $1", 3).Scan(&id, &name); err != nil {
		fail("pgx parameterized SELECT", "%v", err)
	}
	assertRows("pgx parameterized SELECT", [][2]any{{id, name}}, [][2]any{{3, "Linus"}})

	if _, err := conn.Exec(ctx, "CREATE TABLE go_pgx_cert (id INT NOT NULL, label TEXT)"); err != nil {
		fail("pgx create table", "%v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO go_pgx_cert VALUES ($1, $2)", 1, "alpha"); err != nil {
		fail("pgx insert alpha", "%v", err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO go_pgx_cert VALUES ($1, $2)", 2, "beta"); err != nil {
		fail("pgx insert beta", "%v", err)
	}
	rows, err := conn.Query(ctx, "SELECT id, label FROM go_pgx_cert ORDER BY id")
	if err != nil {
		fail("pgx select inserted", "%v", err)
	}
	var selected [][2]any
	for rows.Next() {
		var rowID int
		var label string
		if err := rows.Scan(&rowID, &label); err != nil {
			fail("pgx scan inserted", "%v", err)
		}
		selected = append(selected, [2]any{rowID, label})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		fail("pgx inserted rows", "%v", err)
	}
	assertRows("pgx parameterized INSERT", selected, [][2]any{{1, "alpha"}, {2, "beta"}})

	tx, err := conn.Begin(ctx)
	if err != nil {
		fail("pgx begin rollback", "%v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO go_pgx_cert VALUES ($1, $2)", 3, "rollback"); err != nil {
		fail("pgx rollback insert", "%v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		fail("pgx rollback", "%v", err)
	}
	var count int
	if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM go_pgx_cert").Scan(&count); err != nil {
		fail("pgx rollback count", "%v", err)
	}
	assertOneInt("pgx explicit transaction rollback", count, 2)

	tx, err = conn.Begin(ctx)
	if err != nil {
		fail("pgx begin failed transaction", "%v", err)
	}
	if _, err := tx.Exec(ctx, "SELECT missing_column FROM go_pgx_cert"); err == nil {
		fail("pgx failed transaction", "expected missing-column failure")
	}
	if err := tx.Rollback(ctx); err != nil {
		fail("pgx failed transaction rollback", "%v", err)
	}
	if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM go_pgx_cert").Scan(&count); err != nil {
		fail("pgx recovery count", "%v", err)
	}
	assertOneInt("pgx recovery after error", count, 2)
}

func certifyGORM(ctx context.Context, dsn string) {
	db, err := gorm.Open(
		postgres.New(postgres.Config{
			DSN:                  dsn + "&application_name=driver_cert_gorm",
			PreferSimpleProtocol: false,
		}),
		&gorm.Config{},
	)
	if err != nil {
		fail("GORM startup", "%v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		fail("GORM sql db", "%v", err)
	}
	defer sqlDB.Close()

	if err := db.WithContext(ctx).AutoMigrate(&GormCert{}); err != nil {
		fail("GORM AutoMigrate", "%v", err)
	}

	var selected []struct {
		ID   int
		Name string
	}
	if err := db.WithContext(ctx).
		Table("users").
		Select("id, name").
		Where("id = ?", 1).
		Scan(&selected).Error; err != nil {
		fail("GORM parameterized SELECT", "%v", err)
	}
	if len(selected) != 1 || selected[0].ID != 1 || selected[0].Name != "Ada" {
		fail("GORM parameterized SELECT", "expected [(1, Ada)], got %#v", selected)
	}

	if err := db.WithContext(ctx).Create(&GormCert{ID: 1, Label: "alpha"}).Error; err != nil {
		fail("GORM create alpha", "%v", err)
	}
	if err := db.WithContext(ctx).Create(&GormCert{ID: 2, Label: "beta"}).Error; err != nil {
		fail("GORM create beta", "%v", err)
	}

	var rows []GormCert
	if err := db.WithContext(ctx).Order("id").Find(&rows).Error; err != nil {
		fail("GORM query inserted", "%v", err)
	}
	if len(rows) != 2 || rows[0].ID != 1 || rows[0].Label != "alpha" || rows[1].ID != 2 || rows[1].Label != "beta" {
		fail("GORM create/query", "unexpected rows %#v", rows)
	}

	if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&GormCert{ID: 3, Label: "rollback"}).Error; err != nil {
			return err
		}
		return fmt.Errorf("rollback GORM transaction")
	}); err == nil || err.Error() != "rollback GORM transaction" {
		fail("GORM transaction rollback", "expected rollback marker, got %v", err)
	}
	var count int64
	if err := db.WithContext(ctx).Model(&GormCert{}).Count(&count).Error; err != nil {
		fail("GORM rollback count", "%v", err)
	}
	if count != 2 {
		fail("GORM transaction rollback", "expected 2, got %d", count)
	}

	if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Exec("SELECT missing_column FROM gorm_cert").Error
	}); err == nil {
		fail("GORM failed transaction", "expected missing-column failure")
	}
	rows = rows[:0]
	if err := db.WithContext(ctx).Order("id").Find(&rows).Error; err != nil {
		fail("GORM recovery after error", "%v", err)
	}
	if len(rows) != 2 || rows[0].ID != 1 || rows[1].ID != 2 {
		fail("GORM recovery after error", "unexpected rows %#v", rows)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go_cert DSN")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	certifyLibPQ(ctx, os.Args[1])
	certifyPGX(ctx, os.Args[1])
	certifyGORM(ctx, os.Args[1])
}
