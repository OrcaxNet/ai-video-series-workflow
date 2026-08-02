//go:build integration

package repository

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestFLO167MigrationRejectsSkippedAndBackwardTransitions(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("VIDEO_TEST_POSTGRES_DSN is required")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE flo167_state_probe (state text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `CREATE TRIGGER flo167_state_probe_guard
		BEFORE UPDATE OF state ON flo167_state_probe FOR EACH ROW
		WHEN (OLD.state IS DISTINCT FROM NEW.state)
		EXECUTE FUNCTION video_pipeline.guard_flo167_state_transition()`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO flo167_state_probe VALUES ('supersession_package_pending_v3')`); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"v3_authorized_A02_A10", "quota_reserved", "A02_submitted"} {
		if _, err := tx.Exec(ctx, `UPDATE flo167_state_probe SET state=$1`, state); err != nil {
			t.Fatalf("valid transition to %s failed: %v", state, err)
		}
	}

	assertRejected := func(name, from, to string) {
		t.Helper()
		if _, err := tx.Exec(ctx, `SAVEPOINT flo167_negative`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `ALTER TABLE flo167_state_probe DISABLE TRIGGER flo167_state_probe_guard`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE flo167_state_probe SET state=$1`, from); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `ALTER TABLE flo167_state_probe ENABLE TRIGGER flo167_state_probe_guard`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE flo167_state_probe SET state=$1`, to); err == nil {
			t.Fatalf("%s transition %s -> %s was accepted", name, from, to)
		}
		if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT flo167_negative`); err != nil {
			t.Fatal(err)
		}
	}
	assertRejected("skip authorization", "supersession_package_pending_v3", "quota_reserved")
	assertRejected("skip quota", "v3_authorized_A02_A10", "A02_submitted")
	assertRejected("backward", "A02_submitted", "quota_reserved")
}
