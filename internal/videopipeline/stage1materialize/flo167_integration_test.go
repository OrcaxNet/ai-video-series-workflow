//go:build integration

package stage1materialize

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFLO167MaterializesIdenticallyAcrossFreshPostgresAndReplay(t *testing.T) {
	primaryDSN := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	secondaryDSN := os.Getenv("VIDEO_TEST_POSTGRES_DSN_SECONDARY")
	if primaryDSN == "" || secondaryDSN == "" || os.Getenv("VIDEO_TEST_FLO167_RESTORED") != "1" {
		t.Skip("two databases restored from the frozen A01 stop archive are required")
	}
	packageBytes, err := os.ReadFile("../../../docs/flo-167/provider-free-execution-package.json")
	if err != nil {
		t.Fatal(err)
	}
	projectionBytes, err := os.ReadFile("../../../docs/flo-167/canonical-projection.json")
	if err != nil {
		t.Fatal(err)
	}
	var package_ stage1.FLO167SupersessionPackage
	var projection stage1.FLO167CanonicalProjection
	if json.Unmarshal(packageBytes, &package_) != nil || json.Unmarshal(projectionBytes, &projection) != nil {
		t.Fatal("decode delivered FLO-167 artifacts")
	}
	createdAt := time.Date(2026, 8, 3, 6, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	input := FLO167Materialization{LegacyActivationID: "142952f1-8dd1-5ebe-99c8-f2cb538ac702", Package: package_, Projection: projection, CreatedAt: createdAt}
	authorizationPayload, err := json.Marshal(map[string]any{
		"schemaVersion":           "flo100.batch-a-continuation-authorization.v3",
		"supersessionPackageHash": package_.ContentHash,
		"canonicalProjectionHash": projection.ContentHash,
		"pricingSnapshotDigest":   package_.Shots[0].Pricing.PricingSnapshotDigest,
		"decision": map[string]bool{"a02A10ProviderPostAuthorizedConditionally": true,
			"batchBProviderPostAuthorized": false, "batchCProviderPostAuthorized": false, "stage4Authorized": false},
	})
	if err != nil {
		t.Fatal(err)
	}
	authorization := FLO167Authorization{LegacyActivationID: input.LegacyActivationID,
		Payload: authorizationPayload, IssuedAt: createdAt.Add(time.Minute), ValidUntil: createdAt.Add(24 * time.Hour)}

	ctx := context.Background()
	var canonical []byte
	for index, dsn := range []string{primaryDSN, secondaryDSN} {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		if err := MaterializeFLO167Supersession(ctx, pool, input); err != nil {
			pool.Close()
			t.Fatalf("database %d first materialization: %v", index+1, err)
		}
		if err := MaterializeFLO167Supersession(ctx, pool, input); err != nil {
			pool.Close()
			t.Fatalf("database %d exact replay: %v", index+1, err)
		}
		if err := AuthorizeFLO167Supersession(ctx, pool, authorization); err != nil {
			pool.Close()
			t.Fatalf("database %d authorization: %v", index+1, err)
		}
		if err := AuthorizeFLO167Supersession(ctx, pool, authorization); err != nil {
			pool.Close()
			t.Fatalf("database %d authorization replay: %v", index+1, err)
		}
		var stored []byte
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
			'package',package,'projection',canonical_projection,'packageHash',execution_package_hash,
			'projectionHash',canonical_projection_hash,'state',state,'authorizationHash',authorization_hash,
			'shots',(SELECT jsonb_agg(to_jsonb(ss) ORDER BY ordinal)
			         FROM video_pipeline.stage1_live_supersession_shots ss WHERE ss.supersession_id=s.id))
			FROM video_pipeline.stage1_live_supersessions s WHERE legacy_activation_id=$1`, input.LegacyActivationID).Scan(&stored); err != nil {
			pool.Close()
			t.Fatal(err)
		}
		pool.Close()
		var value any
		if json.Unmarshal(stored, &value) != nil {
			t.Fatal("decode stored canonical projection")
		}
		normalized, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			canonical = normalized
		} else if !reflect.DeepEqual(canonical, normalized) {
			t.Fatal("fresh PostgreSQL materializations produced different canonical bytes")
		}
	}
}
