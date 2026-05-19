package lifecycle_test

import (
	"context"
	"fmt"

	"github.com/rbaliyan/event-extras/lifecycle"
)

// Example_onDeploymentStart demonstrates the canonical use case: a
// database migration that must run on exactly one pod per deployment.
func Example_onDeploymentStart() {
	// In production use one of NewRedisStore / NewPostgresStore /
	// NewMongoStore — MemoryStore only coordinates within a single process.
	store := lifecycle.NewMemoryStore()
	ctx := context.Background()

	// Version comes from your build metadata (e.g. github.com/rbaliyan/go-version).
	hook := lifecycle.Hook{
		Name:    "users-table-migration",
		Version: "v1.2.3",
	}

	outcome, err := lifecycle.Once(ctx, store, hook, func(ctx context.Context) error {
		// runMigration(ctx, db)
		return nil
	})
	if err != nil {
		fmt.Println("migration error:", err)
		return
	}

	switch outcome {
	case lifecycle.OutcomeRan:
		fmt.Println("this pod ran the migration")
	case lifecycle.OutcomeSkippedCompleted:
		fmt.Println("migration already done for this version, skipping")
	case lifecycle.OutcomeSkippedRunning:
		fmt.Println("another pod is running the migration, skipping")
	case lifecycle.OutcomeSkippedFailed:
		fmt.Println("migration is in failed state, manual intervention needed")
	}
	// Output: this pod ran the migration
}
