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

	switch outcome {
	case lifecycle.OutcomeRan:
		fmt.Println("this pod ran the migration")
	case lifecycle.OutcomeSkippedCompleted:
		fmt.Println("migration already done for this version, skipping")
	case lifecycle.OutcomeSkippedRunning:
		fmt.Println("another pod is running the migration, skipping")
	case lifecycle.OutcomeSkippedFailed:
		fmt.Println("migration is in failed state, manual intervention needed")
	case lifecycle.OutcomeFailed:
		// This pod ran the migration but it errored (or panicked). With the
		// default WithRetryable(false) the hook is now in terminal failed
		// state and needs a Reset (or version bump) before it runs again.
		fmt.Println("this pod ran the migration and it failed:", err)
	case lifecycle.OutcomeCompleteFailed:
		// The migration succeeded but the bookkeeping write did not; the work
		// happened, only the completion record is wedged until lease expiry.
		fmt.Println("migration ran but completion was not recorded:", err)
	default:
		// Argument-validation failures return an empty outcome and an error.
		fmt.Println("migration could not start:", err)
	}
	// Output: this pod ran the migration
}
