// Package saga provides saga pattern implementation for distributed transactions.
//
// # Error Handling
//
// This package uses shared error types from github.com/rbaliyan/event/v3/errors
// for consistent error handling across the event ecosystem.
//
// Version Conflicts:
//
// Saga stores use optimistic locking via the Version field. When concurrent
// updates occur, ErrVersionConflict is returned. Handle with retry logic:
//
//	state, err := store.Get(ctx, sagaID)
//	if err != nil {
//	    return err
//	}
//	state.Status = saga.StatusRunning
//	if err := store.Update(ctx, state); err != nil {
//	    if errors.Is(err, saga.ErrVersionConflict) {
//	        // Retry: re-read state and try again
//	    }
//	    return err
//	}
//
// Not Found Errors:
//
// When a saga is not found, stores return an error that can be checked with:
//
//	if eventerrors.IsNotFound(err) {
//	    // Handle not found
//	}
package saga

import (
	eventerrors "github.com/rbaliyan/event/v3/errors"
)

// IsVersionConflict checks if an error indicates a version conflict.
// This occurs when optimistic locking fails due to concurrent modifications.
func IsVersionConflict(err error) bool {
	return eventerrors.IsVersionConflict(err)
}

// IsNotFound checks if an error indicates a saga was not found.
func IsNotFound(err error) bool {
	return eventerrors.IsNotFound(err)
}
