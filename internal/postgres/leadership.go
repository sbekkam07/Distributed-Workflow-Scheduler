package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schedulerAdvisoryLockKey identifies the one leader-only scheduler operation.
// PostgreSQL scopes advisory locks to a database session, so a lost session
// automatically releases leadership.
const schedulerAdvisoryLockKey int64 = 0x44575309

// SchedulerElector obtains PostgreSQL-backed leadership for scheduler work.
// It keeps a pool connection checked out for the entire leadership term because
// session advisory locks cannot safely move between pooled connections.
type SchedulerElector struct {
	pool *pgxpool.Pool
}

// NewSchedulerElector constructs the PostgreSQL implementation of the
// scheduler election boundary.
func NewSchedulerElector(pool *pgxpool.Pool) *SchedulerElector {
	return &SchedulerElector{pool: pool}
}

// SchedulerLeadership represents an acquired PostgreSQL session advisory lock.
// It is not safe to use after Release.
type SchedulerLeadership struct {
	connection *pgxpool.Conn
}

// TryAcquire tries to become the scheduler leader without waiting. At most one
// session can hold this advisory lock in the database at a time.
func (e *SchedulerElector) TryAcquire(ctx context.Context) (*SchedulerLeadership, bool, error) {
	if e == nil || e.pool == nil {
		return nil, false, fmt.Errorf("scheduler pool is required")
	}
	connection, err := e.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire scheduler connection: %w", err)
	}
	var acquired bool
	if err := connection.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", schedulerAdvisoryLockKey).Scan(&acquired); err != nil {
		connection.Release()
		return nil, false, fmt.Errorf("try scheduler advisory lock: %w", err)
	}
	if !acquired {
		connection.Release()
		return nil, false, nil
	}
	return &SchedulerLeadership{connection: connection}, true, nil
}

// ResolveBlocked runs the leader-only dependency failure reconciliation on the
// same session that holds the advisory lock. A database disconnect cannot leave
// a stale leader session holding the lock.
func (l *SchedulerLeadership) ResolveBlocked(ctx context.Context) (int64, error) {
	if l == nil || l.connection == nil {
		return 0, fmt.Errorf("scheduler leadership is not active")
	}
	return resolveBlocked(ctx, l.connection)
}

// Release relinquishes leadership. If unlock cannot be confirmed, the
// connection is closed rather than returned to the pool, so PostgreSQL releases
// any session lock before another pool user can receive that connection.
func (l *SchedulerLeadership) Release(ctx context.Context) error {
	if l == nil || l.connection == nil {
		return nil
	}
	connection := l.connection
	l.connection = nil

	var released bool
	err := connection.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", schedulerAdvisoryLockKey).Scan(&released)
	if err != nil || !released {
		_ = connection.Conn().Close(context.Background())
		connection.Release()
		if err != nil {
			return fmt.Errorf("release scheduler advisory lock: %w", err)
		}
		return fmt.Errorf("scheduler advisory lock was not held")
	}
	connection.Release()
	return nil
}
