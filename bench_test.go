package pg_pl_bench_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

var loopCounts []int32

func init() {
	loopCounts = []int32{1000000}
}

func WithConn(b *testing.B, f func(ctx context.Context, conn *pgx.Conn)) {
	ctx := context.Background()

	config, err := pgx.ParseConfig(os.Getenv("DATABASE_URL"))
	require.NoError(b, err)

	config.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		b.Logf("PostgreSQL %s: %s", n.Severity, n.Message)
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(b, err)
	defer CloseConn(b, conn)

	b.ResetTimer()
	f(ctx, conn)
	b.StopTimer()
}

func CloseConn(t testing.TB, conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
}

func Benchmark_Select_1(b *testing.B) {
	WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(ctx, "select 1")
			rows.Close()
			err := rows.Err()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Batch_Select_1000000(b *testing.B) {
	batch := &pgx.Batch{}
	batchSize := 1000000
	for i := 0; i < batchSize; i++ {
		batch.Queue("select 1")
	}

	WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
		for i := 0; i < b.N; i++ {
			br := conn.SendBatch(ctx, batch)
			err := br.Close()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_PlPgSQL_Loop_Select(b *testing.B) {
	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d loops", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select plpgsql_loop_n_select($1)", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func Benchmark_PlPgSQL_Empty_Loop(b *testing.B) {
	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d loops", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select plpgsql_empty_loop_n($1)", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func Benchmark_PlPgSQL_Loop_Select_Increment(b *testing.B) {
	loopCounts := []int32{100, 10000, 1000000}

	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d loops", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select plpgsql_loop_n_select_increment($1)", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func Benchmark_PlPgSQL_Loop_Assign_Increment(b *testing.B) {
	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d loops", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select plpgsql_loop_n_assign_increment($1)", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func Benchmark_PlPerl_Loop_Increment(b *testing.B) {
	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d loops", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select perl_loop_n_increment($1)", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func Benchmark_PlPgSQL_Loop_Call_PlPgSQL_Add(b *testing.B) {
	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d loops", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select plpgsql_loop_call_plpgsql_add($1)", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func Benchmark_PlPgSQL_Loop_Call_SQL_Add(b *testing.B) {
	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d loops", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select plpgsql_loop_call_sql_add($1)", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func Benchmark_PlPgSQL_Loop_Call_Perl_Add(b *testing.B) {
	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d loops", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select plpgsql_loop_call_perl_add($1)", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func Benchmark_Select_Call_PlPgSQL_Add(b *testing.B) {
	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d rows", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select plpgsql_add(n, 1) from generate_series(1, $1) n", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func Benchmark_Select_Call_SQL_Add(b *testing.B) {
	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d rows", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select sql_add(n, 1) from generate_series(1, $1) n", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func Benchmark_Select_Call_Perl_Add(b *testing.B) {
	for _, loopCount := range loopCounts {
		b.Run(fmt.Sprintf("%d rows", loopCount), func(b *testing.B) {
			WithConn(b, func(ctx context.Context, conn *pgx.Conn) {
				for i := 0; i < b.N; i++ {
					rows, _ := conn.Query(ctx, "select perl_add(n, 1) from generate_series(1, $1) n", loopCount)
					rows.Close()
					err := rows.Err()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
