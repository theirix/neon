import os
import statistics
import threading
import time
import timeit
from random import randint, randrange

import pytest
from batch_others.test_backpressure import pg_cur
from fixtures import log_helper
from fixtures.benchmark_fixture import MetricReport
from fixtures.compare_fixtures import PgCompare
from fixtures.neon_fixtures import Postgres

from performance.test_perf_pgbench import get_scales_matrix

# Number of tables to use in test_measure_read_latency_heavy_write_workload.
n_tables = 100

# Array containing the # of rows, for each table in range 0..n_tables
n_rows = []

# Number of threads to use
n_threads = 10

def start_heavy_write_workload(pg: Postgres, scale: int = 1, n_iters: int = 100):
    """Start an intensive write workload which first initializes a table `t` with `new_rows_each_update` rows.
    At each subsequent step, we update a subset of rows in the table and insert new `new_rows_each_update` rows.
    The variable `new_rows_each_update` is equal to `scale * 100_000`."""
    global n_rows
    global n_tables

    new_rows_each_update = scale * 100_000

    for _ in range(n_iters):
        table = randrange(n_tables)
        with pg_cur(pg) as cur:
            cur.execute(
                f"INSERT INTO t{table} (cnt) SELECT 0 FROM generate_series(1,{new_rows_each_update}) s"
            )
        n_rows[table] += new_rows_each_update


@pytest.mark.parametrize("scale", get_scales_matrix(1))
def test_measure_read_latency_heavy_write_workload(neon_with_baseline: PgCompare, scale: int):
    global n_tables

    env = neon_with_baseline
    pg = env.pg

    # Initialize the test tables
    with pg_cur(pg) as cur:
        for table in range(n_tables):
            cur.execute(f"CREATE TABLE t{table}(key serial primary key, cnt int, filler text default 'foooooooooooooooooooooooooooooooooooooooooooooooooooo')")
            cur.execute(f"INSERT INTO t{table} (cnt) values (0)")
            n_rows.append(1)

    for _ in range(n_threads):
        write_thread = threading.Thread(target=start_heavy_write_workload, args=(pg, ))
        write_thread.start()

    record_read_latency(env, write_thread, get_query)

def get_query():
    global n_rows
    tab = randrange(n_tables)
    n = n_rows[tab]
    key = randint(1, n)
    return f"SELECT * FROM t{tab} WHERE key = {key}"

def start_pgbench_simple_update_workload(env: PgCompare, scale: int, transactions: int):
    env.pg_bin.run_capture(['pgbench', '-j10', '-c10', '-N', '-T120', '-Mprepared', env.pg.connstr(options="-csynchronous_commit=off")])
    env.flush()


def get_transactions_matrix(default: int = 100_000):
    scales = os.getenv("TEST_PG_BENCH_TRANSACTIONS_MATRIX", default=str(default))
    return list(map(int, scales.split(",")))


@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("transactions", get_transactions_matrix())
def test_measure_read_latency_pgbench_simple_update_workload(neon_with_baseline: PgCompare,
                                                             scale: int,
                                                             transactions: int):
    env = neon_with_baseline

    # create pgbench tables
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', '-Idt', env.pg.connstr()])
    env.flush()
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', '-Igvp', env.pg.connstr()])
    env.flush()

    write_thread = threading.Thread(target=start_pgbench_simple_update_workload,
                                    args=(env, scale, transactions))
    write_thread.start()

    record_read_latency(env, write_thread, get_pgbench_query)

def get_pgbench_query():
    aid = randint(1, 1000000)
    return f"SELECT * FROM pgbench_accounts WHERE aid = {aid}"

def start_pgbench_intensive_initialization(env: PgCompare, scale: int):
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
    env.flush()


@pytest.mark.parametrize("scale", get_scales_matrix(1000))
def test_measure_read_latency_other_table(neon_with_baseline: PgCompare, scale: int):
    # Measure the latency when reading from a table when doing a heavy write workload in another table
    # This test tries to simulate the scenario described in https://github.com/neondatabase/neon/issues/1763.

    env = neon_with_baseline
    with pg_cur(env.pg) as cur:
        cur.execute("CREATE TABLE foo(key int primary key, i int)")
        cur.execute(f"INSERT INTO foo SELECT s, s FROM generate_series(1,100000) s")

    write_thread = threading.Thread(target=start_pgbench_intensive_initialization,
                                    args=(env, scale))
    write_thread.start()

    record_read_latency(env, write_thread,
                        lambda: "SELECT count(*) from foo")


def record_read_latency(env: PgCompare, write_thread: threading.Thread, read_query_func):
    with env.record_duration("run_duration"):
        read_latencies = []
        while write_thread.is_alive():
            time.sleep(1.0)

            with pg_cur(env.pg) as cur:
                t = timeit.default_timer()
                read_query = read_query_func()
                cur.execute(read_query)
                duration = timeit.default_timer() - t

                log_helper.log.info(
                    f"Executed read query {read_query}, got {cur.fetchall()}, took {duration}")

                read_latencies.append(duration)

    env.zenbenchmark.record("read_latency_avg",
                            statistics.mean(read_latencies),
                            's',
                            MetricReport.LOWER_IS_BETTER)
    env.zenbenchmark.record("read_latency_stdev",
                            statistics.stdev(read_latencies),
                            's',
                            MetricReport.LOWER_IS_BETTER)
