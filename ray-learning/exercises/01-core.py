from posix import read
from sys import path
from this import s

from pandas.core.generic import clean_reindex_fill_method
# REVIEW: none of these four imports are used anywhere below (`path`
# stops being the sys.path module the moment it's reassigned further
# down anyway) — `posix.read` / `this.s` look like stray autocomplete
# picks, and pandas.core.generic is a private module even where it isn't
# unused. Worth cutting all four.

import ray
import pandas as pd
from pathlib import Path


ray.init()
# Starts a local Ray cluster inside this process: a head process, GCS
# (cluster metadata), a shared-memory object store, and a raylet that
# schedules tasks/actors onto worker processes. Every .remote() call and
# ray.wait/ray.get below talks to this.

#------
# Ray actor
# ------
@ray.remote
# @ray.remote on a class makes it an Actor definition. RunTracker.remote()
# (further down) does NOT run this code in the driver — it spawns a
# dedicated worker PROCESS holding one long-lived instance, and every
# .method.remote() call on the handle queues onto that same process, one
# at a time. That serialization is how Ray gives you safe shared mutable
# state instead of a race between concurrent tasks.
class RunTracker:
    """
    Tracks mutable state for this ingestion run.
    One actor isntance = one explicit owner of these coutners
    """

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        # REVIEW: `Any` isn't imported anywhere (no `from typing import
        # Any`) — annotations on a def are evaluated immediately, so this
        # raises NameError the moment Python defines the class, before
        # ray.init() or anything else below gets a chance to run. Once
        # that import exists, there's a second question: which dunder
        # does Python (and Ray) actually invoke to set up initial state
        # on a freshly constructed object — is it __call__, or something
        # else Ray calls for you at RunTracker.remote() time?
        self.files_successed = 0
        self.files_failed = 0
        self.row_read = 0
        self.row_written = 0
        self.bad_rows = 0
        self.failures = 0
        # REVIEW: `failures` starts as an int, but record_failure below
        # calls .append() on it. What should an accumulating list of
        # (path, error) pairs actually start out as?

    def record_success(self, result):
        # Meant to be called once per finished process_file task, folding
        # that file's result dict into this run's running totals.
        self.files_successed += 1
        self.rows_read += result["row_read"]
        # REVIEW: two separate mismatches on this one line —
        #   1. self.rows_read (plural) vs what's actually set above,
        #      self.row_read (singular) — not the same attribute.
        #   2. result["row_read"] (singular key) — check the key names
        #      process_file's `return {...}` really uses below.
        self.rows_written += result["rows_written"]
        self.bad_rows = result["bad_rows"]
        # REVIEW: every other counter on this method accumulates (+=)
        # across every file processed this run; this one overwrites (=)
        # instead. Given the tracker's job is a running total for the
        # whole run, which behavior does bad_rows actually need?

    def record_failure(self, path, error):
        self.files_failed += 1
        self.failures.append((path, error))

    def summary(self):
        # Pulls the actor's accumulated counters back out to the driver —
        # only reachable via ray.get() on the ObjectRef this method's
        # own .remote() call returns (see the bottom of the file).
        return {
            "files_successed": self.files_successed,
            "files_failed": self.files_failed,
            "row_read": self.row_read,
            "row_written": self.row_written,
            "bad_rows": self.bad_rows,
            "failures": self.failures,
        }

@ray.remote
# @ray.remote on a function makes it a Task. Each .remote() call below
# runs one invocation on whatever worker process is free, in parallel
# with every other call currently in flight, and hands back an
# ObjectRef — a future — immediately, without blocking for the result.
def process_file(input_path: str, output_dir: str) -> dict:
    """
    One indepent DE unit work.
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    #---
    # Extract
    #---
    df = pd.read_parquet(input_path)
    rows_read = len(df)

    #---
    # Validate
    #---
    required_columns ={
        "event_id",
        "user_id",
        "event_time",
        "amount"
    }

    missing = required_columns - set(df.columns)
    if missing:
        # A task's exception is stored, not raised into the driver — it
        # only surfaces once something calls ray.get() on this task's
        # ObjectRef. The try/except further down does exactly that, and
        # is what's meant to route it to record_failure.
        raise ValueError(f"Missing columns: {missing}")

    #---
    # Transform
    #---
    valid = (
        df["event_id"].notna() &
        df["user_id"].notna() &
        df["event_time"].notna() &
        df["amount"] > 0
    )
    # REVIEW: classic pandas trap. Plain Python `and`/`or` bind looser
    # than comparisons, but `&` is the opposite — it binds TIGHTER than
    # `>`. So this doesn't parse as
    #   ...notna() & (df["amount"] > 0)
    # it parses as
    #   (...notna() & df["amount"]) > 0
    # — bitwise-ANDing a bool column against a numeric column before the
    # comparison ever happens. Every operand around a chained `&`/`|`
    # mask needs its own parens. What does the un-parenthesized version
    # actually do when it runs against a real amount column?

    clean_df = df[valid].copy()

    clean_df["event_date"] = (
        pd.to_datetime(clean_df["event_time"])
        .dt.date
    )

    rows_written = len(clean_df)
    bad_rows = rows_read - rows_written

    #---
    # Load
    #---
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / input_path.name
    clean_df.to_parquet(output_path, index=False)

    return {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "rows_read": rows_read,
        "rows_written": rows_written,
        "bad_rows": bad_rows,
    }
    # These are the exact keys record_success reads back out as `result`
    # — this dict is the source of truth; record_success has to match
    # it, not the other way around.

tracker = RunTracker.remote()
# Spawns the actor's worker process and returns a handle — from here on
# you never touch a RunTracker instance directly, only through
# tracker.<method>.remote(...).

files = [
    f"/data/raw/part-{i:05d}.parquet"
    for i in range(10000)
]
# REVIEW: none of these paths exist on disk, so every process_file call
# will fail at pd.read_parquet with FileNotFoundError — caught below and
# routed to record_failure, so the script won't crash from this, but it
# also can't prove the success path (or the row-count bugs above) ever
# works. ray-learning/ already has a generator for real parquet files —
# SETUP.md step 4, `make -f ray-learning/Makefile data` — or point this
# at a handful of tiny files you write yourself for a quick local run.

output_dir = "/data/processed"

file_iter = iter(files)
# An iterator, not the list — lets the loop below pull "the next file"
# one at a time as slots free up, without hand-tracking an index.

MAX_IN_FLIGHT = 20
# The reason for ray.wait instead of one big ray.get(all_refs) at the
# end: cap concurrent tasks at 20 instead of submitting all 10000 at
# once. This cap is backpressure — the whole point of this exercise.

in_flight = {}
# Maps ObjectRef -> the input_path that ref belongs to, so that once
# ray.wait says "this ref is ready," the loop still knows which file it
# was.

for _ in range(MAX_IN_FLIGHT):
    try:
        input_path = next(file_iter)
    except StopIteration:
        break
    # Priming loop: get the first 20 tasks running before the drain loop
    # below even starts.

    ref = process_file.remote(
        input_path,
        output_dir,
    )
    # Returns immediately with a future — process_file may not have even
    # started executing yet.

    in_flight[ref] = input_path

while in_flight:
    read_refs, _ = ray.wait(
        list(in_flight.keys()),
        num_returns=1,
    )
    # Unlike ray.get, ray.wait doesn't block for everything — only until
    # `num_returns` of the given refs are ready — and hands back
    # (ready_refs, still_running_refs) so you can act on what's done
    # without waiting on the stragglers. It does NOT remove anything from
    # `in_flight` for you; that's the job of the next two lines.

    read_ref = read_refs[0]

    path = in_flight.pop(read_ref)
    # REVIEW: this reassigns the module-level `path` name that
    # `from sys import path` brought in at the top — one more reason
    # that import needs to go.
    try:
        result = ray.get(read_ref)
        tracker.record_success.remote(result)
    except Exception as exc:
        tracker.record_failure.remote(path, str(exc))
        # REVIEW: tracker.record_success.remote(...) / record_failure
        # .remote(...) each return their own ObjectRef, which nothing
        # here ever ray.get()s. An actor CALL nobody ray.get()s can raise
        # inside the actor process and the driver will never see it — it
        # just disappears into Ray's logs. Once the __call__/attribute
        # bugs above are fixed, keep that in mind if summary() ever comes
        # back looking wrong with no visible crash to explain it.

    try:
        next_path = next(file_iter)
        ref = process_file.remote(
            input_path,
            output_dir,
        )
        in_flight[ref] = input_path
    except StopIteration:
        pass
    # REVIEW: `next_path` is pulled from file_iter and then never used —
    # both process_file.remote(...) and in_flight[ref] still reference
    # `input_path`, the stale variable last set by the priming for-loop
    # above. Walk through what `input_path` equals on the 2nd, 3rd, 4th
    # time through this while loop, versus what `next_path` equals each
    # time — same value, or not? This one won't crash anything; it'll
    # just quietly resubmit one file forever while files 20 through 9999
    # never actually get processed.

#Get the final actor state
sumamry_ref = tracker.summary.remote()
summary = ray.get(summary_ref)
# REVIEW: `summary_ref` here doesn't match the name actually assigned
# right above it (`sumamry_ref`) — NameError. Separately: once that's
# fixed, this particular ray.get() is the one place an actor-side
# AttributeError from the __call__/init bug would actually surface in
# the driver (see the note a few lines up) — expect to meet that error
# here next, if it's still unfixed by the time you get this far.

print(summary)
# Optional: ray.shutdown() here releases the local cluster explicitly
# instead of relying on Ray's atexit hook — a good habit once a session
# runs more than one script back to back.
