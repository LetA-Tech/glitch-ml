"""
Ray core demo: remote actor + ray.wait/ray.get bounded-concurrency pipeline.

Process a list of files with at most MAX_IN_FLIGHT tasks running at once
(backpressure), tally results in a Ray actor, and drain completions with
ray.wait instead of blocking on the whole batch with one ray.get(all_refs).
"""

import ray
import pandas as pd
from pathlib import Path


@ray.remote
class RunTracker:
    """
    One actor instance = one process = one owner of these counters.
    Ray runs calls to the same actor one at a time, in submission order,
    so record_success/record_failure never race each other even with
    many tasks finishing concurrently — no lock needed.
    """

    def __init__(self):
        self.files_succeeded = 0
        self.files_failed = 0
        self.rows_read = 0
        self.rows_written = 0
        self.bad_rows = 0
        self.failures = []

    def record_success(self, result: dict) -> None:
        self.files_succeeded += 1
        self.rows_read += result["rows_read"]
        self.rows_written += result["rows_written"]
        self.bad_rows += result["bad_rows"]

    def record_failure(self, input_path: str, error: str) -> None:
        self.files_failed += 1
        self.failures.append((input_path, error))

    def summary(self) -> dict:
        return {
            "files_succeeded": self.files_succeeded,
            "files_failed": self.files_failed,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "bad_rows": self.bad_rows,
            "failures": self.failures,
        }


@ray.remote
def process_file(input_path: str, output_dir: str) -> dict:
    """
    One independent unit of DE work: extract -> validate -> transform ->
    load. Runs in its own worker process. Raising here only marks THIS
    task's ObjectRef as failed — it doesn't touch the driver or any
    other in-flight task.
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    # Extract
    df = pd.read_parquet(input_path)
    rows_read = len(df)

    # Validate
    required_columns = {"event_id", "user_id", "event_time", "amount"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # Transform. Note the parens around each side of a chained pandas
    # `&` mask: `&` binds TIGHTER than `>`, so `x.notna() & y > 0` would
    # actually parse as `(x.notna() & y) > 0` — not what you want.
    valid = (
        df["event_id"].notna()
        & df["user_id"].notna()
        & df["event_time"].notna()
        & (df["amount"] > 0)
    )
    clean_df = df[valid].copy()
    clean_df["event_date"] = pd.to_datetime(clean_df["event_time"]).dt.date

    rows_written = len(clean_df)
    bad_rows = rows_read - rows_written

    # Load
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


def run(files: list[str], output_dir: str, max_in_flight: int = 20) -> dict:
    """
    Bounded-concurrency drain loop. Keeps at most `max_in_flight`
    process_file tasks running at once — ray.wait is what makes that
    possible without submitting (and holding refs to) all of `files` at
    once.
    """
    tracker = RunTracker.remote()
    file_iter = iter(files)
    in_flight: dict[ray.ObjectRef, str] = {}

    def submit_next() -> None:
        try:
            path = next(file_iter)
        except StopIteration:
            return
        ref = process_file.remote(path, output_dir)
        in_flight[ref] = path

    # Prime the pipeline: get max_in_flight tasks running before the
    # drain loop below even starts.
    for _ in range(max_in_flight):
        submit_next()

    while in_flight:
        # Block only until ONE ref is ready — not the whole batch.
        # Returns (ready refs, still-running refs); it does NOT remove
        # anything from in_flight for you, that's the next line.
        ready_refs, _ = ray.wait(list(in_flight.keys()), num_returns=1)
        ready_ref = ready_refs[0]
        finished_path = in_flight.pop(ready_ref)

        try:
            # The value is already sitting in the object store — ray.wait
            # just confirmed that — so this returns immediately. Any
            # exception process_file raised gets re-raised here.
            result = ray.get(ready_ref)
            tracker.record_success.remote(result)
        except Exception as exc:
            tracker.record_failure.remote(finished_path, str(exc))

        # Replenish: one new task in for every one that just finished, so
        # in_flight stays near max_in_flight until file_iter runs dry.
        submit_next()

    # record_success/record_failure above were fire-and-forget — never
    # ray.get()'d individually. Safe: since Ray serializes calls to the
    # SAME actor in submission order, every one of them has already run
    # by the time this final call's result comes back.
    return ray.get(tracker.summary.remote())


if __name__ == "__main__":
    ray.init()

    files = [f"/data/raw/part-{i:05d}.parquet" for i in range(10000)]
    summary = run(files, output_dir="/data/processed", max_in_flight=20)
    print(summary)

    ray.shutdown()
