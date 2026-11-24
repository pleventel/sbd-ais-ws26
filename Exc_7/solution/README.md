# Exercise 7 – Solution: From the log to the lake

## Step 0 & 1: Starting point: the example job and why the checker fails

The example job (notebook section 1) is:

```python
def run_once():
    new = read_new(None)          # None = from the beginning of the topic
    write_files(new, LAKE)        # straight into the lake
```

Output of the first run + check (cell 4):

~~~text
reset: lake, staging and state deleted
read_new: 2,000,000 new records
write_files: 2,000,000 records -> /home/jovyan/data/lake/logs/run-20261124-104743-e49571

Lake check — topic 'logs' (4 partitions)   2026-11-24 10:48:24
lake: read_parquet('/home/jovyan/data/lake/logs/**/*.parquet')

partition          topic offsets     in lake  duplicates    missing  not loaded
-------------------------------------------------------------------------------
        0           [0, 566,592)     566,592           0          0           0
        1           [0, 453,786)     453,786           0          0           0
        2           [0, 519,035)     519,035           0          0           0
        3           [0, 460,587)     460,587           0          0           0
-------------------------------------------------------------------------------
    total      2,000,000 records   2,000,000           0          0           0

files under /home/jovyan/data/lake: 4 data files, 111.4 MB, 27.86 MB per file on average
check took 0.1 s

Exactly once: PASS
{'topic': 2000000,
 'lake': 2000000,
 'dup': 0,
 'miss': 0,
 'lag': 0,
 'files': 4,
 'mb': 111.431943,
 'ok': True,
 'problems': []}
~~~

Output of the second run + check (cell 5):

~~~text
read_new: 2,000,000 new records
write_files: 2,000,000 records -> /home/jovyan/data/lake/logs/run-20261124-104825-93131a

Lake check — topic 'logs' (4 partitions)   2026-11-24 10:48:48
lake: read_parquet('/home/jovyan/data/lake/logs/**/*.parquet')

partition          topic offsets     in lake  duplicates    missing  not loaded
-------------------------------------------------------------------------------
        0           [0, 566,592)   1,133,184     566,592          0           0
        1           [0, 453,786)     907,572     453,786          0           0
        2           [0, 519,035)   1,038,070     519,035          0           0
        3           [0, 460,587)     921,174     460,587          0           0
-------------------------------------------------------------------------------
    total      2,000,000 records   4,000,000   2,000,000          0           0

files under /home/jovyan/data/lake: 8 data files, 222.9 MB, 27.86 MB per file on average
check took 0.1 s

Exactly once: FAIL — 2,000,000 duplicates
{'topic': 2000000,
 'lake': 4000000,
 'dup': 2000000,
 'miss': 0,
 'lag': 0,
 'files': 8,
 'mb': 222.863886,
 'ok': False,
 'problems': ['2,000,000 duplicates']}
~~~

The job has no memory. It reads from offset 0 every time, so the second run writes the same 2,000,000 records into a new run folder. Every `(kafka_partition, kafka_offset)` pair is then in the lake twice. The checker therefore reports duplicates but no missing records.

## Step 2: Choose an architecture: Architecture B ("the lake is the truth")

| | A: state file | B: lake is the truth | C: dedup on read |
| --- | --- | --- | --- |
| Order | read, write into lake, `save_state` | position from lake, read, write into staging, `publish` | like A; readers use `DEDUP_SQL` |
| Crash while writing | half a run is visible in the lake and the state is unchanged, so the next run writes it again: duplicates | only staging is dirty, the lake is untouched, next run redoes it cleanly | like A, but the view hides the duplicates |
| Crash after writing | the whole run is in the lake, state not saved: duplicates | nothing was published, so the run is redone | like A, hidden by the view |
| Weak spot | lake and state file are two things that can disagree | needs a single writer and an atomic rename | duplicates stay in the files; every reader must use the view and pays for it on every query |

A job does three things: read, write, remember. The question is where it remembers.

- **Why not A:** the position lives in a second place next to the lake, and a process cannot update both in one step. If the job dies after writing but before saving the state, the next run starts from the old position and repeats work (duplicates). Saving the state first would instead lose records after a crash.
- **Why not C:** the duplicates stay in the files. Every reader has to remember to use the dedup view, every query pays for the deduplication, and storage grows with every crash.
- **Why B:** there is no second place. The job derives its position from the lake itself (`MAX(kafka_offset) + 1` per partition) and writes every run into a staging folder first. `publish()` then moves the finished folder into the lake with one rename. The lake therefore contains only complete runs, and the position is always exactly what the lake contains. Readers need no special view.

## Step 3: implement `run_once()`

```python
def run_once():
    start = lake_position()            # 1. where did the last run stop? Ask the lake.
    new = read_new(start)              # 2. read from there up to the end of the topic now
    run = write_files(new, STAGING)    # 3. write into staging, which readers never look at
    publish(run)                       # 4. one rename moves the finished run into the lake
```

What happens in each line:

1. `lake_position()` queries the Parquet files in the lake with DuckDB and returns the highest `kafka_offset + 1` per partition, or `None` if the lake is empty.
2. `read_new(start)` fixes the end offsets of the topic at this moment and reads the range from `start` to that end. Records that arrive later are left for the next run.
3. `write_files(new, STAGING)` first deletes leftovers of an earlier crashed run in staging, then writes the new records as Parquet into a new run folder in `staging/`.
4. `publish(run)` renames `staging/run-…` to `lake/logs/run-…`. Both folders are on the same mounted volume, so this is a single atomic step. If there was nothing to write, `run` is `None` and `publish` does nothing.

### Explanation of the result

`check_all` empties the lake and then runs the job in five situations. The crashes are simulated by raising `SimulatedStop` inside `write_files`, which `run_once()` does not catch.

1. **First run:** the lake is empty, so `lake_position()` returns `None` and the job reads the whole topic from the beginning. All records are in the lake exactly once.
2. **New records arrive:** the job starts after the highest offset in the lake of every partition, so it copies only the new records. No duplicates, none missing.
3. **Nothing new:** start equals end, so `write_files` prints "nothing to write" and returns `None`, and `publish` does nothing. The lake does not change (the run takes only a fraction of a second).
4. **Stop while writing:** the crash happens after the first partition was written, but only into `staging/`. Right after the crash the "in lake" column is unchanged and "not loaded" is 100,000: nothing of the unfinished run is visible to readers. The next run reads the same range again (the lake position did not move), deletes the leftovers in staging, writes the full run and publishes it. Result: no duplicates, none missing.
5. **Stop after writing:** all files of the run exist, but the run was not yet published, so it is still invisible. The next run redoes it in the same way.

In every situation the checker reports 0 duplicates and 0 missing, and `Exactly once after every run: YES`. "Not loaded" right after a crash is not an error, it is the work of the next run.

**Limits of B.**

- Only one job may run at a time. Two overlapping runs would read the same position and both publish it, which creates duplicates.
- The rename is atomic only inside one file system, which is why staging lies next to the lake. On object storage (e.g. S3) a directory rename is not atomic. There, table formats such as Delta Lake or Apache Iceberg provide the atomic commit.

## Step 4: check it

Every check passed. 🟢

~~~text
reset: lake, staging and state deleted

situation                                 in lake  duplicates  missing  not loaded    run  check
-----------------------------------------------------------------------------------------------
1 first run                             2,000,000           0        0           0  21.5s  PASS
2 new records arrive, run again         2,100,000           0        0           0   4.5s  PASS
3 nothing new, run again                2,100,000           0        0           0   0.0s  PASS
4 run stops WHILE writing: right after  2,100,000           0        0     100,000         PASS
  ... then the next run                 2,200,000           0        0           0   4.5s  PASS
5 run stops AFTER writing: right after  2,200,000           0        0     100,000         PASS
  ... then the next run                 2,300,000           0        0           0   3.8s  PASS
-----------------------------------------------------------------------------------------------
Exactly once after every run: YES
[('1 first run',
  {'topic': 2000000,
   'lake': 2000000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 4,
   'mb': 111.431943,
   'ok': True,
   'problems': []},
  21.498744249343872),
 ('2 new records arrive, run again',
  {'topic': 2100000,
   'lake': 2100000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 8,
   'mb': 112.589974,
   'ok': True,
   'problems': []},
  4.522165775299072),
 ('3 nothing new, run again',
  {'topic': 2100000,
   'lake': 2100000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 8,
   'mb': 112.589974,
   'ok': True,
   'problems': []},
  0.028470516204833984),
 ('4 run stops WHILE writing: right after',
  {'topic': 2200000,
   'lake': 2100000,
   'dup': 0,
   'miss': 0,
   'lag': 100000,
   'files': 8,
   'mb': 112.589974,
   'ok': True,
   'problems': []},
  None),
 ('  ... then the next run',
  {'topic': 2200000,
   'lake': 2200000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 12,
   'mb': 113.74307,
   'ok': True,
   'problems': []},
  4.538753986358643),
 ('5 run stops AFTER writing: right after',
  {'topic': 2300000,
   'lake': 2200000,
   'dup': 0,
   'miss': 0,
   'lag': 100000,
   'files': 12,
   'mb': 113.74307,
   'ok': True,
   'problems': []},
  None),
 ('  ... then the next run',
  {'topic': 2300000,
   'lake': 2300000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 15,
   'mb': 114.834974,
   'ok': True,
   'problems': []},
  3.7591423988342285)]
~~~
