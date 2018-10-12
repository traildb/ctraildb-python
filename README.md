TrailDB Python bindings -- C implementation
-------------------------------------------

This is a Python library that is compatible with a subset of `traildb-python`.

Most features of `traildb-python` are not implemented but the most important
feature of scanning a TrailDB is there.

```python
import ctraildb      # Drop-in replacement for `import traildb`.

t = ctraildb.TrailDB('wikipedia-history-small.tdb')

for uuid, trails in t.trails():
    for events in trails:
        print(events.uuid, events.time, events.ip)
```

These bindings are much faster than `traildb-python` for scanning tasks, unless
you are using `pypy` (in which case performance gets worse).

Not implemented:

  * Constructing TrailDBs
  * Event filters
  * Most keywords are not accepted in trails
  * No multicursors
