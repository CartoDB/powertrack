# Powertrack importer for CartoDB

Example, from a python console:

```python
from powertrack.api import *
from datetime import datetime
p = PowerTrack()
```

Add a new job:

```
start = datetime(2014, 03, 1, 12, 00)
end = datetime(2014, 03, 1, 12, 05)
title = "test"
p.add_job(start, end, title, rules)
```

Get jobs:

```
jobs = p.jobs.get()
```

Get quote for a job:

```python
jobs[0].get_quote()
```

Get full details for a job:

```python
jobs.get(job[0].uuid)
```

Accept a job:

```python
jobs[0].accept()
```

Reject a job:

```python
jobs[0].reject()
```

Export job to CSV:

```
jobs[0].export_tweets()
```

TODO:

* improve error handling
* message output
