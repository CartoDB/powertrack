# Powertrack importer for CartoDB

## Useful methods

Get jobs:

```
jobs = p.jobs.get()
```

Get quote for a job (returns None is job is still being estimated in GNIP):

```python
jobs[0].get_quote()
```

Get full details for a job:

```python
jobs.get(job[0].uuid)
```

Update a job:

```python
jobs[0].update()
```

Get the status (doesn't update in GNIP automatically, so you might want to update the job first, see above):

```python
jobs[0].status
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

## Example flow

From a python console:

```python
from powertrack.api import *
from datetime import datetime
p = PowerTrack()
```

Add a new job:

```python
start = datetime(2014, 03, 1, 12, 00)
end = datetime(2014, 03, 1, 12, 05)
title = "test"
job = p.jobs.create(start, end, title)
```

Check status:

```python
job.status
u'open'
```

Request qoute:

```python
job.get_quote()
job.status
u'estimating'
```

Check quote (it'll be empty until the status changes):

```python
job.get_quote()
{u'estimatedActivityCount': 200,
 u'costDollars': 5000,
 u'estimatedDurationHours': u'1.0',
 u'estimatedFileSizeMb': u'0.16',
 u'expiresAt': u'2015-02-17T17:26:11Z'}
 job.status
 u'quoted'
```

Accept job:

```python
job.accept()
job.status
u'accepted'
```

Eventually, job will start processing in GNIP...

```python
job.status
u'running'
```

...until the job is processed in GNIP

```python
job.status
u'delivered'
```

Now you can export tweets to a CSV file named after the job title ("test" in this case) and placed in the folder defined in the config file.

```python
job.export_tweets()
```


## TODO:

* improve error handling
* message output
