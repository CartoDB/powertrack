# Powertrack importer for CartoDB

## Installation

You can either clone the repo and run:

```
python setup.py install
```

or either do:

```
pip install git+ssh://git@github.com/CartoDB/powertrack.git
```

## Usage

### Search API

http://support.gnip.com/apis/search_api2.0/

#### Example flow

From a python console:

```python
from powertrack.api import *
from datetime import datetime
p = PowerTrack(api="search")
```

Create a new job:

```python
start = datetime(2014, 03, 1, 12, 00)
end = datetime(2014, 03, 1, 12, 05)
title = "test"
job = p.jobs.create(start, end, title, ["#lakers", "#celtics"])
```

You can also specify which columns you want to have in your CSV file by means of the `column` parameter (default to all available).

Export tweets to a CSV file named after the job title ("test" in this case) and placed in the folder defined in the config file.

If the query includes double quotes, they need to be escaped with a backslash `\"`.

`start` and `end` default to GNIP's Search API 30-day interval.

```python
job.export_tweets()
```

A new file will be created in the specified folder.

### Historical API

http://support.gnip.com/apis/historical_api2.0/

#### Useful methods

Get jobs from the server:

```python
jobs = p.jobs.get()
```

Get a single job from the server:

```python
jobs.get(uuid)
```

Refresh a job:

```python
jobs[0].refresh()
```

Get the status (updated from GNIP automatically):

```python
jobs[0].status
jobs[0].status_message
```

Get quote for a job (returns None is job is still being estimated in GNIP):
```python
jobs[0].get_quote()
```

Accept a job (*this costs money!!!*):

```python
jobs[0].accept()
```

Reject a job:

```python
jobs[0].reject()
```

Export job to CSV (only if status is "delivered"):

```python
jobs[0].export_tweets()
```

Create a new job:

```python
new_job = p.jobs.create(datetime(2014, 12, 12, 0, 0), datetime(2014, 12, 13, 0, 0), "newjob", ["@nba", "#lakers", "#celtics"], geo_enrichment=True)
```

Params to create() are: start timestamp, end timestamp, unique title for the job, and search terms.

See the rules for the start and end timestamps [here](http://support.gnip.com/apis/historical_api2.0/api_reference.html#Create) (look for "Specifying the Correct Time Window")

#### Example flow

From a python console:

```python
from powertrack.api import *
from datetime import datetime
p = PowerTrack(api="historical")  # This is the default API, so p = PowerTrack() works as well
```

Add a new job:

```python
job = p.jobs.create(datetime(2014, 12, 12, 0, 0), datetime(2014, 12, 13, 0, 0), "newjob", ["@nba", "#lakers", "#celtics"], geo_enrichment=False)
```

Check status:

```python
job.status
u'quoted'
```

Check quote:

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

Accept job (*this costs money!!!*):

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
job.update()
job.status
u'delivered'
```

Now you can export tweets to a CSV file named after the job title ("test" in this case) and placed in the folder defined in the config file.

```python
job.export_tweets()
```

### Category searches

A typical use case for our Powertrack library is when someone wants to make a category torque map out of the tweets. In many cases, each category is defined by a list of simple (words, hashtags, etc.) search terms.

For this particular use case, we have built some helper classes. Just take a look at this example:

```python
from powertrack.category_helper import Job
from datetime import datetime


job = Job("test_categories")

job.create_category("lakers", terms=["#lakers", "randle"])
job.create_category("celtics", terms=["#celtics", "ainge"])

job.run(datetime(2016, 6, 9, 5))
```
