import ConfigParser
import csv
import json
import os
import re
import requests
import urlparse
from StringIO import StringIO
from datetime import datetime
from gzip import GzipFile

from powertrack.csv_helper import tweet2csv


config = ConfigParser.RawConfigParser()
config.read("powertrack.conf")


class Job(object):
    _quote = None
    data_url = None

    def __init__(self, pt, uuid=None, job_data=None):
        """
        Job constructor
        :param pt: Powertrack instance, used to connect with GNIP
        :param uuid: UUID (it'll try to get it from the jobURL field if not set)
        :param job_data: Data dictionary to populate the internal attributes
        :return:
        """
        self.pt = pt

        if uuid is not None:
            self.uuid = uuid
            self.job_url = self.build_url(self.uuid)

        if job_data is not None:
            self.update_fields(job_data)

    @staticmethod
    def build_url(uuid):
        """
        Get URL from URL
        :param uuid: UUID
        :return: URL
        """
        return "publishers/twitter/historical/track/jobs/{uuid}.json)".format(uuid=uuid)

    @staticmethod
    def build_uuid(url):
        """
        Get UUID from URL
        :param url: URL
        :return: UUID
        """
        try:
            return re.match(r".*/(\w+).json", url).group(1)
        except (AttributeError, IndexError, TypeError):
            pass

    def update_fields(self, job_data):
        """
        Update internal fields with data from a dictionary
        :param job_data: Data dictionary (typically a response from GNIP)
        """
        self.expires_at = datetime.strptime(job_data["expiresAt"], "%Y-%m-%dT%H:%M:%SZ") if "expiresAt" in job_data else None
        self.from_date = datetime.strptime(job_data["fromDate"], "%Y%m%d%H%M") if "fromDate" in job_data else None
        self.to_date = datetime.strptime(job_data["toDate"], "%Y%m%d%H%M") if "toDate" in job_data else None
        self.percent_complete = job_data.get("percentComplete")
        self.publisher = job_data.get("publisher")
        self.status = job_data.get("status")
        self.status_message = job_data.get("statusMessage")
        self.stream_type = job_data.get("streamType")
        self.title = job_data.get("title")
        self.account = job_data.get("account")
        self.format = job_data.get("format")
        self.requested_by = job_data.get("requestedBy")
        self.requested_at = datetime.strptime(job_data["requestedAt"], "%Y-%m-%dT%H:%M:%SZ") if "requestedAt" in job_data else None
        self.accepted_by = job_data.get("acceptedBy")
        self.accepted_at = datetime.strptime(job_data["acceptedAt"], "%Y-%m-%dT%H:%M:%SZ") if "acceptedAt" in job_data else None
        self._quote = job_data.get("quote")

        results = job_data.get("results")
        if results is not None:
            self.data_url = results.get("dataURL")

        self.job_url = job_data.get("jobURL")
        self.uuid = self.build_uuid(self.job_url)

    def accept(self):
        """
        Tell GNIP to accept job
        :return: True if successful, False if not
        """
        r = self.pt.put(self.job_url, data={"status": "accept"})
        self.update_fields(r.json())
        return True if self.status == "accepted" else False

    def reject(self):
        """
        Tell GNIP to reject job
        :return: True if successful, False if not
        """
        r = self.pt.put(self.job_url, data={"status": "reject"})
        self.update_fields(r.json())
        return True if self.status == "rejected" else False

    def update(self):
        """
        Call GNIP and get latest info for this job. Update the job accordingly.
        """
        r = self.pt.get(self.pt.build_job_url(self.uuid))
        self.update_fields(r.json())

    def get_quote(self):
        """
        Get the estimation of the execution of the job, calling GNIP if necessary
        :return: Quote (estimation)
        """
        if self._quote is None:
            self.update()

        return self._quote

    def export_tweets(self):
        """
        Generate CSV file from this job's data files (if job is completed in GNIP)
        """
        if self.data_url is None:
            self.update()

        if self.data_url is None:
            return None

        r = self.pt.get(self.data_url)
        urls = r.json().get("urlList")
        with open(os.path.join(config.get('output', 'folder'), "{filename}.csv".format(filename=self.title)), 'w') as csv_file:
            writer = csv.writer(csv_file, dialect=csv.QUOTE_MINIMAL)
            writer.writerow(tweet2csv())
            for url in urls:
                r = requests.get(url)
                gz = GzipFile(fileobj=StringIO(r.content))
                for line in gz:
                    try:
                        tweet = json.loads(line)
                    except ValueError:
                        continue
                    tweet = tweet2csv(tweet)
                    if tweet is not None:
                        writer.writerow(tweet)


class JobManager(object):
    def __init__(self, pt):
        self.pt = pt

    def create(self, from_date, to_date, title):
        """
        Create a new job in GNIP
        :param from_date: Start timestamp
        :param to_date: End timestamp
        :param title: Title for the job
        :return:
        """
        hashtags = config.get("rules", "hashtags").split(",")
        rules = [{"tag": hashtag} for hashtag in hashtags]

        data = {
            "publisher": "twitter",
            "streamType": "track",
            "dataFormat": "activity-streams",
            "fromDate": from_date.strftime("%Y%m%d%H%M"),
            "toDate": to_date.strftime("%Y%m%d%H%M"),
            "title": title,
            "rules": rules,
        }

        r = self.pt.post("jobs", data)
        return Job(self.pt, job_data=r.json())

    def get(self, uuid=None):
        """
        Retrieve jobs from GNIP
        :param uuid: If present, retrieve only this particular job (it'll trigger a quote request in GNIP too!)
        :return: Job or jobs
        """
        if uuid is None:
            r = self.pt.get("jobs")
            return [Job(self.pt, job_data=job_data) for job_data in r.json()["jobs"]]
        else:
            r = self.pt.get(self.pt.build_job_url(uuid))
            return Job(self.pt, job_data=r.json())


class PowerTrack(object):
    def __init__(self):
        self.account_name = config.get('credentials', 'account_name')
        self.username = config.get('credentials', 'username')
        self.password = config.get('credentials', 'password')

        self.powertrack_root_url = "https://historical.gnip.com/accounts/{account_name}/".format(account_name=self.account_name)

        self.jobs = JobManager(self)

    def build_url(self, path):
        """
        Get full URL from relative path
        :param path: Relative path
        :return:
        """
        return urlparse.urljoin(self.powertrack_root_url, path)

    def build_job_url(self, uuid):
        """
        Build GNIP API URL for a job
        :param uuid: Job's UUID
        :return: URL
        """
        return urlparse.urljoin(self.powertrack_root_url, "publishers/twitter/historical/track/jobs/{uuid}.json)".format(uuid=uuid))

    def get(self, path):
        """
        GET request to a relative path with basic authentication
        :param path: Relative path
        :return: Response
        """
        url = self.build_url(path)
        return requests.get(url, auth=(self.username, self.password))

    def post(self, path, data):
        """
        POST request to a relative path with basic authentication
        :param path: Relative path
        :param data: POST body data
        :return: Response
        """
        url = self.build_url(path)
        return requests.post(url, data=json.dumps(data), auth=(self.username, self.password), headers={'Content-Type': 'application/json'})

    def put(self, url, data):
        """
        PUT request to a full URL with basic authentication
        :param url: Absolute URL
        :param data: PUT body data
        :return: Response
        """
        return requests.put(url, data=json.dumps(data), auth=(self.username, self.password), headers={'Content-Type': 'application/json'})
