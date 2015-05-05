import ConfigParser
import csv
import json
import logging
import os
import re
import sys
import requests
from gzip import GzipFile
from threading import Thread
from StringIO import StringIO
from datetime import datetime
from Queue import Queue
from requests.exceptions import ConnectionError, SSLError

from powertrack.csv_helper import tweet2csv


config = ConfigParser.RawConfigParser()
config.read("powertrack.conf")


class Job(object):
    _quote = None
    _status = None
    _status_message = None
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
        self._status = job_data.get("status")
        self._status_message = job_data.get("statusMessage")
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
        return True if self._status == "accepted" else False

    def reject(self):
        """
        Tell GNIP to reject job
        :return: True if successful, False if not
        """
        r = self.pt.put(self.job_url, data={"status": "reject"})
        self.update_fields(r.json())
        return True if self._status == "rejected" else False

    def update(self):
        """
        Call GNIP and get latest info for this job. Update the job accordingly.
        """
        r = self.pt.get(self.pt.build_job_url(self.uuid))
        self.update_fields(r.json())

    @property
    def status(self):
        """
        Return updated status from GNIP
        """
        r = self.pt.get(self.pt.build_job_url(self.uuid))
        self.update_fields(r.json())
        return self._status

    @property
    def status_message(self):
        """
        Return updated status message from GNIP
        """
        r = self.pt.get(self.pt.build_job_url(self.uuid))
        self.update_fields(r.json())
        return self._status_message

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
        build_csv_file(urls,
                       os.path.join(config.get('output', 'folder'), "{filename}.csv".format(filename=self.title)),
                       int(config.get('output', 'num_threads')))


class JobManager(object):
    def __init__(self, pt):
        self.pt = pt

    def create(self, from_date, to_date, title, rules):
        """
        Create a new job in GNIP
        :param from_date: Start timestamp
        :param to_date: End timestamp
        :param title: Title for the job
        :param rules: Powertrack rules. Each rule will be ANDed with geo-enabled filter.
        :return:
        """
        rules = [{"value": "({value}) (has:geo OR has:profile_geo)".format(value=rule) for rule in rules}]

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


class GetRequestThread(Thread):
    """
    These threads will get a job data file, convert tweets to CSV and put them in a writer queue.
    The number of these threads come from config file.
    """
    def __init__ (self, url_q, writer_q):
        self.url_q = url_q
        self.writer_q = writer_q
        super(GetRequestThread, self).__init__()

    def run(self):
        while True:
            url = self.url_q.get()
            sys.stdout.write("Processing ({url}). URLs in queue: {pending}\n".format(url=url, pending=self.url_q.qsize()))
            try:
                r = requests.get(url)
            except (ConnectionError, SSLError):
                logging.warning("Connection error ({url}). Will retry later.\n".format(url=url))
                self.url_q.put(url)
            else:
                gz = GzipFile(fileobj=StringIO(r.content))
                for line in gz:
                    try:
                        tweet = json.loads(line)
                    except ValueError:
                        continue
                    csv_tweet = tweet2csv(tweet)
                    if csv_tweet is not None:
                        self.writer_q.put(csv_tweet)
                self.url_q.task_done()


class WriteTweetThread(Thread):
    """
    This thread (only one!!!) will take the CSV tweets from a writer queue and put them into the CSV file.
    """
    def __init__ (self, csv_writer, writer_q):
        self.csv_writer = csv_writer
        self.writer_q = writer_q
        super(WriteTweetThread, self).__init__()

    def run(self):
        while True:
            csv_tweet = self.writer_q.get()
            self.csv_writer.writerow(csv_tweet)
            self.writer_q.task_done()


def build_csv_file(urls, file_name, num_get_request_threads):
    sys.stdout.write("Building CSV file {file_name} from {num_urls} URLs.\n".format(file_name=file_name, num_urls=len(urls)))
    csv_file = open(file_name, 'w')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(tweet2csv())  # Header row

    url_q = Queue()
    writer_q = Queue()

    for i in range(num_get_request_threads):
        t = GetRequestThread(url_q, writer_q)
        t.daemon = True
        t.start()

    writer = WriteTweetThread(csv_writer, writer_q)
    writer.daemon = True
    writer.start()

    try:
        for url in urls:
            url_q.put(url)
        url_q.join()
        if not writer_q.empty():
            writer_q.join()
    except KeyboardInterrupt:
        csv_file.close()
        sys.exit(1)
    else:
        csv_file.close()
        sys.stdout.write("Done.\n")
