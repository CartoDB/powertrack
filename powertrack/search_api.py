import ConfigParser
import csv
import json
import logging
import os
import sys
import requests

from powertrack.csv_helper import tweet2csv


config = ConfigParser.RawConfigParser()
config.read("powertrack.conf")


class Job(object):
    data_url = None

    def __init__(self, pt, title, job_data):
        """
        Job constructor
        :param pt: Powertrack instance
        :param title: Title for the job, used as a file name
        :param job_data: Dictionary with parameters for the GNIP request
        :return:
        """
        self.pt = pt
        self.file_name = os.path.join(config.get('output', 'folder'), "{filename}.csv".format(filename=title))
        self.request_data = job_data
        self.data_url = "search/{label}.json".format(label=config.get('credentials', 'label'))

    def export_tweets(self):
        """
        Gets data from GNIP and generates CSV file
        """
        next_page = True

        sys.stdout.write("Building CSV file {file_name}.\n".format(file_name=self.file_name))

        csv_file = open(self.file_name, 'w')
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(tweet2csv())  # Header row

        while next_page:
            if next_page is not True:
                self.request_data.update({"next": next_page})

            r = self.pt.post(self.data_url, self.request_data)

            if r.status_code != requests.codes.ok:
                logging.error(r.json()["error"]["message"])
                next_page = False
                continue

            response_data = r.json()
            for tweet in response_data["results"]:
                csv_tweet = tweet2csv(tweet)
                if csv_tweet is not None:
                    csv_writer.writerow(csv_tweet)

            next_page = response_data["next"] if "next" in response_data else False

        csv_file.close()


class JobManager(object):
    def __init__(self, pt):
        self.pt = pt

    def create(self, from_date, to_date, title, rules):
        """
        Create a new job definition
        :param from_date: Start timestamp
        :param to_date: End timestamp
        :param title: Title for the job (file name)
        :param rules: Powertrack rules. Each rule will be ANDed with geo-enabled filter. Only one rule is currently supported (others ignored if more than one)
        :return:
        """
        query = "({value}) (has:geo OR has:profile_geo)".format(value=rules[0])

        data = {
            "publisher": "twitter",
            "fromDate": from_date.strftime("%Y%m%d%H%M"),
            "toDate": to_date.strftime("%Y%m%d%H%M"),
            "query": query,
            "maxResults": 500
        }

        return Job(self.pt, title=title, job_data=data)
