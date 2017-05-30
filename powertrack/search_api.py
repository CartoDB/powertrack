import csv
import logging
import os
import sys
import requests

from powertrack.csv_helper import tweet2csv


class Job(object):
    data_path = None

    def __init__(self, pt, title, job_data, columns=None):
        """
        Job constructor
        :param pt: Powertrack instance
        :param title: Title for the job, used as a file name
        :param job_data: Dictionary with parameters for the GNIP request
        :param columns: Array of columns to be created in CartoDB's table. None for all columns.
        :return:
        """
        self.pt = pt
        self.file_name = os.path.join(pt.folder, "{filename}.csv".format(filename=title))
        self.request_data = job_data
        self.columns = columns
        self.data_path = "search/30day/accounts/{account_name}/{label}.json".format(account_name=pt.account_name, label=pt.label)
        self.count_path = "search/30day/accounts/{account_name}/{label}/counts.json".format(account_name=pt.account_name, label=pt.label)

    def export_tweets(self, append=False):
        """
        Gets data from GNIP and generates CSV file
        :param append: whether the rows are to be added to the file in append mode
        :return: number of tweets collected
        """
        next_page = True

        count = 0

        sys.stdout.write("Building CSV file {file_name}.\n".format(file_name=self.file_name))

        if append is True:
            csv_file = open(self.file_name, 'a')
        else:
            csv_file = open(self.file_name, 'w')
        csv_writer = csv.writer(csv_file)
        if append is False:
            csv_writer.writerow(tweet2csv(columns=self.columns))  # Header row

        while next_page:
            if next_page is not True:
                self.request_data.update({"next": next_page})

            r = self.pt.post(self.data_path, self.request_data)

            if r.status_code != requests.codes.ok:
                try:
                    logging.error(r.json()["error"]["message"])
                except ValueError:
                    logging.error(r.text)
                next_page = False
                continue

            response_data = r.json()

            for tweet in response_data["results"]:
                csv_tweet = tweet2csv(tweet, columns=self.columns)
                if csv_tweet is not None:
                    csv_writer.writerow(csv_tweet)
                    count += 1

            next_page = response_data["next"] if "next" in response_data else False

        csv_file.close()

        return count

    def estimate_tweets(self):
        """
        Gets data from GNIP and estimates tweet count for these rules
        """
        self.request_data["bucket"] = "day"
        r = self.pt.post(self.count_path, self.request_data)

        return r.json()["totalResults"]


class JobManager(object):
    def __init__(self, pt):
        self.pt = pt

    def create(self, start=None, end=None, title="twitter_search_result", rule=None, columns=None):
        """
        Create a new job definition
        :param start: Start timestamp, defaults to 30 days ago
        :param end: End timestamp, defaults to now
        :param title: Title for the job (file name)
        :param rule: Powertrack rule, up to 2048 chars, including all operators and spaces, with no single term exceeding 128 characters. Will be ANDed with geo-enabled filter, so you need to reserve room for that.
        :param columns: Array of columns to be created in CartoDB's table. None for all columns.
        :return:
        """
        query = "({value}) (has:geo)".format(value=rule) if rule is not None else "has:geo"

        data = {
            "query": query,
            "maxResults": 500
        }

        if start is not None:
            data["fromDate"] = start.strftime("%Y%m%d%H%M")

        if end is not None:
            data["toDate"] = end.strftime("%Y%m%d%H%M")

        return Job(self.pt, title=title, job_data=data, columns=columns)
