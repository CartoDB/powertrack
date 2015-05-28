import ConfigParser
import csv
import logging
import os
import sys
import requests

from powertrack.csv_helper import tweet2csv


config = ConfigParser.RawConfigParser()
config.read("powertrack.conf")


class Job(object):
    data_url = None

    def __init__(self, pt, title, job_data, category_terms=None):
        """
        Job constructor
        :param pt: Powertrack instance
        :param title: Title for the job, used as a file name
        :param job_data: Dictionary with parameters for the GNIP request
        :param category_terms: Terms used to define the category on CartoDB
        :return:
        """
        self.pt = pt
        self.file_name = os.path.join(config.get('output', 'folder'), "{filename}.csv".format(filename=title))
        self.request_data = job_data
        self.category_terms = category_terms
        self.data_url = "search/{label}.json".format(label=config.get('credentials', 'label'))
        self.count_url = "search/{label}/counts.json".format(label=config.get('credentials', 'label'))

    def export_tweets(self, category=None, append=False):
        """
        Gets data from GNIP and generates CSV file
        :param category: Category number for CartoDB
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
            csv_writer.writerow(tweet2csv(category_name=category))  # Header row

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
                csv_tweet = tweet2csv(tweet, category_name=category, category_terms=self.category_terms)
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
        r = self.pt.post(self.count_url, self.request_data)

        return sum([hour["count"] for hour in r.json()["results"]])


class JobManager(object):
    def __init__(self, pt):
        self.pt = pt

    def create(self, from_date=None, to_date=None, title="twitter_search_result", rules=None):
        """
        Create a new job definition
        :param from_date: Start timestamp, defaults to 30 days ago
        :param to_date: End timestamp, defaults to now
        :param title: Title for the job (file name)
        :param rules: Powertrack rules (OR). Each rule will be ANDed with geo-enabled filter.
        :return:
        """
        if rules is not None:
            terms = " OR ".join(['%s' % rule for rule in rules])
            query = "({value}) (has:geo OR has:profile_geo)".format(value=terms)
        else:
            terms = None
            query = "has:geo OR has:profile_geo"

        data = {
            "publisher": "twitter",
            "query": query,
            "maxResults": 500
        }

        if from_date is not None:
            data["fromDate"] = from_date.strftime("%Y%m%d%H%M")

        if to_date is not None:
            data["toDate"] = to_date.strftime("%Y%m%d%H%M")

        return Job(self.pt, title=title, job_data=data, category_terms=terms)
