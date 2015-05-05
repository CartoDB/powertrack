import ConfigParser
import json
import requests
import urlparse

from powertrack.historical_api import JobManager as HistoricalAPIJobManager
from powertrack.search_api import JobManager as SearchAPIJobManager


HISTORICAL_API = "historical"
SEARCH_API = "search"

config = ConfigParser.RawConfigParser()
config.read("powertrack.conf")


class PowerTrack(object):
    def __init__(self, api=HISTORICAL_API):
        self.api = api

        self.account_name = config.get('credentials', 'account_name')
        self.username = config.get('credentials', 'username')
        self.password = config.get('credentials', 'password')

        if self.api == HISTORICAL_API:
            self.powertrack_root_url = "https://historical.gnip.com/accounts/{account_name}/".format(account_name=self.account_name)
            self.jobs = HistoricalAPIJobManager(self)
        elif self.api == SEARCH_API:
            self.powertrack_root_url = "https://search.gnip.com/accounts/{account_name}/".format(account_name=self.account_name)
            self.jobs = SearchAPIJobManager(self)

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
        if self.api == HISTORICAL_API:
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
