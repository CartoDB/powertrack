import csv
import json
import logging
import requests
import sys
from gzip import GzipFile
from Queue import Queue
from StringIO import StringIO
from threading import Thread
from requests.exceptions import ConnectionError, SSLError


def get_field(obj, name, default, as_json=False):
    """
    Get a field from an object (dictionary) and turn it into a properly-formed CVS field
    :param obj: Object (dictionary) to take the field from
    :param name: Field name
    :param default: What to use if field is empty or non-existing. Must match the expected data type (e.g. "" for strings, 0 for integers...)
    :param as_json:
    :return:
    """
    if name in obj:
        field = obj[name]

        if field is not None:
            field = json.dumps(field) if as_json is True else field
        else:
            field = default
    else:
        field = default

    if default == "":
        field = field.encode("utf-8")

    return field


def get_the_geom(tweet):
    """
    Get the_geom from a tweet, either from the geo field or from the gnip field
    :param tweet:
    :return:
    """
    try:
        the_geom = tweet["geo"]
    except KeyError:
        pass
    else:
        # Fix twitter's geojson in the geo field
        lat = the_geom["coordinates"][0]
        lon = the_geom["coordinates"][1]
        the_geom["coordinates"][0] = lon
        the_geom["coordinates"][1] = lat
        return json.dumps(the_geom)

    try:
        the_geom = tweet["gnip"]["profileLocations"][0]["geo"]
    except (KeyError, TypeError, IndexError):
        pass
    else:
        if the_geom["type"] == "point" or the_geom["type"] == "Point":
            return json.dumps(the_geom)


def tweet2csv(tweet=None):
    """
    Turn a tweet in json format into a CSV row.
    :param tweet: Tweet in json format. If None, header row will be returned.
    :return:CSV row
    """

    if tweet is not None:
        the_geom = get_the_geom(tweet)
        if the_geom is None:
            return None
    else:  # Header row
        the_geom = None

    tweet = tweet or {}

    actor = tweet.get("actor", {})
    category = tweet.get("category", {})
    generator = tweet.get("generator", {})
    location = tweet.get("location", {})
    object = tweet.get("object", {})
    provider = tweet.get("provider", {})

    row = {
        "actor_displayname": get_field(actor, "displayName", ""),
        "actor_followerscount": get_field(actor, "followersCount", 0),
        "actor_friendscount": get_field(actor, "friendsCount", 0),
        "actor_id": get_field(actor, "id", ""),
        "actor_image": get_field(actor, "image", ""),
        "actor_languages": get_field(actor, "languages", "", as_json=True),
        "actor_link": get_field(actor, "link", ""),
        "actor_links": get_field(actor, "links", "", as_json=True),
        "actor_listedcount": get_field(actor, "listedCount", 0),
        "actor_location": get_field(actor, "location", "", as_json=True),
        "actor_objecttype": get_field(actor, "objectType", ""),
        "actor_postedtime": get_field(actor, "postedTime", ""),
        "actor_preferredusername": get_field(actor, "preferredUsername", ""),
        "actor_statusescount": get_field(actor, "statusesCount", 0),
        "actor_summary": get_field(actor, "summary", ""),
        "actor_twittertimezone": get_field(actor, "twitterTimeZone", ""),
        "actor_utcoffset": get_field(actor, "utcOffset", 0),
        "actor_verified": get_field(actor, "verified", False),
        "body": get_field(tweet, "body", ""),
        "category_name": get_field(category, "name", ""),
        "category_terms": get_field(category, "terms", ""),
        "favoritescount": get_field(tweet, "favoritesCount", 0),
        "generator_displayname": get_field(generator, "displayName", ""),
        "generator_link": get_field(generator, "link", ""),
        "geo": get_field(tweet, "geo", "", as_json=True),
        "the_geom": the_geom,
        "gnip": get_field(tweet, "gnip", "", as_json=True),
        "id": get_field(tweet, "id", ""),
        "inreplyto_link": get_field(tweet, "inReplyTo", "", as_json=True),
        "link": get_field(tweet, "link", ""),
        "location_displayname": get_field(location, "displayName", ""),
        "location_geo":  get_field(location, "geo", "", as_json=True),
        "location_link":  get_field(location, "link", ""),
        "location_name":  get_field(location, "name", ""),
        "location_objecttype":  get_field(location, "objectType", ""),
        "location_streetaddress":  get_field(location, "streetAddress", ""),
        "object_id":  get_field(object, "id", ""),
        "object_link":  get_field(object, "link", ""),
        "object_objecttype":  get_field(object, "objectType", ""),
        "object_postedtime":  get_field(object, "postedTime", ""),
        "object_summary":  get_field(object, "summary", ""),
        "object_type": get_field(tweet, "objectType", ""),
        "postedtime": get_field(tweet, "postedTime", ""),
        "provider_displayname": get_field(provider, "displayName", ""),
        "provider_link":  get_field(provider, "link", ""),
        "provider_objecttype":  get_field(provider, "objectType", ""),
        "retweetcount": get_field(tweet, "retweetCount", 0),
        "twitter_entities": get_field(tweet, "twitter_entities", "", as_json=True),
        "twitter_filter_level": get_field(tweet, "twitter_filter_level", ""),
        "twitter_lang": get_field(tweet, "twitter_lang", ""),
        "verb": get_field(tweet, "verb", ""),
    }

    if tweet:
        keys = sorted(row.keys())
        return [row[key] for key in keys]
    else:
        return sorted(row.keys())


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
