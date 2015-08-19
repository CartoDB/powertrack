import json


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

    try:
        field = field.replace('\n', ' ').replace('\r', '')
    except AttributeError:
        pass

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
        the_geom = tweet["location"]["geo"]
    except (KeyError, TypeError, IndexError):
        pass
    else:
        if the_geom["type"] == "point" or the_geom["type"] == "Point":
            return json.dumps(the_geom)
        elif the_geom["type"] == "polygon" or the_geom["type"] == "Polygon":
            bbox = zip(*the_geom["coordinates"][0])
            the_geom = {"type": "point", "coordinates": [(max(bbox[0]) + min(bbox[0])) / 2, (max(bbox[1]) + min(bbox[1])) / 2]}
            return json.dumps(the_geom)

    try:
        the_geom = tweet["gnip"]["profileLocations"][0]["geo"]
    except (KeyError, TypeError, IndexError):
        pass
    else:
        if the_geom["type"] == "point" or the_geom["type"] == "Point":
            return json.dumps(the_geom)
        elif the_geom["type"] == "polygon" or the_geom["type"] == "Polygon":
            bbox = zip(*the_geom["coordinates"][0])
            the_geom = {"type": "point", "coordinates": [(max(bbox[0]) + min(bbox[0])) / 2, (max(bbox[1]) + min(bbox[1])) / 2]}
            return json.dumps(the_geom)


def tweet2csv(tweet=None, category_name=None, category_terms=None):
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
    generator = tweet.get("generator", {})
    location = tweet.get("location", {})
    object = tweet.get("object", {})
    provider = tweet.get("provider", {})
    category = {"name": category_name, "terms": category_terms}

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
        "category_name": get_field(category, "name", "category_name"),
        "category_terms": get_field(category, "terms", "category_terms"),
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
