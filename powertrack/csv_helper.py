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
        try:
            # Fix twitter's geojson in the geo field
            lat = the_geom["coordinates"][0]
            lon = the_geom["coordinates"][1]
            the_geom["coordinates"][0] = lon
            the_geom["coordinates"][1] = lat
            return json.dumps(the_geom)
        except (KeyError, TypeError, IndexError):
            pass

    try:
        the_geom = tweet["location"]["geo"]
    except (KeyError, TypeError, IndexError):
        pass
    else:
        try:
            if the_geom["type"] == "point" or the_geom["type"] == "Point":
                return json.dumps(the_geom)
            elif the_geom["type"] == "polygon" or the_geom["type"] == "Polygon":
                bbox = zip(*the_geom["coordinates"][0])
                the_geom = {"type": "point", "coordinates": [(max(bbox[0]) + min(bbox[0])) / 2, (max(bbox[1]) + min(bbox[1])) / 2]}
                return json.dumps(the_geom)
        except (KeyError, TypeError, IndexError):
            pass

    try:
        the_geom = tweet["gnip"]["profileLocations"][0]["geo"]
    except (KeyError, TypeError, IndexError):
        pass
    else:
        try:
            if the_geom["type"] == "point" or the_geom["type"] == "Point":
                return json.dumps(the_geom)
            elif the_geom["type"] == "polygon" or the_geom["type"] == "Polygon":
                bbox = zip(*the_geom["coordinates"][0])
                the_geom = {"type": "point", "coordinates": [(max(bbox[0]) + min(bbox[0])) / 2, (max(bbox[1]) + min(bbox[1])) / 2]}
                return json.dumps(the_geom)
        except (KeyError, TypeError, IndexError):
            pass


def tweet2csv(tweet=None, columns=None):
    """
    Turn a tweet in json format into a CSV row.
    :param tweet: Tweet in json format. If None, header row will be returned.
    :param columns: Array of columns to be created in CartoDB's table. None for all columns.
    :return: CSV row
    """

    if tweet is not None:
        the_geom = get_the_geom(tweet)
        if the_geom is None:
            return None
    else:  # Header row
        the_geom = None

    tweet = tweet or {}

    actor = tweet.get("actor", {})
    location = tweet.get("location", {})

    row = {
        "the_geom": the_geom,
        "postedtime": get_field(tweet, "postedTime", ""),
    }

    if columns is None or "actor_displayname" in columns:
        row["actor_displayname"] = get_field(actor, "displayName", "")
    if columns is None or "actor_followerscount" in columns:
        row["actor_followerscount"] = get_field(actor, "followersCount", 0)
    if columns is None or "actor_friendscount" in columns:
        row["actor_friendscount"] = get_field(actor, "friendsCount", 0)
    if columns is None or "actor_id" in columns:
        row["actor_id"] = get_field(actor, "id", "")
    if columns is None or "actor_dispactor_imagelayname" in columns:
        row["actor_image"] = get_field(actor, "image", "")
    if columns is None or "actor_listedcount" in columns:
        row["actor_listedcount"] = get_field(actor, "listedCount", 0)
    if columns is None or "actor_location" in columns:
        row["actor_location"] = get_field(actor, "location", "", as_json=True)
    if columns is None or "actor_postedtime" in columns:
        row["actor_postedtime"] = get_field(actor, "postedTime", "")
    if columns is None or "actor_preferredusername" in columns:
        row["actor_preferredusername"] = get_field(actor, "preferredUsername", "")
    if columns is None or "actor_statusescount" in columns:
        row["actor_statusescount"] = get_field(actor, "statusesCount", 0)
    if columns is None or "actor_summary" in columns:
        row["actor_summary"] = get_field(actor, "summary", "")
    if columns is None or "actor_utcoffset" in columns:
        row["actor_utcoffset"] = get_field(actor, "utcOffset", 0)
    if columns is None or "actor_verified" in columns:
        row["actor_verified"] = get_field(actor, "verified", False)
    if columns is None or "body" in columns:
        row["body"] = get_field(tweet, "body", "")
    if columns is None or "favoritescount" in columns:
        row["favoritescount"] = get_field(tweet, "favoritesCount", 0)
    if columns is None or "geo" in columns:
        row["geo"] = get_field(tweet, "geo", "", as_json=True)
    if columns is None or "actoinreplyto_linkr_displayname" in columns:
        row["inreplyto_link"] = get_field(tweet, "inReplyTo", "", as_json=True)
    if columns is None or "link" in columns:
        row["link"] = get_field(tweet, "link", "")
    if columns is None or "location_geo" in columns:
        row["location_geo"] = get_field(location, "geo", "", as_json=True)
    if columns is None or "location_name" in columns:
        row["location_name"] = get_field(location, "name", "")
    if columns is None or "object_type" in columns:
        row["object_type"] = get_field(tweet, "objectType", "")
    if columns is None or "retweetcount" in columns:
        row["retweetcount"] = get_field(tweet, "retweetCount", 0)
    if columns is None or "actor_distwitter_entitiesplayname" in columns:
        row["twitter_entities"] = get_field(tweet, "twitter_entities", "", as_json=True)
    if columns is None or "actor_twitter_langdisplayname" in columns:
        row["twitter_lang"] = get_field(tweet, "twitter_lang", "")

    if tweet:
        keys = sorted(row.keys())
        return [row[key] for key in keys]
    else:
        return sorted(row.keys())
