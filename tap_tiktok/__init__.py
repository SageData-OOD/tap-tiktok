#!/usr/bin/env python3
import os
import json
import backoff
import requests
import arrow
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform
from datetime import datetime, timedelta
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse

REQUIRED_CONFIG_KEYS = ["advertiser_id", "start_date", "token"]
LOGGER = singer.get_logger()
HOST = "business-api.tiktok.com"
PATH = "/open_api/v1.2/reports/integrated/get"


class TiktokError(Exception):
    def __init__(self, msg, code):
        self.msg = msg
        self.code = code
        super().__init__(self.msg)


def giveup(exc):
    """
    code 40100 shows rate limit reach error
    it will give up on retry operation, if code is not 40100
    """
    return exc.code != 40100


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def _extract_stream_id(tap_stream_id):
    """
    E.x. auction_audience_ad_id_report
         -> ad_id
    """
    stream_id = tap_stream_id.replace("auction_basic_", "").replace("auction_audience_", ""). \
        replace("auction_playable_", "").replace("_report", "")
    return stream_id


def _get_report_type(tap_stream_id):
    """
     E.x. auction_audience_ad_id_report
          -> AUDIENCE
     """
    if "audience" in tap_stream_id:
        return "AUDIENCE"
    elif "playable" in tap_stream_id:
        return "PLAYABLE_MATERIAL"
    else:
        return "BASIC"


def get_attr_for_auto_inclusion(tap_stream_id):
    """
    Some Dimension attributes and attributes used in Pk(record_id) for a stream will be in auto-inclusion
    """
    stream_id = _extract_stream_id(tap_stream_id)
    auto_inclusion = {
        "ad_id": ["stat_time_day", "campaign_id", "adgroup_id", "ad_id"],
        "adgroup_id": ["stat_time_day", "campaign_id", "adgroup_id"],
        "campaign_id": ["stat_time_day", "campaign_id"],
        "advertiser_id": ["stat_time_day", "advertiser_id"],
        "playable_id": ["stat_time_day", "playable_id", "country_code"]
    }
    return auto_inclusion.get(stream_id, [])


def get_attrs_for_exclusion(attr):
    """
    Audience reports only allows to select 1 audience_dimensions at a time
    [ exception: gender & age allowed together ]
    hence, other should be excluded
    """
    audience_dimensions = ["gender", "age", "country_code", "ac", "language", "platform", "placement"]
    if attr in ["age", "gender"]:
        audience_dimensions.remove("age")
        audience_dimensions.remove("gender")
    elif attr in audience_dimensions:
        audience_dimensions.remove(attr)
    else:
        return []

    return [["properties", "dimensions", "properties", p] for p in audience_dimensions]


def get_key_properties(stream_name):
    return ["record_id"]


def get_data_level(stream_id):
    """
    E.x. stream_id = "campaign_id"
         -> AUCTION_CAMPAIGN
    """
    return "AUCTION_" + stream_id.replace("_id", "").upper()


def get_selected_attrs(stream, property_name):
    """
    property_name = metrics
    -> return list of selected "metrics"
    property_name = dimension
    -> return list of selected "dimension"
    """
    list_attrs = list()
    for md in stream.metadata:
        if property_name in md["breadcrumb"]:
            if md["metadata"].get("selected", False) or md["metadata"].get("inclusion") == "automatic":
                list_attrs.append(md["breadcrumb"][-1])

    # Make metrics ["spend"] as default selected if no metrics is selected for a stream.
    if property_name == "metrics" and not list_attrs:
        list_attrs = ["spend"]
    return list_attrs


def build_url(path, query=""):
    # type: (str, str) -> str
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", HOST
    return urlunparse((scheme, netloc, path, "", query, ""))


def create_metadata_for_report(schema, tap_stream_id):
    auto_inclusion_keys = get_attr_for_auto_inclusion(tap_stream_id)
    key_properties = get_key_properties(tap_stream_id)
    if key_properties:
        mdata = [{"breadcrumb": [], "metadata": {"table-key-properties": key_properties, "inclusion": "available",
                                                 "forced-replication-method": "INCREMENTAL",
                                                 "valid-replication-keys": ["stat_time_day"]}}]
    else:
        mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "INCREMENTAL",
                                                 "valid-replication-keys": ["stat_time_day"]}}]

    for key in schema.properties:
        # hence when property is object, we will only consider properties of that object without taking object itself.
        # here: dimension & metrics
        if "object" in schema.properties.get(key).type:
            # For all audience report dimensions
            if "audience" in tap_stream_id and key == "dimensions":
                for prop in schema.properties.get(key).properties:
                    field_exclusions = get_attrs_for_exclusion(prop)
                    inclusion = "automatic" if prop in auto_inclusion_keys else "available"
                    prop_metadata = {"inclusion": inclusion, "fieldExclusions": field_exclusions}

                    # Making country_code pre-selected
                    if prop == "country_code": prop_metadata["selected"] = True

                    mdata.extend([{
                        "breadcrumb": ["properties", key, "properties", prop],
                        "metadata": prop_metadata
                    }])
            else:
                for prop in schema.properties.get(key).properties:
                    inclusion = "automatic" if prop in auto_inclusion_keys else "available"
                    mdata.extend([{
                        "breadcrumb": ["properties", key, "properties", prop],
                        "metadata": {"inclusion": inclusion}
                    }])
        else:
            inclusion = "automatic" if key in key_properties else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(schema, stream_id)
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


@backoff.on_exception(backoff.expo, TiktokError, max_tries=5, giveup=giveup, factor=2)
@utils.ratelimit(10, 1)
def make_request(url, headers):
    response = requests.get(url, headers=headers)
    code = response.json().get("code")
    if code != 0:
        LOGGER.error('Return Code = %s', code)
        raise TiktokError(response.json().get("message", "an error occurred while calling API"), code)

    return response


def request_data(attr, headers):
    page = 1
    total_page = 1
    all_items = []

    # do pagination
    while page <= total_page:
        attr["page"] = page

        query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in attr.items()})
        url = build_url(PATH, query_string)
        response = make_request(url, headers=headers)

        data = response.json().get("data", {})
        all_items += data.get("list", [])

        page = data.get("page_info", {}).get("page", 1) + 1
        total_page = data.get("page_info", {}).get("total_page", 1)
    return all_items


def _to_str(_list):
    return [str(i) for i in _list]


def generate_id(row, stream_id, report_type, advertiser_id):
    stat_time_day = row["dimensions"]["stat_time_day"].split(" ")[0]
    id_attrs = list()
    if stream_id == "ad_id":
        id_attrs = [stat_time_day, advertiser_id, row["metrics"]["campaign_id"],
                    row["metrics"]["adgroup_id"], row["dimensions"]["ad_id"]]
    elif stream_id == "adgroup_id":
        id_attrs = [stat_time_day, advertiser_id, row["metrics"]["campaign_id"],
                    row["dimensions"]["adgroup_id"]]
    elif stream_id == "campaign_id":
        id_attrs = [stat_time_day, advertiser_id, row["dimensions"]["campaign_id"]]
    elif stream_id == "advertiser_id":
        id_attrs = [stat_time_day, advertiser_id]
    elif stream_id == "playable_id":
        id_attrs = [stat_time_day, advertiser_id, row["dimensions"]["playable_id"], row["dimensions"]["country_code"]]

    # For Audience dimensions
    if report_type == "AUDIENCE":
        for key, val in row["dimensions"].items():
            if key != "stat_time_day" and "id" not in key:
                id_attrs.append(val)

    return "#".join(_to_str(id_attrs))


def get_valid_start_date(date_to_poll):
    """
    fix for data freshness
    e.g. Sunday's data is available at 3 AM UTC on Monday
    If integration is set to sync at 1AM then a problem occurs
    """

    utcnow = datetime.utcnow()
    date_to_poll = datetime.strptime(date_to_poll, "%Y-%m-%d")

    if date_to_poll >= utcnow - timedelta(days=3):
        date_to_poll = utcnow - timedelta(days=4)

    return date_to_poll.strftime("%Y-%m-%d")


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = "stat_time_day"
        mdata = metadata.to_map(stream.metadata)
        schema = stream.schema.to_dict()

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=schema,
            key_properties=stream.key_properties,
        )
        stream_id = _extract_stream_id(stream.tap_stream_id)
        headers = {"Access-Token": config["token"]}
        attr = {
            "advertiser_id": config["advertiser_id"],
            "report_type": _get_report_type(stream.tap_stream_id),
            "dimensions": get_selected_attrs(stream, "dimensions"),
            "metrics": get_selected_attrs(stream, "metrics"),
            "lifetime": False,
            "page_size": 200
        }

        # playable_id report not required "data_level" attribute in request params
        if stream_id != "playable_id":
            attr["data_level"] = get_data_level(stream_id)

        start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column).split(" ")[0] \
            if state.get("bookmarks", {}).get(stream.tap_stream_id) else config["start_date"]

        start_date = get_valid_start_date(start_date)

        while True:
            attr["start_date"] = attr["end_date"] = start_date  # as both date are in closed interval
            LOGGER.info("Querying Date -------------->  %s  <--------------", attr["start_date"])
            tap_data = request_data(attr, headers)

            # "placement" is renamed with "placement_id" in api response, hence change it back to "placement".
            if "placement" in attr["dimensions"]:
                for row in tap_data:
                    row["dimensions"]["placement"] = row["dimensions"].pop("placement_id")

            bookmark = attr["start_date"]
            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in tap_data:
                    row["record_id"] = generate_id(row, stream_id, attr["report_type"], config["advertiser_id"])

                    # Type Conversation and Transformation
                    transformed_data = transform(row, schema, metadata=mdata)

                    # write one or more rows to the stream:
                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()
                    bookmark = max([bookmark, row["dimensions"][bookmark_column]])

            # Print state only if there is some data available
            if len(tap_data):
                state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, bookmark)
                singer.write_state(state)

            if start_date < str(arrow.utcnow().date()):
                start_date = str(arrow.get(start_date).shift(days=1).date())
            if bookmark >= str(arrow.utcnow().date()):
                break

    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        state = args.state or {}
        sync(args.config, state, catalog)


if __name__ == "__main__":
    main()
