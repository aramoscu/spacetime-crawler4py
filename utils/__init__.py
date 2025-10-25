import os
import logging
from hashlib import sha256
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

def get_logger(name, filename=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not os.path.exists("Logs"):
        os.makedirs("Logs")
    fh = logging.FileHandler(f"Logs/{filename if filename else name}.log")
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
       "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def get_urlhash(url):
    parsed = urlparse(url)
    # everything other than scheme.
    return sha256(
        f"{parsed.netloc}/{parsed.path}/{parsed.params}/"
        f"{parsed.query}/{parsed.fragment}".encode("utf-8")).hexdigest()

def normalize(url):
    if url.endswith("/"):
        return url.rstrip("/")
    return url

def remove_nonfunctional_params(url):
    non_functional_params = {"utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "sessionid",
                             "jsessionid", "sid", "ref", "gclid", "fbclid", "ts", "v"}
    try:
        parsed_url = urlparse(url)
        query_list = parse_qsl(parsed_url.query, keep_blank_values=True)
        filtered_query_list = [(key, value) for key, value in query_list if key.lower() not in non_functional_params]
        filtered_query = urlencode(filtered_query_list)
        filtered_url = urlunparse(parsed_url._replace(query=filtered_query))
        return filtered_url
    except Exception as e:
        print(f"Couldn't remove nonfunctional parameters for {url}: {e}")
        return url


def sort_query_parameters(url):
    try:
        parsed_url = urlparse(url)
        query_list = parse_qsl(parsed_url.query, keep_blank_values=True)
        sort_query_list = sorted(query_list)
        sorted_query = urlencode(sort_query_list)
        sorted_url = urlunparse(parsed_url._replace(query=sorted_query))
        return sorted_url
    except Exception as e:
        print(f"Couldn't sort parameters for {url}: {e}")
        return url

def check_max_depth(url):
    try:
        max_depth = 8
        path = urlparse(url).path
        different_path_segments = [segment for segment in path.split("/") if segment]
        if len(different_path_segments) > max_depth:
            return True
        else:
            return False
    except Exception as e:
        print(f"Couldn't check max depth for {url}: {e}")
        return False