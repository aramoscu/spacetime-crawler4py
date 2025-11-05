from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
from urllib.parse import urlparse
from hashlib import sha256
from scraper import scraper, is_valid
import time
import shelve
import urllib.error
import socket
from urllib.robotparser import RobotFileParser


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.save_content = shelve.open("worker.save_content")
        if "longest_page_length" not in self.save_content:
            self.save_content["longest_page_length"] = ["", 0]
        if "unique_pages" not in self.save_content:
            self.save_content["unique_pages"] = 0
        self.save_content.sync()
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            # deal with robots.txt file for current domain
            tbd_parsed = urlparse(tbd_url)
            domain = tbd_parsed.netloc
            hash_domain = sha256(domain.encode("utf-8")).hexdigest()
            if hash_domain not in self.save_content:
                robots_url = f"{tbd_parsed.scheme}://{tbd_parsed.netloc}/robots.txt"
                robot_parser = RobotFileParser()
                robot_parser.set_url(robots_url)
                delay = None
                try:
                    robot_parser.read()
                except (urllib.error.URLError, socket.gaierror) as e:
                    self.logger.info(f"FAILED to resolve or access {robots_url}: {e}")
                else:
                    delay = robot_parser.crawl_delay(self.config.user_agent)
                self.save_content[hash_domain] = [delay if delay is not None else self.config.time_delay, time.time(), robot_parser] # [delay, last_accessed]
                self.save_content.sync()
                self.frontier.complete_robots_url(robots_url)
            robot_data = self.save_content[hash_domain]
            robot_parser = robot_data[2]
            if not robot_parser.can_fetch(self.config.user_agent, tbd_url):
                print(f"SKIPPED {tbd_url}: Disallowed by robots.txt.")
                self.frontier.mark_url_complete(tbd_url)
                continue
            current_time = time.time()
            last_accessed_time = self.save_content[hash_domain][1]
            delta_time = current_time - last_accessed_time
            if delta_time < self.save_content[hash_domain][0]:
                wait = self.save_content[hash_domain][0] - delta_time
                time.sleep(wait)
            resp = download(tbd_url, self.config, self.logger)
            page_length = 0 if resp.raw_response is None else len(resp.raw_response.content)
            if resp.status == 200 and page_length > 0:
                longest_page_info = self.save_content["longest_page_length"] 
                longest_page_info[0] = tbd_url if page_length > self.save_content["longest_page_length"][1] else self.save_content["longest_page_length"][0]
                longest_page_info[1] = page_length if page_length > self.save_content["longest_page_length"][1] else self.save_content["longest_page_length"][1]
                self.save_content["longest_page_length"] = longest_page_info
                self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
                self.save_content["unique_pages"] += 1
                scraped_urls = scraper(tbd_url, resp, self.save_content)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            last_accessed = self.save_content[hash_domain]
            last_accessed[1] = time.time()
            self.save_content.sync()
        self.save_content.close()

    def worker_content(self):
        return self.save_content