from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
from urllib.parse import urlparse
from hashlib import sha256
from scraper import scraper, check_dead_urls
import time
import shelve
from urllib.robotparser import RobotFileParser


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.save_content = shelve.open("worker.save_content")
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
            domain = f".{".".join(tbd_parsed.netloc.split(".")[-3:])}"
            hash_domain = sha256(domain)
            if hash_domain not in self.save_content:
                robots_url = f"{tbd_parsed.scheme}://{tbd_parsed.netloc}/robots.txt" 
                robot_parser = RobotFileParser()
                robot_parser.set_url(robots_url)
                robot_parser.read()
                delay = robot_parser.crawl_delay(self.config.user_agent)
                self.save_content[hash_domain] = [delay if delay is not None else 0.0, time.time()] # [delay, last_accessed]
                self.save_content.sync()
                self.frontier.complete_robots_url(robots_url)
            current_time = time.time()
            last_accessed_time = self.save_content[hash_domain][1]
            delta_time = current_time - last_accessed_time
            if delta_time < self.save_content[hash_domain][0]:
                wait = self.save_content[hash_domain][0] - delta_time
                time.sleep(wait)
            resp = download(tbd_url, self.config, self.logger)
            if resp.status == 200 and len(resp.raw_response.content) > 0:
                self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
                scraped_urls = scraper.scraper(tbd_url, resp, self.save_content)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            self.save_content[hash_domain][1] = time.time()
            self.save_content.sync()
            time.sleep(self.config.time_delay)
