import re
from urllib.parse import urlparse, urljoin, urlunparse
from utils import normalize
from bs4 import BeautifulSoup
from hashlib import sha256

def scraper(url, resp, save_content):
    links = extract_next_links(url, resp, save_content)
    return [link for link in links if is_valid(link)] # if link in links is valid, then keep in list and return all

def extract_next_links(url, resp, save_content):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    valid_links = list()
    if resp.raw_response is not None:
        html_page = BeautifulSoup(resp.raw_response.content, 'html.parser')
        html_page_text = html_page.get_text()
        if url not in ["https://www.stat.uci.edu", "https://www.informatics.uci.edu", "https://www.cs.uci.edu", "https://www.ics.uci.edu"] and low_information_detector(resp.raw_response.content, html_page_text):
            return list()
        hash_html_page_content = sha256(html_page_text.encode('utf-8')).hexdigest()
        if hash_html_page_content not in save_content: # avoid exact duplication of html pages
            save_content[hash_html_page_content] = html_page_text
            save_content.sync()
            for link in html_page.find_all('a'): # extract urls from anchor tags
                href_link = link.get('href')
                if href_link:
                    base_url = url
                    if "YOUR_IP" in base_url or "localhost" in base_url.lower():
                        continue
                    try:
                        full_link = urljoin(base_url, href_link)
                        defragmented_link = normalize(urlunparse(urlparse(full_link)._replace(fragment="")))
                        if is_valid(defragmented_link):
                            valid_links.append(defragmented_link)
                    except ValueError as e:
                        print(f"Error joining URL for base: {base_url} and href: {href_link} - Error: {e}")
                        continue
    return valid_links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        if not parsed.netloc: return False
        domain = ".".join(parsed.netloc.split(".")[-3:]).lower()
        if f".{domain}" not in set([".ics.uci.edu", ".cs.uci.edu", ".informatics.uci.edu", ".stat.uci.edu"]):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|ova"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|mpg"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|war"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def low_information_detector(raw_html_page, text_html_page):
    text_ratio_threshold = 0.08
    min_words_ratio = 100
    html_text_ratio = len(text_html_page) / len(raw_html_page)
    num_words_in_text = len(text_html_page.split())
    if html_text_ratio < text_ratio_threshold or num_words_in_text < min_words_ratio: # if size of text in raw resposne is less than 10 percent, avoid the page
        return True
    return False
