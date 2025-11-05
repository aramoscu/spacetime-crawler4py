import shelve
import nltk
import collections
from urllib.parse import urlparse
from nltk.corpus import stopwords
from tokenization.wordfrequencies import WordFrequencies

class Result():
    def __init__(self, config):
        self.config = config
        self.url_save = shelve.open(self.config.save_file)
        self.crawler_info = shelve.open("worker.save_content")
        try:
            stopwords.words('english')
        except LookupError:
            nltk.download('stopwords')
        self.stop_words = set(stopwords.words('english'))
    
    def num_unique_pages(self):
        return self.crawler_info["unique_pages"]
    
    def longest_page_url(self):
        return self.crawler_info["longest_page"][0]
    
    def fifty_most_common_words(self):
        tokenize = WordFrequencies()
        token_list = []
        for key, value in self.crawler_info.items():
            if key not in {"longest_page_length", "unique_pages"}:
                token_list += tokenize.tokenize(value, self.stop_words)
        frequency = tokenize.computeWordFrequencies(token_list)
        sorted_frequencies = dict(sorted(frequency.items(), key=lambda item: item[1], reverse=True))
        top_fifty = list(sorted_frequencies.keys())
        return top_fifty[:50]
    
    def get_subdomain_counts(self):
        subdomain_counts = collections.defaultdict(int)
        for url, completed in self.url_save.values():
            if completed and "uci.edu" in url:
                netloc = urlparse(url).netloc
                if netloc.startswith('www.'):
                    netloc = netloc[4:]
                subdomain_counts[netloc] += 1
        self.url_save.close()
        output_lines = []
        for subdomain in sorted(subdomain_counts.keys()):
            count = subdomain_counts[subdomain]
            output_lines.append(f"{subdomain}, {count}")
        return "\n".join(output_lines)


    def print_results(self):
        print(f"num_unique_pages: {self.num_unique_pages()}")
        print(f"longest_page_url: {self.longest_page_url()}")
        print(f"fifty_most_common_word: {self.fifty_most_common_words()}")
        print(f"subdomain counts:")
        print(self.get_subdomain_counts())
        self.crawler_info.close()