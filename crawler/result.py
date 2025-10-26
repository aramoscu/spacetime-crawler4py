import shelve
import nltk
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
        for key, value in self.crawler_info:
            if key not in {"longest_page_length", "unique_pages"}:
                token_list += tokenize.tokenize(value, self.stop_words)
        frequency = tokenize.computeWordFrequencies(token_list)
        sorted_frequencies = dict(sorted(frequency.items(), key=lambda item: item[1], reverse=True))
        top_fifty = list(sorted_frequencies.keys())
        return top_fifty[:50]
    
    def url_list(self):
        url_list = []
        for url, completed in self.url_save.values():
            if completed and "uci.edu" in url:
                url_list.append(url)
        self.url_save.close()
        sorted_url_list = sorted(url_list)
        return sorted_url_list


    def print_results(self):
        print(self.num_unique_pages())
        print(self.longest_page_url())
        print(self.fifty_most_common_words())
        print(self.url_list())
        self.crawler_info.close()