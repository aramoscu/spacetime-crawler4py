
class WordFrequencies:
    def __init__(self):
        pass

    def tokenize(self, content, uncounted_words):
        token_list = []
        left = 0
        content_size = len(content)
        while left < content_size:
            try:
                if content[left].isalnum() and content[left].isascii():
                    right = left
                    while right < content_size and (content[right].isalnum() or content[right] in "'–-") and content[right].isascii():
                        right += 1
                    word = content[left:right].lower()
                    if word not in uncounted_words and len(word) > 1:
                        token_list.append(word)
                    left = right
                else:
                    left += 1
            except Exception:
                left += 1
                continue
        return token_list

    def computeWordFrequencies(self, token_list):
        token_dict = {}
        for token in token_list:
            if token in token_dict:
                token_dict[token] += 1
            else:
                token_dict[token] = 1
        return token_dict
