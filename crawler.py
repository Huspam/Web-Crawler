import logging
import re
from urllib.parse import urlparse, urljoin
from lxml import html, etree

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.subdomains = {}
        self.max_out_links = (None, -1)
        self.longest_page = (None, -1)
        self.freq_words = {}
        with open('stopwords.txt') as f:
            self.stopwords = set(f.read().split('\n'))


    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        i = 0
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                    i +=1
            
            if i > 1000000:
                break

        with open('analytics.txt', 'w') as analytics_file:
            analytics_file.write(str(self.subdomains) + '\n')
            analytics_file.write(str(self.max_out_links) + '\n')
            analytics_file.write(str(self.longest_page) + '\n')
            analytics_file.write(str(sorted(self.freq_words.items(), key=lambda x: (x[1], x[0]), reverse=True)[:50]))
    

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        url = url_data['url']
        final_url = url_data['final_url']
        content = url_data['content']

        tree = None
        try:
            tree = html.fromstring(content)
        except UnicodeDecodeError:
            print("Invalid content format: ", url)
            return []
        except etree.ParserError:
            print("Empty content: ", url)
            return []
        
        output_links = []
        
        links = tree.xpath('//a/@href')
        base_url = final_url if final_url else url
        for link in links:
            try:
                output_links.append(urljoin(base_url, link))
            except ValueError:     # Link does not appear to be an IPv4 or IPv6 address
                print("Invalid output link: ", link)

        # update subdomains visited and num urls processed from subdomain
        domain = urlparse(url).netloc
        if domain not in self.subdomains:
            self.subdomains[domain] = 1
        else:
            self.subdomains[domain] += 1

        # keep track of page with most valid outlinks
        if len(output_links) > self.max_out_links[1]:
            self.max_out_links = (url, len(output_links))

        # keep track of longest page in terms of word count
        words = self._tokenize(tree.text_content())
        words_length = len(words)
        if words_length > self.longest_page[1]:
            self.longest_page = (url, words_length)

        # keep track of freqwords
        self._compute_word_frequencies(words)
            
        return output_links
    

    def _tokenize(self, content):
        retlist = []
        fast = slow = 0
        while fast < len(content):
            if not content[slow].isascii() or not content[slow].isalnum():
                slow += 1
                fast += 1
            elif not content[fast].isascii() or not content[fast].isalnum():
                retlist.append(''.join(content[slow:fast]).lower())
                slow = fast
            else:
                fast += 1
        
        return retlist


    def _compute_word_frequencies(self, tokens):
        for token in tokens:
            if token not in self.stopwords:
                if token in self.freq_words:
                    self.freq_words[token] += 1
                else:
                    self.freq_words[token] = 1
    

    # def _get_decoded_content(self, content):
    #     if not content:
    #         return None
    #     if isinstance(content, str):
    #         return content
        
    #     encoding = self._detect_encoding(content)
    #     # if not encoding:
    #     #     return None
        
    #     decoded_content = content.decode(encoding)
    #     return decoded_content
    

    # def _detect_encoding(self, content):
    #     res = chardet.detect(content)
    #     return res['encoding']


    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False

