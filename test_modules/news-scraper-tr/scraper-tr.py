from newspaper import Article
import newspaper
from pprint import pprint
import yaml


with open('../../configuration/configuration.yaml','r') as f:
    CONFIG = yaml.load(f, Loader=yaml.FullLoader)
urls_tr = CONFIG['scraper']['turkish_news_urls']
news_list_by_url = []
corona_count = 0
corona_dict = {}
for url in urls_tr:
    print(url)
    news_list = newspaper.build(url)
    count = 0
    for article in news_list.articles:
        print(article.url.lower())
        if 'koronavirus' in article.url.lower() or 'coronavirus' in article.url.lower() \
            or 'covid' in article.url.lower():
            news_list_by_url.append(article)
            count += 1
    corona_dict[url] = count
    print(count)
pprint(corona_dict)
    # news_list_by_url.append(news_list.articles)
# print('='*75)
for article in news_list_by_url:
    print("{}\n{}\n{}\n{}\n{}\n{}\n{}\n".\
        format(article.title, article.top_img,article.text, \
            article.keywords, article.tags, article.publish_date, \
                article.summary))
    print('='*75)