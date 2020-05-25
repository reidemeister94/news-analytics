from GoogleNews import GoogleNews
from pprint import pprint
from newspaper import Article
import newspaper
import yaml

googlenews = GoogleNews()
googlenews.setlang('tr')
googlenews.setTimeRange('12/01/2019','05/20/2020')
googlenews.search('virus')
downloaded_links_count = len(googlenews.get__links())
count_page = 2
finished = False
while not finished:
    googlenews.getpage(count_page)
    print(count_page)
    print(len(googlenews.get__links()))
    print('='*75)
    count_page += 1
    if len(googlenews.get__links()) > downloaded_links_count:
        downloaded_links_count = len(googlenews.get__links())
    else:
        finished = True









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