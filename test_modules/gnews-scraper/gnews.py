from GoogleNews import GoogleNews
from pprint import pprint

googlenews = GoogleNews()
googlenews.setlang('tr')
googlenews.setTimeRange('12/01/2019','05/20/2020')
googlenews.search('virus')
links = googlenews.get__links()
print(len(links))
for i in range(2,100):
    print(i)
    googlenews.getpage(i)
    print(len(googlenews.get__links()))
    print('='*75)
# for r in res:
#     pprint(r)
# print(len(res))

# googlenews.search('coronavirus')
# res = googlenews.result()
# for r in res:
#     pprint(r)
# print(len(res))
# googlenews.search('covid')
# res = googlenews.result()
# for r in res:
#     pprint(r)
# print(len(res))