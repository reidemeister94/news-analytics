from news_scraper import NewsScraper
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scheduled News Scraping')
    parser.add_argument('--lang', type=str, default='IT')
    args = parser.parse_args()
    news_scraper = NewsScraper()
    news_scraper.save_news_to_db(args.lang)