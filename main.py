import boto3
import feedparser
import pprint
import re
import time

pp = pprint.PrettyPrinter(indent=4)


def get_rss_data(max_articles=5) -> dict:

    feeds = [
        {'name': 'cnn', 'url': 'http://rss.cnn.com/rss/cnn_topstories.rss'},
        {'name': 'ny_times',
            'url': 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml'},
        {'name': 'washington_post', 'url': 'http://feeds.washingtonpost.com/rss/national'},
        {'name': 'npr', 'url': 'https://feeds.npr.org/1001/rss.xml'},
        {'name': 'fox_news', 'url': 'http://feeds.foxnews.com/foxnews/national'},
        {'name': 'oann', 'url': 'https://www.oann.com/category/newsroom/feed/'},
        # error scenario for bad url
        # { name: 'oann', url: 'https://www.oasdffdnn.com/csfdsaflkjategory/newsroom/feed/' },
    ]

    # not all feeds provide these, need to account for that
    target_keys = ['author', 'link', 'published', 'title', 'summary']

    def sanitize_input(input: str) -> str:
        ''' Removes html tags and strips whitespace chars (' ', \n, \r, tab). '''
        res = re.sub('<[^<]+?>', '', input).strip()
        res = res.replace(u'\xa0', u' ')
        res = res.replace('\n', '')
        res = res.replace('\r', '')
        return res

    def get_articles(target_rss_feed: str, max_articles: int) -> list:
        ''' gets articles for the specific rss feed '''
        rss_data = feedparser.parse(target_rss_feed)
        articles = rss_data.entries[0:max_articles]
        # print('type', type(articles))
        # pp.pprint(articles)
        return articles

    # test_articles = get_articles(feeds[1]['url'], max_articles)

    def filter_article(article: dict, filter_fields: list) -> dict:
        ''' takes full article and filters the fields based on filter_field list provided. '''
        def field_sanitizer(d: dict, k: str) -> str:
            ''' sanitizes field and returns empty field if dict key doesn't exist. '''
            if k in d:
                return sanitize_input(d[k])
            return ''
        filtered_dict = {field: field_sanitizer(
            article, field) for field in filter_fields}
        return filtered_dict

    # gather all returned articles and filter them
    # test_filtered_articles = [filter_article(article, target_keys)
        #   for article in test_articles]

    # pp.pprint(test_filtered_articles)

    feed_data = []
    time_stamp = int(time.time())

    for item in feeds:
        articles = get_articles(item['url'], max_articles)
        res = {
            'name': item['name'], 'ts': time_stamp, 'data': [filter_article(article, target_keys)
                                                             for article in articles]
        }
        feed_data.append(res)

    pp.pprint(feed_data)

    return feed_data


def create_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource(
            'dynamodb', endpoint_url="http://localhost:8000")
    table = ''
    try:
        table = dynamodb.create_table(
            TableName='RSS_NEWS',
            KeySchema=[
                {
                    'AttributeName': 'name',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'ts',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'name',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'ts',
                    'AttributeType': 'N'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print("Created RSS_NEWS table. ")

    except Exception as e:
        print("Table already exists. ")
        print(e)

    return table


def delete_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource(
            'dynamodb', endpoint_url="http://localhost:8000")

    try:
        table = dynamodb.Table('RSS_NEWS')
        table.delete()
        print("Deleted RSS_NEWS table. ")
    except Exception as e:
        print("No table to delete. ")
        print(e)


def load_news(rss_data, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource(
            'dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('RSS_NEWS')
    for news in rss_data:
        name = news['name']
        ts = news['ts']
        print("Adding news: ", name, ts)
        table.put_item(Item=news)


if __name__ == '__main__':
    rss_data = get_rss_data()
    create_table()
    load_news(rss_data)

    # clean up resources
    # delete_table()
