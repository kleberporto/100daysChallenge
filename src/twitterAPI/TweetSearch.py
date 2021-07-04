import tweepy as tw
import keys


def get_twitter_auth():
    auth = tw.OAuthHandler(keys.CONSUMER_KEY, keys.CONSUMER_SECRET)
    auth.set_access_token(keys.ACCESS_TOKEN, keys.ACCESS_TOKEN_SECRET)
    return auth


def get_twitter_api():
    auth = get_twitter_auth()
    return tw.API(auth, wait_on_rate_limit=True)


def retrieve_twitter_search_data(api, search_words, date_since, num_of_entries):

    return tw.Cursor(api.search, q=search_words, lang="pt-br", since=date_since).items(
        num_of_entries
    )


def twitter_search(search_words, date_since, num_of_entries):
    api = get_twitter_api()
    # Collect tweets
    return retrieve_twitter_search_data(api, search_words, date_since, num_of_entries)


def main():
    # Define the search term and the date_since date as variables
    search_words = "Covid"
    date_since = "2021-05-04"

    tweets = twitter_search(search_words, date_since, 10)

    # Iterate and print tweets
    for tweet in tweets:
        print("*" * 30)
        print(tweet.text)


if __name__ == "__main__":
    main()
