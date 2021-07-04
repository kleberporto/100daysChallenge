import tweepy as tw
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import keys
import connection_config

from src.twitterAPI.TweetUtilFunctions import prepare_tweet_data


def get_twitter_auth():
    """
    Function to connect to the Twitter API with user Keys.
    :return: tweepy OAuthHandler
    """
    auth = tw.OAuthHandler(keys.CONSUMER_KEY, keys.CONSUMER_SECRET)
    auth.set_access_token(keys.ACCESS_TOKEN, keys.ACCESS_TOKEN_SECRET)
    return auth


class TweetListener(StreamListener):
    def __init__(self, csocket):
        super().__init__()
        self.client_socket = csocket

    def on_data(self, data):
        try:
            prepared_data = prepare_tweet_data(data)
            if prepared_data:
                self.client_socket.send(prepared_data)
            return True
        except BaseException as e:
            print("ERROR ", e)
        return True

    def on_error(self, status):
        print(status)
        return True


def send_data(c_socket, search_keywords):
    """
    Function to connect to the Twitter API and filter tweets
    :param c_socket: Socket object used to connect the API
    :param search_keywords: List with words to track on the Twitter Stream
    :return: None
    """
    auth = get_twitter_auth()

    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track=search_keywords)


if __name__ == "__main__":
    s = socket.socket()
    host = "127.0.0.1"
    port = connection_config.PORT
    s.bind((host, port))

    print("Searching tweets with: " + ", ".join(connection_config.SEARCH_LIST))
    print("\nlistening on port " + str(port))
    s.listen(10)
    c, address = s.accept()
    send_data(c, connection_config.SEARCH_LIST)
