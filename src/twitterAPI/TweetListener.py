import tweepy as tw
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import keys
import connection_config


def get_twitter_auth():
    """
    Function to connect to the Twitter API with user Keys.
    :return: tweepy OAuthHandler
    """
    auth = tw.OAuthHandler(keys.CONSUMER_KEY, keys.CONSUMER_SECRET)
    auth.set_access_token(keys.ACCESS_TOKEN, keys.ACCESS_TOKEN_SECRET)
    return auth


def clean_tweet_text(tweet):
    """
    Cleans the 'Tweet' data
    :param tweet: tweet text string
    :return: tweet text string without unwanted chars
    """
    tweet.strip('\n')
    tweet.strip('\t')
    return tweet


def assembly_tweet_data(tweet_json_data):
    """
    Receives the Tweet in json format and returns a string ready to be sent over TCP/IP
    :param tweet_json_data: json data loaded from on_data method
    :return: String to be sent over TCP/IP
    """
    user = tweet_json_data.get('user', '').get('screen_name', '')
    date = tweet_json_data.get('created_at', '')
    text = clean_tweet_text(tweet_json_data['text'])
    data_to_stream = "\t".join([user, date, text])
    return data_to_stream


def prepare_tweet_data(data):
    """
    Formats Twitter stream data to be sent through the TCP/IP Socket
    :param data: data from the StreamListener 'on_data' method
    :return: string with User,Date and Tweet split by '\t'
    """
    json_data = json.loads(data)
    data_to_stream = assembly_tweet_data(json_data)
    print(data_to_stream)

    return data_to_stream


class TweetListener(StreamListener):

    def __init__(self, csocket):
        super().__init__()
        self.client_socket = csocket

    def on_data(self, data):
        try:
            prepared_data = prepare_tweet_data(data)
            self.client_socket.send(prepared_data.encode('utf-8'))
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


if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = connection_config.PORT
    s.bind((host, port))

    print('listening on port ' + str(port))

    s.listen(10)
    c, address = s.accept()
    send_data(c, connection_config.SEARCH_LIST)
