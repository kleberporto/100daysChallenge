import json
import re


def remove_hashtag_symbol(text):
    """
    Function to remove # symbols
    :param text: string
    :return: string with # symbols removed
    """
    clean_text = " ".join(
        [
            (lambda x: x if x.startswith("#") is False else x[1:])(word)
            for word in text.split()
        ]
    )
    return clean_text


def remove_url(text):
    """
    Simple function to remove url
    :param text: string to have its urls removed
    :return: string with urls removed
    """
    text = " ".join(word for word in text.split() if word.startswith("http") is False)


def remove_emojis(text):
    """
    Function used to remove emojis from text.
    By Abdul-Razak Adam on StackOverflow
    Found in
    https://stackoverflow.com/questions/33404752/
    removing-emojis-from-a-string-in-python
    :param text: string to be cleaned
    :return: string without emojis
    """
    regex_pattern = re.compile(
        pattern="["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "]+",
        flags=re.UNICODE,
    )
    return regex_pattern.sub(r"", text)


def clean_tweet_text(tweet):
    """
    Cleans the 'Tweet' data
    :param tweet: string with tweet text
    :return: string without unwanted chars
    """
    tweet = tweet.strip("\n")
    tweet = tweet.strip("\t")
    tweet = remove_emojis(tweet)
    tweet = remove_hashtag_symbol(tweet)
    return tweet


def assembly_tweet_data(tweet_json_data):
    """
    Receives the Tweet in json format and returns a string
     ready to be sent over TCP/IP
    :param tweet_json_data: json data loaded from
    on_data method
    :return: String to be sent over TCP/IP if language requirements
    are met, None if not
    """
    language = tweet_json_data.get("lang", "")

    if language == "pt":
        user = tweet_json_data.get("id_str", "")
        screen_name = tweet_json_data.get("user", "").get("screen_name", "")
        date = tweet_json_data.get("created_at", "")
        text = clean_tweet_text(tweet_json_data["text"])
        data_to_stream = "\t".join([user, screen_name, date, text]) + "\n"
        return data_to_stream
    else:
        return None


def prepare_tweet_data(data):
    """
    Formats Twitter stream data to be sent through the TCP/IP Socket
    :param data: data from the StreamListener 'on_data' method
    :return: string with User,Date and Tweet split by '\t'
    """
    json_data = json.loads(data)
    data_to_stream = assembly_tweet_data(json_data)

    if data_to_stream:
        print(data_to_stream)
        return data_to_stream.encode("utf-8")
    else:
        return None
