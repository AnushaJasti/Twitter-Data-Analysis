from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

consumer_key = "3f2jc0t7XIQku6SLTmRXXBgdN"
consumer_secret = "pU1TvkPRMWH5xeKA6Me16fVHKbQdTNmhsCI8vtMuatIrnn3zln"
access_token = "616881093-dVUNYQJjL8rJsKvdpQxCsUtep4AyiJ2CgGM3OqUh"
access_secret = "7xNZi0DQUykpq8t0kcDl7J4c3rMXmPIgvMTbqWXezFnu1"


class StdOutListener(StreamListener):
    def on_data(self, data):
        print(data)
        return;

    def on_error(self, status):
        print(status)


if __name__ == '__main__' :
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    stream = Stream(auth, listener)
    stream.filter(track=['Tesla'])

