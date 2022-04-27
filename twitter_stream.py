import sys
import socket
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream

from tweepy.streaming import StreamListener

class TweetsListener(StreamListener):

    def __init__(self, socket):

        print ("Tweets listener initialized")
        self.client_socket = socket

    def on_data(self, data):

        try:
            jsonMessage = json.loads(data)
            # print(jsonMessage)
            text = jsonMessage["text"]
            # print(text)
            created_at = jsonMessage["created_at"]
            message = json.dumps({'text': text, 'created_at': created_at}).encode('UTF_8')
            #print (message)
            #self.client_socket.send(message) # This does not work for some reason..
            self.client_socket.send(data.encode('UTF_8'))

        except BaseException as e:
            print("Error on_data: %s" % str(e))

        return True

    def on_error(self, status):

        print (status)
        return True

def connect_to_twitter(connection, tracks):


    # write your own keys
    api_key = "mysNTwJiByRo5jLUbwaxx6zlG"
    api_secret = "OmqqSrLpez084iUSbOebaneFFChu1sFs9gUzLCtXxPvSCP51gG"

    access_token = "923212273069334529-p9148hCh4cLGWepEvNAU4goVF6vclAF"
    access_token_secret =  "4h7ZzPyoFtFC4UTK0HKbrKaAZNQYqmHfAMyhu3LKx27qZ"
    
    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetsListener(connection))
    twitter_stream.filter(track=tracks, languages=["en"])
    
    #twtr_stream = TweetsListener(
    #    api_key, api_secret,
    #   access_token, access_token_secret,
    #   connection
    #)
    #twtr_stream.filter(track=['ETH'])

if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: python m02_demo08_twitterStreaming.py <hostname> <port> <tracks>", 
                file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    tracks = sys.argv[3:]

    s = socket.socket()
    s.bind((host, port))

    print("Listening on port: %s" % str(port))
    #5 the "backlog" which is the number of unaccepted connections that the system will allow before refusing new connections
    s.listen(5)

    connection, client_address = s.accept()
    
    print(connection)
    print(client_address)

    print( "Received request from: " + str(client_address))
    print("Initializing listener for these tracks: ", tracks)

    connect_to_twitter(connection, tracks)
