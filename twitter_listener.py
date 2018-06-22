class Twitter_listener(Tweepy.StreamListener):

    def on_status(self, status):
        tweet = self.get_tweet(Status)
        print(' '.join(tweet))
        self.client_socket.send(json.dump(tweet))
        return True

    def on_error(self, status_code):
        if status_code = 403:
            print(
                "The request is understood, but the access is not allowed. Limit may be reached.")
            return False
