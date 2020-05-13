import tweepy
import json
import lib553
import random
from sys import argv

output_file = argv[2]


def output(tweet_num: int, tags_board: list):
    with open(output_file, 'a') as f:
        f.write(f'The num of tweets with tags from the beginning: {tweet_num}\n')
        for tag, num in tags_board:
            f.write(f'{tag} : {num}\n')
        f.write('\n')


def pprint(num, tag_board_in: list):
    print('Tweets num: ', num)
    for tag, number in tag_board_in:
        print(f'{tag}, {number}')
    print('\n')


# override tweepy. StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):
    tag_board = lib553.TweetTagBoard()
    tag_board.top_num = 3
    tweet_counter = 0

    def on_status(self, status):
        print(status.text)

    def on_data(self, data):
        # tag_list = []
        tag_text_list = []

        line = json.loads(data)
        # print(line)

        # if line['lang'] == 'en':
        # print('This is english')
        # text = line['text']
        try:
            tag_list = line['entities']['hashtags']
        except KeyError:
            return 1
        if tag_list:
            for tag_line in tag_list:
                tag_text = tag_line['text']
                if tag_text.encode('UTF-8').isalpha():
                    tag_text_list.append(tag_text)

        if tag_text_list:
            self.tweet_counter += 1
            # print(tag_text_list)
            if self.tag_board.tweet_num < 100:
                # print('Adding new tweet')
                self.tag_board.add_one(tag_text_list)
                # pprint(self.tweet_counter, self.tag_board.tags_board)
            else:
                if random.random() < 100 / self.tweet_counter:
                    # print('Replacing one tweet')
                    self.tag_board.pop_one()
                    self.tag_board.add_one(tag_text_list)
                    # pprint(self.tweet_counter, self.tag_board.tags_board)

            output(self.tweet_counter, self.tag_board.tags_board)
        return 1


consumer_token = 'consumer_token'
consumer_secret = 'consumer_secret'

key = 'key'
secret = 'secret'

auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(key, secret)

api = tweepy.API(auth, wait_on_rate_limit=True)
# api.update_status('tweepy + oauth!')
api.configuration()

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
myStream.filter(track='has:hashtags')
