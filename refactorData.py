import pandas as pd

df = pd.read_csv('Tweets.csv')
# tweet_id,airline_sentiment,airline_sentiment_confidence,negativereason,negativereason_confidence,airline,
# airline_sentiment_gold,name,negativereason_gold,retweet_count,text,tweet_coord,tweet_created,tweet_location,user_timezone

# ItemID,Sentiment,SentimentText
df = df.drop(['airline_sentiment_confidence', 'negativereason',
              'negativereason_confidence', 'airline', 'airline_sentiment_gold', 'name',
              'negativereason_gold', 'retweet_count', 'tweet_coord', 'tweet_created', 'tweet_location', 'user_timezone'], axis=1)
df.columns = ['ItemID', 'Sentiment', 'SentimentText']
df['SentimentText']=df['SentimentText'].str.replace('\\n','') 
df['SentimentText']=df['SentimentText'].str.replace('\\r','') 
df.to_csv('Tweets_new.csv', index=False)
