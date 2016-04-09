import sys
import json
import itertools
import datetime

# the function to extract text field from hashtags
def getHashTags(hashtags):
    tags = []
    for tag in hashtags:
        tag = (tag["text"])
        if len(tag) > 0:
            tags.append(tag)
    return list(set(tags))


# the function to check whether two tweets are within 60 secs
def lessThanAMinute(time1,time2):
    ts1 =  datetime.datetime.strptime(time1,'%a %b %d %H:%M:%S +0000 %Y')
    ts2 =  datetime.datetime.strptime(time2,'%a %b %d %H:%M:%S +0000 %Y')
    tDelta = ts2 - ts1
    return tDelta.total_seconds() <= 60


# the function to check whether time1 is less than time2
def lessThan(time1, time2):
    ts1 =  datetime.datetime.strptime(time1,'%a %b %d %H:%M:%S +0000 %Y')
    ts2 =  datetime.datetime.strptime(time2,'%a %b %d %H:%M:%S +0000 %Y')
    tDelta = ts2 - ts1
    return tDelta.total_seconds() >= 0

# the function to find maximaum timestamp of a tweet among the tweets which are being processed
def findMaxTime(tweets):
    for i in range(len(tweets)):
        if (lessThan(tweets[0]["created_at"],tweets[i]["created_at"])):
            max = tweets[i]["created_at"]
    return max

# the function to do 2 element permutation of a list
def getHashTagLinks(tags):
    return itertools.permutations(tags,2)

# the function to format how the code is being run
def printUsage():
    print ("Usage: python average_degree.py <input_file> <output_file>")

# the function to read one tweet at a time, add the tweets fall within 60 second window of the current maximum timestamp
# tweet, remove the tweets which fall outside 60 secs window of the current maximum timestamp tweet, and compute the
# average degree of vertices and keep updated this number.
def generateGraph(inputTweets):
    tweets = [];
    avgDegree = []
    for tweetNumber, t in enumerate(inputTweets):
        if tweetNumber % 20 == 0:
            print('Processing tweet ' + str(tweetNumber))
        t = json.loads(t)
        if not("created_at" in t or "text" in t):
            continue
        tweet = {}
        tweet["created_at"] =  t["created_at"]
        tweet["hashtags"] = getHashTags(t["entities"]["hashtags"])
        tweets.append(tweet)
        time_current_tweets = findMaxTime(tweets)
        graph = {}

        i = 0  # Current Tweet
        remaining_tweets = []
        for i in range(len(tweets)):
            # Add Links
            if(lessThanAMinute(tweets[i]["created_at"],time_current_tweets)):
                remaining_tweets.append(tweet)
                if len(tweets[i]["hashtags"]) > 1:
                    addEdges = getHashTagLinks(tweets[i]["hashtags"])
                    for edge in addEdges:
                        if edge[0] in graph:
                            graph[edge[0]].append(edge[1])
                        else:
                            graph[edge[0]] = [edge[1]]


        #Count Degree
        degree = 0.0
        if len(graph) ==0:
            avgDegree.append(format(0.00,'.2f'))
        else:
            for node in graph:
                degree += len(set((graph[node])))
            avgDegree.append(format(round(degree/len(graph),2),'.2f'))

        tweets = remaining_tweets

    return avgDegree


def main(argv):
    if len(argv) < 2 :
        printUsage()
        sys.exit(2)
    inputTweetsFile = argv[0]
    outputTweetsFile = argv[1]
    inputTweets = []

    #Read Input File and save all input tweets in memory
    with open(inputTweetsFile,"r") as fIn :
        inputTweets = fIn.readlines()

    #GenerateGraph for every tweet
    output = generateGraph(inputTweets)

    #Clean the Output File if it already exists
    open(outputTweetsFile, 'w').close()

    #Save Avg Degree for each incoming tweet
    with open(outputTweetsFile,"w") as fOut:
        for avgDegree  in output:
            fOut.write(str(avgDegree) + "\n")


if __name__=="__main__":
    main(sys.argv[1:])

