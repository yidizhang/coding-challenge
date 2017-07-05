import sys
import json
import itertools
#import datetime
#import networkx as nx
#import matplotlib.pyplot as plt
from collections import deque
import numpy as ny


#the function to find the friends list within Dth degree network
def find_friends(Graph,start,num):
    q1=deque() #q1 is the queue to be emptied
    q2=deque()  # q2 is the queue to be filled
    q1.append(start)
    result=[]
    visited=[]
    num = min(len(Graph),num)
    for current_level in range(1,num):
        while q1:
            u=q1.pop()#remove the vertex from queueTobeEmptied and call it as u
            for (u,v) in Graph:
                if v not in visited:
                    visited.append(v)
                    q2.append(v)    
                    result.append(v)                       
        q1,q2 = q2,q1        #swap the queues now for next iteration of for loop

    #print (visited)
    return visited

# the function to generate all edges from social network graph
def generate_edges(graph):
    edges = []
    for node in graph:
        for neighbour in graph[node]:
            edges.append((node, neighbour))

    return edges

# the function to compute mean of last T purchases within D-th degree social network
def compute_mean(purchase_history,find_friends, T):
    #friends_purchase=[]
    amount_list=[]

    for i, list in enumerate(purchase_history):
        if len(purchase_history)==1:
            print ("no enough purchase history")
            return
        if len(purchase_history) < T:
            if list[0] in find_friends:                 
                amount_list.append(list[2])
        if len(purchase_history) >= T:
            if list[0] in find_friends and len(amount_list)< T+1:                   
                amount_list.append(list[2])
   
    result = (amount_list,round(ny.mean(amount_list),2))

    return result
# the function to compute standard deviation                        
def compute_sd(mean,amount_list):
    sum_sq=0
    for amount in amount_list:
        diff= amount - mean
        sq = diff**2
        sum_sq += sq
    result = round((sum_sq/len(amount_list))**(1/2),2)
    return result
# the function to check anomaly 
def check_anomaly(event,mean, sd):
    if float(event['amount']) > mean + 3*sd:
        event.update({'mean': format(mean,'.2f')})
        event.update({'sd': format(sd,'.2f')})                           
    return event    
            

def printUsage():
    print ("Usage: python average_degree.py <input_file1> <input_file2> <output_file>")  # output_file here is always flagged_purchase.json

def getHashTagLinks(tags):
    return itertools.permutations(tags,2)

           
# the function to read batch_log file to generate initial social graph and purchase history, then it reads stream_log file to find anomaly and flag the event in output file
# the social graph and purchase history is also updated. 
def generateGraph(inputTweets,inputTweets1):
    social_graph = {}
    purchase_list =[]
    
    for t in inputTweets:
        t = json.loads(t)
        if "D" in t and "T" in t:
            D = int(t["D"]) 
            T = int(t["T"])
        if "event_type" in t:
        # keep record of the purchase history and store it as a list of tuples.
            if t["event_type"]=="purchase":
                purchase_list.append((t["id"],t["timestamp"],float(t["amount"])))
        # add to social graph with event_type befriend                                 
            if t["event_type"]=="befriend":
                addEdges = getHashTagLinks((t["id1"],t["id2"]))
                for edge in addEdges:
                    if edge[0] in social_graph:
                        social_graph[edge[0]].append(edge[1])
                    else:
                        social_graph[edge[0]] = [edge[1]]
        # remove from social graph with event_tpye unfriend
            if t["event_type"]=="unfriend":
                addEdges = getHashTagLinks((t["id1"],t["id2"]))
                for edge in addEdges:
                    if edge[0] in social_graph and edge[1] in social_graph:
                        if edge[1] in social_graph[edge[0]]:
                            social_graph[edge[0]].remove(edge[1])
                        if edge[0] in social_graph[edge[1]]:
                            social_graph[edge[1]].remove(edge[0])
               
#begin reading stream_log.json file
    for t in inputTweets1:
        t = json.loads(t)
        
        
        # keep record of the purchase history and store it as a list of tuples.
        if t["event_type"]=="purchase":
            edges = generate_edges(social_graph)                                
            mean = compute_mean(purchase_list,find_friends(edges,t['id'],int(T)),int(T))[1]
            amount_list = compute_mean(purchase_list,find_friends(edges,t['id'],int(T)),int(D))[0]
            sd = compute_sd(mean, amount_list)                
            detection = check_anomaly(t,mean,sd)
            f = open('flagged_purchases.json', 'w') # open for 'w'riting
            f.write(str(detection))
            purchase_list.append((t["id"],t["timestamp"],t["amount"]))                   
        
        # add to social graph with event_type befriend
        if t["event_type"]=="befriend":
            addEdges = getHashTagLinks((t["id1"],t["id2"]))
            for edge in addEdges:
                if edge[0] in social_graph:
                    social_graph[edge[0]].append(edge[1])
                else:
                    social_graph[edge[0]] = [edge[1]]
        # remove from social graph with event_tpye unfriend
        if t["event_type"]=="unfriend":
            addEdges = getHashTagLinks((t["id1"],t["id2"]))
            for edge in addEdges:
                if edge[0] in social_graph and edge[1] in social_graph:
                    if edge[1] in social_graph[edge[0]]:
                        social_graph[edge[0]].remove(edge[1])
                    if edge[0] in social_graph[edge[1]]:
                        social_graph[edge[1]].remove(edge[0])                                       

def main(argv):
    
    if len(argv) < 2 :
        printUsage()
        sys.exit(2)
    
    inputTweetsFile = argv[0]
    inputTweetsFile1= argv[1]
    outputTweetsFile = argv[2]
    inputTweets = []
    inputTweets1 = []

    #Read Input File and save all input tweets in memory
    with open(inputTweetsFile,"r") as fIn :
        inputTweets = fIn.readlines()

    with open(inputTweetsFile1,"r") as fIn :
        inputTweets1 = fIn.readlines()

    #process events
    generateGraph(inputTweets, inputTweets1)   



if __name__=="__main__":
    main(sys.argv[1:])
