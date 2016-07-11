import sys
import simplejson as json
import pandas as pd
import numpy as np
import re
import time
import datetime
import dateutil.relativedelta
from collections import defaultdict
import itertools
import statistics 



class Graph(object):
    # Graph data structure, undirected by default
    def __init__(self, connections, directed= False):
        self._graph = defaultdict(set)
        self._directed = directed
        self.add_connections(connections)

    def __iter__(self):
        return self
    
    def add_connections(self, connections):
        #Add connections (list of tuple pairs) to graph
        
        for node1, node2 in connections:
            self.add(node1, node2)

    def add(self, node1, node2):
        #Add connection between node1 and node2 

        self._graph[node1].add(node2)
        if not self._directed:
            self._graph[node2].add(node1)



def extract_data(file_path):
    contents = open(file_path, "r").read() 
    data = np.array([json.loads(str(item)) for item in contents.strip().split('\n') if item.strip()])
    return data

def clean_data(data):
	list1= [] # list to hold the actor
	list2= [] # list to hold the target
	list3= [] # list to hold the timestamp
	list4= []
	l=[]# list to hold actor- target pair
   
	for i in data:
		try:   # ensure that the json contains the "text" field
            		list1.append(i['actor'])
	        	list2.append(i['target'])
            		list3.append(i['created_time'])

        	except KeyError:
            		continue
	
	list4= zip(list1,list2) 
	l = [list(l) for l in zip(list1, list2)]
	
	actor = np.asarray(list1).transpose()
	target = np.asarray(list2).transpose()
	created_at = np.asarray(list3).transpose()

	clean_df = pd.DataFrame(data = zip(actor,target,created_at), columns=['actor','target','created_time'])
        clean_df['actor_target'] = clean_df[['actor', 'target']].values.tolist()
	
	return (clean_df,l,list3)

def set_date(date_str):
    #Convert string to datetime
    time_struct = time.strptime(date_str,"%Y-%m-%dT%H:%M:%SZ")	# 2016-03-28T23:23:12Z
    date = datetime.datetime.fromtimestamp(time.mktime(time_struct))
    return date

def median_degree(g):
    temp_list =[]
    numofNodes=0

    for i in g._graph.keys():
        numofNodes+=1
	temp_list.append(len(list(g._graph[i])))
    number = "{0:.2f}".format(statistics.median(temp_list))
    return number

def file_write(a,outfile):
    f = open(outfile,'a')
    f.write(str(a) + "\n")
    f.close()

def sliding_time_Window(df,l,timestamp,outfile):
    counter = 0
    idx=0
    # live_nodes would be the node that contains all the transactions as a list of list [[]]
    live_nodes = []
    temp = []
    new_list = []

    for i in df['created_time']:
        # iterate row by row in the dataframe
        
        if counter ==0:
            # first transaction in the input file         
            live_nodes.append(df['actor_target'][counter])
            time1 = set_date(i) # Starting point of 60seconds window
            time2 = set_date(i) # Ending point of 60seconds window
            

            connections = [[x for x in itertools.combinations(a,2)] for a in live_nodes]
            # we then feed the connection list to the graph class to create a graph
            g = Graph(sum(connections,[])) # flatten the connections list by sum(conenctions,[])
            a = median_degree(g)# compute average dergree 
            file_write(a,outfile)
            
        else:
            # from 2nd row onwards
            time2 = set_date(i)   # ending point of the 60 seconds window
            # are the two tweet timestamps within the 60seconds timeframe
            if (abs((time2-time1).seconds <=60) or abs((time2-time1).seconds == 86399) ):
                
                live_nodes.append(df['actor_target'][counter])
                connections = [[x for x in itertools.combinations(a,2)] for a in live_nodes]
                g = Graph(sum(connections,[]))
                a = median_degree(g)
                file_write(a,outfile)
                               
            else:
         
		# delete the transaction that does not fall in this window
		while (abs((time2- time1).seconds) > 60):                		
			temp.append(l[idx])
                	# index to figure out the starting point of the next window
                	idx +=1
                	time1 =set_date(timestamp[idx])

                
		new_list = [x for x in live_nodes if x not in temp]
		live_nodes= new_list
		temp= []         
                live_nodes.append(df['actor_target'][counter])
                connections = [[x for x in itertools.combinations(a,2)] for a in live_nodes]
                g = Graph(sum(connections,[]))
                a = median_degree(g)
                file_write(a,outfile)

        counter+=1



def main(argv):
	filepath = argv[1]
	outfile = argv[2]
	venmo_data = extract_data(filepath)
	(clean_df,l,timestamp) = clean_data(venmo_data)
	sliding_time_Window(clean_df,l,timestamp,argv[2])



if __name__ == "__main__":
    main(sys.argv)
