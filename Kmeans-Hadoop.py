#!/usr/bin/env python
# coding: utf-8

# ## Part 1.1

# ### Generated data: 2,000,000 rows and 5 columns.
# #### Name: kmeans_data.csv

# In[3]:


#!/usr/bin/python
import random

random.seed(1997)

#Generate 2M 5-dimensional data points with random values between 0 and 1.
#Change to '2000000' when run on the instance.
k_meansData = [[random.random() for column in range(5)] for row in range(2000000)]

with open('kmeans_data.csv', 'w') as f:
    for row in k_meansData:
        f.write(','.join(map(str, row)) + '\n')


# -- Code --
# 
# vim generateKmeans_data.py
# 
# python generateKmeans_data.py
# 
# cat kmeans_data.csv | wc -l
# 
# cat kmeans_data.csv | less

# ### Generated initial cluster centers: 5 rows.
# #### Name: centers.csv

# #### Using Kmeans++ to reduce the potential of picking two close starting centroid. Try to run but took way to long to complete. --DONT run this code--

# In[5]:


#!/usr/bin/python
import random
import csv

def squared_euclideanDistance(point1, point2):
    '''Calculate square of Euclidean distance between 2 points.'''
    return sum((x - y)**2 for x, y in zip(point1, point2))

def kmeansCenters(data, kNum):
    '''Get the initial cluster centers using Kmeans++.'''
    
    #Randomly choose the first center, stored as a list.
    centers = [random.choice(data)]

    for k in range(1, kNum):
        
        #Calculate squared distances to the nearest existing center for each point
        distances = [min(squared_euclideanDistance(point, center) for center in centers) for point in data]

        #Calculate probabilities
        probabilities = [d / sum(distances) for d in distances]

        #Choose the next center with probability proportional to squared distance.
        nextCenter = random.choices(data, weights=probabilities)[0]
        centers.append(nextCenter)

    return centers

#Read data from 'kmeans_data.csv'
with open('kmeans_data.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    #Extract individual data from each cell.
    data = [[float(value) for value in row] for row in reader]

#Chosen number of k clusters.
kNum = 5
initialCenters = kmeansCenters(data, kNum)
with open('centers.txt', 'w') as f:
    for center in initialCenters:
        f.write(','.join(map(str, center)) + '\n')


# #### Using random selection of data points. --RUN this one--

# In[6]:


#!/usr/bin/python
import random
import csv

def clusterCenters(data, k):
    '''Get the initial cluster centers using random points selection.'''
    random.seed(1997)
    return random.sample(data, k)

with open('kmeans_data.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    #Extract individual data from each cell.
    data = [[float(value) for value in row] for row in reader]

#Chosen number of k clusters.
kNum = 5
initialCenters = clusterCenters(data, kNum)
with open('centers.txt', 'w') as f:
    for center in initialCenters:
        f.write(','.join(map(str, center)) + '\n')


# -- Code --
# 
# vim generateCenters.py
# 
# python vim generateCenters.py
# 
# cat centers.txt | wc -l
# 
# cat centers.txt | less

# ### Kmeans Mapper.

# In[7]:


#!/usr/bin/python
import sys

#Detect whichever centers file being used for current iteration.
input_centerFiles = ['centers.txt', 'centers1.txt', 'centers2.txt', 'centers3.txt', 'centers4.txt']

#Try to open the file, if not work then pass.
for input_centerFile in input_centerFiles:
    try:
        with open(input_centerFile, 'r') as centerFile:
            centersDict = {}
            for i, line in enumerate(centerFile):
                centersDict[i]=[float(center) for center in line.strip().split(',')]
            break
    except FileNotFoundError:
        pass

for dataPoint in sys.stdin:
    #Get the five features, stored as a list.
    featuresList = [float(feature) for feature in dataPoint.strip().split(',')]

    #Calculate Euclidean distance between current data point and each centroid.
    euclidDistance_list = [sum((c - f) ** 2 for c, f in zip(centroid, featuresList))
                            for centroid in centersDict.values()]

    #Get the cluster number associated with smallest distance.
    clusterID = euclidDistance_list.index(min(euclidDistance_list))
    #Output format: 'clusterNumber|feature1|feature2|feature3|feature4|feature5'
    print(f"{clusterID + 1}|{'|'.join(map(str, featuresList))}")


# ### Kmeans Reducer.

# In[8]:


#!/usr/bin/python
import sys 

centersDict = {}

for line in sys.stdin:
    columns = line.strip().split('|')
    clusterID = columns[0]
    features = [float(value) for value in columns[1:]]
    if clusterID not in centersDict:
        centersDict[clusterID] = []

    centersDict[clusterID].append(tuple(features))

for clusterID, values in sorted(centersDict.items(), key=lambda x: int(x[0])):
    dpSums = [sum(feature) for feature in zip(*values)]
    dpCounts = [len(values)] * len(dpSums)
    newCentroid = [dpSum / dpCount for dpSum, dpCount in zip(dpSums, dpCounts)]
    print(','.join(map(str, newCentroid)))


# ## Part 1.2 - Using the same data as 1.1

# ### Kmedian Mapper.

# In[48]:


#!/usr/bin/python
import sys

#Detect whichever centers file being used for current iteration.
input_centerFiles = ['centers.txt', 'centers1.txt', 'centers2.txt', 'centers3.txt', 'centers4.txt']

#Try to open the file, if not work then pass.
for input_centerFile in input_centerFiles:
    try:
        with open(input_centerFile, 'r') as centerFile:
            centersDict = {}
            for i, line in enumerate(centerFile):
                centersDict[i]=[float(center) for center in line.strip().split(',')]
            break
    except FileNotFoundError:
        pass

for dataPoint in sys.stdin:
    #Get the five features, stored as a list.
    featuresList = [float(feature) for feature in dataPoint.strip().split(',')]

    #Calculate Manhattan distance between current data point and each centroid.
    manhattanDistance_list = [sum(abs(c - f) for c, f in zip(centroid, featuresList))
                               for centroid in centersDict.values()]

    #Get the cluster number associated with smallest distance.
    clusterID = manhattanDistance_list.index(min(manhattanDistance_list))
    #Output format: 'clusterNumber|feature1|feature2|feature3|feature4|feature5'
    print(f"{clusterID + 1}|{'|'.join(map(str, featuresList))}")


# ### Kmedian Reducer.

# In[47]:


#!/usr/bin/python
import sys 

centersDict = {}

for line in sys.stdin:
    columns = line.strip().split('|')
    clusterID = columns[0]
    features = [float(value) for value in columns[1:]]
    if clusterID not in centersDict:
        centersDict[clusterID] = []

    centersDict[clusterID].append(tuple(features))

#Iterate through each cluster.
#Each data point (5 features) stored as as an element in centersDict value list.
for clusterID, values in sorted(centersDict.items(), key=lambda x: int(x[0])):
    newCentroid = []
    
    #Iterate through each feature.
    for featureIndex in range(len(values[0])): 
        
        #Extract each feature from all data points within a clusters. 
        #Put into a list and sort it.
        featureValues = sorted([value[featureIndex] for value in values])
        
        #Return median index as integer. I.e,: If 3.5 return 3.
        medianIndex = len(featureValues) // 2
        
        #If total number of data point assigned to a cluster is even,
        #Get average value of two middle points.
        if len(featureValues) % 2 == 0:
            median = (featureValues[medianIndex - 1] + featureValues[medianIndex]) / 2
        
        #Else, get middle value.
        else:
            median = featureValues[medianIndex]

        newCentroid.append(median)

    print(','.join(map(str, newCentroid)))


# ## Part 2
select sum(lo_revenue), count(*), d_year, p_category
from lineorder, dwdate, part 
where lo_orderdate = d_datekey and lo_partkey = p_partkey
and d_sellingseason = 'Fall' and p_brand1 = 'MFGR#2123' group by d_year, p_category;
# ### 1st MapReduce
# #### Mapper 1 - Extract column name using index and filtered by indicated column.

# In[ ]:


#!/usr/bin/python

import sys
for line in sys.stdin:
    columnName = line.strip().split('|')
    
    #Extract relevant columns in lineorder table.
    if columnName[1].isdigit():
        lo_revenue = columnName[12]
        lo_orderdate = columnName[5]
        lo_partkey = columnName[3]
        print(f"lineorder|{lo_revenue}|{lo_orderdate}|{lo_partkey}") 
    
    #Extract relevant columns in part table.
    #Tackle condition p_brand1 = 'MFGR#2123'.
    else:
        p_category = columnName[3]
        p_partkey = columnName[0]
        p_brand1 = columnName[4]
        if p_brand1 == 'MFGR#2123': 
            print(f"part|{p_category}|{p_partkey}")


# #### Reducer 1 - Join part and lineorder table.

# In[43]:


#!/usr/bin/python
import sys

#Dictionaries to store data from each table.
#Using join condition as key.
partDict = {}
lineorderDict = {}
#List to store join data.
resData = []

for line in sys.stdin:
    columnName = line.strip().split('|')
    fileSource = columnName[0]
    
    #Using p_partkey as key for part table.
    if fileSource == "part":
        p_category = columnName[1]
        p_partkey = columnName[2]
        
        if p_partkey not in partDict:
            partDict[p_partkey] = []
        partDict[p_partkey].append(p_category)

    #Using lo_partkey as key for lineorder table.
    elif fileSource == "lineorder":
        lo_revenue = columnName[1]
        lo_orderdate = columnName[2]
        lo_partkey = columnName[3]
        
        if lo_partkey not in lineorderDict:
            lineorderDict[lo_partkey] = []
        lineorderDict[lo_partkey].append((lo_revenue, lo_orderdate))

#Tackle lo_orderdate = d_datekey.
for lineorderKey, lineorderValues in lineorderDict.items():
    if lineorderKey in partDict:
        p_categoryList = partDict[lineorderKey]
        for p_category in p_categoryList:
            #Tackle multiple rows with the same lo_partkey.
            for lineorderValue in lineorderValues: 
                resData.append((p_category, lineorderValue[0], lineorderValue[1]))

#Join data are stored in resData list. Given each row is each element.
for variable in resData:
    p_category = variable[0]
    lo_revenue = variable[1]
    lo_orderdate = variable[2]
    print(f"{p_category}|{lo_revenue}|{lo_orderdate}")


# ### 2nd MapReduce
# #### Mapper 2 - Extract column name using index and filtered by indicated column.

# In[45]:


#!/usr/bin/python
import sys
import os

#Dictionary to store data for dwdate table.
dwdateDict = {}

with open('dwdate.tbl', 'r') as cacheFile:
    for line in cacheFile:
        columnName = line.strip().split('|')
        d_datekey = columnName[0]
        d_year = columnName[4]
        d_sellingseason = columnName[12]

        #Tackle d_sellingseason = 'Fall'.
        if d_sellingseason == 'Fall':
            #Incase there are multiple d_year associated with one d_datekey.
            if d_datekey not in dwdateDict:
                dwdateDict[d_datekey] = []
            dwdateDict[d_datekey].append(d_year)

#Tackle output from 1st MapReduce.
for line in sys.stdin:
    columnName = line.strip().split('|')
    p_category = columnName[0]
    lo_revenue = columnName[1]
    lo_orderdate = columnName[2]
    
    #Tackle lo_orderdate = d_datekey.
    if lo_orderdate in dwdateDict:
        d_yearList = dwdateDict[lo_orderdate]
        for d_year in d_yearList:
            print(f"{d_year}|{p_category}|{lo_revenue}")


# #### Reducer 2 - Perform groupby and aggregation.

# In[46]:


#!/usr/bin/python
import sys

#Initiate counters to calculate sum and respective total count.
revSum = 0
revCount = 0
#Dictionary to store aggregated values.
joinDict = {}

print('sum(lo_revenue)|count(*)|d_year|p_category')

for line in sys.stdin:
    columnName = line.strip().split('|')
    d_year = columnName[0]
    p_category = columnName[1]
    lo_revenue = int(columnName[2])
    
    #Tackle group by d_year, p_category.
    #Stored as tuple for dictionary key.
    if (d_year, p_category) not in joinDict: #If new lo_orderdate.
        joinDict[(d_year, p_category)] = {'revSum': 0, 'revCount': 0}
            
    #Keep appending.
    joinDict[(d_year, p_category)]['revSum'] += lo_revenue
    joinDict[(d_year, p_category)]['revCount'] += 1

#For each item, sum revenue output is already calculated here.
for keys, values in joinDict.items():
    print(f"{values['revSum']}|{values['revCount']}|{keys[0]}|{keys[1]}")


# ### Command - Using Hadoop Streaming.

# --1st MR -- \
# time hadoop jar hadoop-streaming-2.6.4.jar -input /data/lineorder.tbl,/data/part.tbl -output /data/Part2_MR1_Output -mapper part2_mapper1.py -reducer part2_reducer1.py -file part2_mapper1.py -file part2_reducer1.py
# 
# --2nd MR -- \
# time hadoop jar hadoop-streaming-2.6.4.jar -input /data/Part2_MR1_Output/part-00000 -output /data/Part2_MR2_Output -mapper part2_mapper2.py -reducer part2_reducer2.py -file part2_mapper2.py -file part2_reducer2.py -file dwdate.tbl

# ### Command - Using MapReduceChain.java
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.streaming.StreamJob;


public class MapReduceChain extends Configured implements Tool
{
    public int run( String[] args) throws Exception
    {
        JobControl jobControl = new JobControl( "Part2");

        String[] job1Args = new String[]
        {
            "-mapper"   , "part2_mapper1.py",
            "-reducer"  , "part2_reducer1.py",
            "-input"    , "/data/lineorder.tbl",
            "-input"    , "/data/part.tbl",
            "-output"   , "/Part2_MR1_Output/",
            "-file"	    , "part2_mapper1.py",
            "-file"	    , "part2_reducer1.py"
        };
        JobConf job1Conf = StreamJob.createJob(job1Args);
        Job job1 = new Job(job1Conf);
        jobControl.addJob(job1);

        String[] job2Args = new String[]
        {
            "-mapper"   , "part2_mapper2.py",
            "-reducer"  , "part2_reducer2.py",
            "-input"    , "/Part2_MR1_Output/part-00000",
            "-output"   , "/Part2_MR2_Output/",
            "-file"     , "part2_mapper2.py",
            "-file"     , "part2_reducer2.py",
            "-file"     , "dwdate.tbl"
        };
        JobConf job2Conf = StreamJob.createJob(job2Args);
        Job job2 = new Job(job2Conf);
        job2.addDependingJob(job1);
        jobControl.addJob(job2);

        Thread runJobControl = new Thread(jobControl);
        runJobControl.start();
        while(!jobControl.allFinished())
        {
            // wait here
        }

        return 0;
    }

    public static void main( String[] args) throws Exception
    {
        int result = ToolRunner.run(new Configuration(), new MapReduceChain(), args);
        System.exit(result);
    }
}
# #### Code

# vim MapReduceChain.java
# 
# javac -cp ~/hadoop-streaming-2.6.4.jar:$(hadoop classpath) MapReduceChain.java
# 
# jar -cf MapReduceChain.jar MapReduceChain.class
# 
# jar xvf MapReduceChain.jar META-INF/MANIFEST.MF
# 
# nano META-INF/MANIFEST.MF
# -- Paste in: Class-Path: /home/ec2-user/hadoop-streaming-2.6.4.jar --
# 
# jar cmf META-INF/MANIFEST.MF MapReduceChain.jar MapReduceChain.class
# 
# hadoop jar MapReduceChain.jar MapReduceChain
