# 🔢 K-Means & K-Median Clustering with Hadoop MapReduce  

## 📜 Overview  
This project implements **K-Means and K-Median Clustering using Hadoop MapReduce** on a **three-node cluster**. The **K-Means algorithm** calculates clusters based on **Euclidean distance**, while **K-Median** uses **Manhattan distance**.  

📌 **Goals**:  
- Implement **K-Means & K-Median clustering** using **Hadoop Streaming**.  
- Optimize centroid selection strategies.  
- Compare **manual MapReduce execution vs. Java-based MapReduceChain execution**.  

📌 **Dataset**: **2,000,000 rows** and **5 feature columns**  

## 🚀 Part 1: K-Means Clustering  
### **1️⃣ Data Generation**  
### **2️⃣ K-Means MapReduce Implementation**
#### Mapper (kmapper.py)
📌 **Roles:**
- Reads centroid file (centers.txt).
- Computes Euclidean distance between data points and centroids.
- Assigns each point to the closest centroid.
#### Reducer (kreducer.py)
📌 **Roles:**
- Aggregates assigned points for each cluster.
- Computes new centroids as the mean of each cluster.
- Outputs updated centroid positions.
### 3️⃣ Hadoop Streaming Execution
### 4️⃣ Automating MapReduce with Java (MapReduceChain.java)
- Instead of manually executing MapReduce, we implemented a Java program to handle iterations automatically.

## 🟠 Part 2: K-Median Clustering
### **1️⃣ K-Median MapReduce Implementation**
📌 **Differences from K-Means:**
- Instead of mean, we calculate the median for each cluster.
- Uses Manhattan distance instead of Euclidean distance.
#### Mapper (kmedian_mapper.py)
📌 **Roles:**
- Reads centroid file (centers.txt).
- Computes Manhattan distance for each data point.
- Assigns the point to the nearest centroid.
#### Reducer (kmedian_reducer.py)
📌 **Roles:**
- Computes medians for each cluster.
- Outputs updated centroid positions.
### 2️⃣ Hadoop Streaming Execution for K-Median
- Subsequent iterations (2nd to 4th) follow the same process, using the updated centroid file from the previous iteration.

## 🏆 Part 3: Two-Step MapReduce Query Execution
### Perform two-step MapReduce execution to:
- Join lineorder & part tables.
- Map-side join with dwdate table and group results by d_year, p_category.

## 🏆 Key Findings & Takeaways
### ✅ K-Means vs. K-Median
- K-Means is more efficient but sensitive to outliers.
- K-Median is robust against outliers but takes slightly longer.
- Java-based MapReduce automation significantly reduces manual execution overhead.
### ✅ Two-Step MapReduce Execution
- Map-side joins optimize performance over reduce-side joins.
- Group-by aggregation improves efficiency in analyzing large datasets.

## 🚀 Technologies Used
### 🛠 Big Data Frameworks:
- Hadoop (MapReduce, HDFS, Streaming)
- Python (NumPy, Pandas)
- Java (MapReduceChain Implementation)
