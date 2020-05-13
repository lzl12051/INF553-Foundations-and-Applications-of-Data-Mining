# Overview of the Assignment

In this assignment, you will explore the spark GraphFrames library as well as implement your own Girvan-Newman algorithm using the Spark Framework to detect communities in graphs. You will use the ub_sample_data.csv dataset to find users who have a similar business taste. The goal of this assignment is to help you understand how to use the Girvan-Newman algorithm to detect communities in an efficient way within a distributed environment.

# Tasks

## Graph Construction

To construct the social network graph, each node represents a user and there will be an edge between two nodes if the number of times that two users review the same business is **greater than or equivalent** to the filter threshold. For example, suppose user1 reviewed [business1, business2, business3] and user2 reviewed [business2, business3, business4, business5]. If the threshold is 2, there will be an edge between user1 and user2.

If the user node has no edge, we will not include that node in the graph. In this assignment, we use filter threshold 7.

## Task1: Community Detection Based on GraphFrames

In task1, you will explore the Spark GraphFrames library to detect communities in the network graph you constructed in the library, it provides the implementation of the Label Propagation Algorithm (LPA) which was proposed by Raghavan, Albert, and Kumara in 2007. It is an iterative community detection solution whereby information “flows” through the graph based on underlying edge structure. In this task, you do not need to implement the algorithm from scratch, you can call the method provided by the library. This [websites](https://docs.databricks.com/spark/latest/graph-analysis/graphframes/user-guide-python.html) may help you get started with the Spark GraphFrames.

## Task2: Community Detection Based on Girvan-Newman algorithm

In task2, you will implement your own Girvan-Newman algorithm to detect the communities in the network graph. Because you task1 and task2 code will execute separately, you need to construct the graph again in this task following the rules in section Graph Construction You can refer to the Chapter 10 from the Mining of Massive Datasets book for the algorithm details.

For task2, you can ONLY use Spark RDD and standard Python libraries. **Remember to delete your code that imports graphframes.**

### Task2.1: Betweenness Calculation 

In this part, you will also need to calculate the betweenness for each edge in the original graph you constructed in above section. Then you need to save your result in a txt file. The format of each line is

**(‘user_id1’, ‘user_id2’), betweenness value**

Your result should be firstly sorted by the betweenness values in the descending order and then the first user_id in the tuple in lexicographical order (the user_id is type of string). The two user_ids in each tuple should also in lexicographical order. 

### Task2.2: Community Detection

You are required to divide the graph into suitable communities, which reaches the global highest modularity. The formula of modularity is shown below:

![](https://github.com/lzl12051/INF553-Foundations-and-Applications-of-Data-Mining/raw/master/A4/pics/Snip20200513_1.png)

According to the Girvan-Newman algorithm, after removing one edge, you should **re-compute the betweenness**. The “m” in the formula represents the edge number of the original graph. The “A” in the formula is the adjacent matrix of the original graph. (Hint: In each remove step, “m” and “A” should not be changed)

If the community only has one user node, we still regard it as a valid community.

