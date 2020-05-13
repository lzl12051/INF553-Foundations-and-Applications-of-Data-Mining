# Overview of the Assignment 

In Assignment 5, you will implement the **K-Means and Bradley-Fayyad-Reina (BFR) algorithm**. The goal is to help you be familiar with clustering algorithms with various distance measurements. The datasets you are going to use are synthetic datasets.

# Task

You  will  write  the  K-Means  and  Bradley-Fayyad-Reina  (BFR)  algorithms  from  scratch.  You  should implement K-Means as the main-memory clustering algorithm that you will use in BFR. You will iteratively load  the  data  points  from  a  file  and  process  these  data  points  with  the  BFR  algorithm. See  below pseudocode for your reference.

![Snip20200513_2](/Users/zhilong/git/INF553-Foundations-and-Applications-of-Data-Mining/A5/pics/Snip20200513_2.png)

In BFR, there are three sets of points that you need to keep track of: **Discard set (DS), Compression set (CS), Retained set (RS)**. For each cluster in the DS and CS, the cluster is summarized by:

> N: The number of points 
>
> SUM: the sum of the coordinates of the points 
>
> SUMSQ: the sum of squares of coordinates

# Appendix

The implementation details of the BFR algorithm (you can/should have your own implementation; this is only for reference). Suppose the number of clusters is  K and the number of dimensions is d.

1. Load the data points from one file.

2. Run  K-Means  on  a  small  random  sample  of  the  data  points  to  initialize  the  K  centroids  using  the Euclidean distance as the similarity measurement.

3. Use the K-Means result from b to generate the DS clusters (i.e., discard points and generate statistics). 

4. The initialization of DS has finished, so far, you have K clusters in DS.

5. Run  K-Means  on  the  rest  of  the  data  points  with  a  large  number  of  clusters  (e.g.,  5  times  of  K)  to generate CS (clusters with more than one points) and RS (clusters with only one point).

6. Load the data points from next file.

7. For the new points, compare them to  the clusters in DS  using the Mahalanobis Distance and assign them to the nearest DS cluster if the distance is < 𝛼√𝑑.

8. For the new points that are not assigned to DS clusters,  using the Mahalanobis Distance and assign the points to the nearest CS cluster if the distance is < 𝛼√𝑑.

9. For the new points that are not assigned to any clusters in DS or CS, assign them to RS.

10. Merge the data points in RS by running K-Means with a large number of clusters (e.g., 5 times of K) to generate CS (clusters with more than one points) and RS (clusters with only one point).

11. Merge clusters in CS that have a Mahalanobis Distance < 𝛼√𝑑

12. Repeat the steps 6 ~ 11 until all the files are processed.

13. If this is the last round (after processing the last chunk of data points), merge clusters in CS with the clusters in DS that have a Mahalanobis Distance < 𝛼√𝑑.

    **(𝛼 is a hyper-parameter, you can choose it to be around 2, 3 or 4)**