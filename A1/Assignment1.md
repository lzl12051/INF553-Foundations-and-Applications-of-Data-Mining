# Overview of the Assignment
In assignment 1, you will complete three tasks. The goal of these tasks is to help you get familiar with Spark operations (e.g., transformations and actions) and MapReduce.

# Tasks

## Task1: Data Exploration
You will explore the review dataset and write a program to answer the following questions: 
1. The total number of reviews
2. The number of reviews in a given year, y
3. The number of **distinct** users who have written the reviews
4. Top m users who have the largest number of reviews and its count
5. Top n frequent words in the review text. The words should be in lower cases. The following punctuations i.e., “(”, “[”, “,”, “.”, “!”, “?”, “:”, “;”, “]”, “)”, and the given stopwords are excluded

## Task2: Exploration on Multiple Datasets

In task2, you will explore the two datasets together (i.e., review and business) and write a program to compute the average stars for each business category and output top n categories with **the highest average stars**. The business categories should be extracted from the “categories” tag in the business file and split by comma (also need to remove leading and trailing spaces for the extracted categories). You need to implement a version **without Spark** and compare to a version with Spark for reducing the time duration of execution.

## Task3: Partition

In this task, you will learn how partitions work in the RDD. You need to compute the businesses that have more than **n** reviews in the **review file**. At the same time, you need to show the number of partitions for the RDD and the number of items per partition with **either default or customized partition function**. You should design a customized partition function to improve the computational efficiency, i.e., reducing the time duration of execution.
