# Overview of the Assignment

In this assignment, you will implement the **SON** algorithm using the Apache Spark Framework. You will develop a program to find frequent itemsets in two datasets, one simulated dataset and one real-world dataset generated from Yelp dataset. The goal of this assignment is to apply the algorithms you have learned in class on large datasets more efficiently in a distributed environment.

**You are required to only use Spark RDD**



# Datasets

In this assignment, you will use one simulated dataset and one real-world dataset. In task 1, you will build and test your program with a small simulated CSV file that has been provided to you.

For task 2, you need to generate a subset using business.json and review.json from the Yelp dataset with the same structure as the simulated data. The figure below shows the file structure, the first column is user_id and the second column is business_id. In task2, you will test your code with this real-world data.

![](https://github.com/lzl12051/INF553-Foundations-and-Applications-of-Data-Mining/raw/master/A2/pic/Snip20200512_1.png)

# Tasks

In this assignment, you will implement the **SON algorithm** to solve all tasks (Task 1 and 2) on top of Apache Spark Framework. You need to find **all the possible combinations of the frequent itemsets** in any given input file within the required time. You can refer to Chapter 6 from the Mining of Massive Datasets book and concentrate on section 6.4 – Limited-Pass Algorithms. (Hint: you can choose either A-Priori, MultiHash, or PCY algorithm to process each chunk of the data)

## Task1: Simulated data

There are two CSV files (small1.csv and small2.csv) provided on the Vocareum in your workspace. The small1.csv is just a sample file that you can used to debug your code. In this task, you need to build two kinds of market-basket model.

### Case 1

You will calculate the combinations of **frequent businesses** (as singletons, pairs, triples, etc.) that are qualified as frequent given a support threshold. You need to create a basket for each user containing the business ids reviewed by this user. If a business was reviewed more than once by a reviewer, we consider this product was rated only once. More specifically, the business ids within each basket are unique. The generated baskets are similar to:

> **user1: [business11, business12, business13, ...]**
>
> **user2: [business21, business22, business23, ...]**
>
> **user3: [business31, business32, business33, ...]**

### Case 2

You will calculate the combinations of **frequent users** (as singletons, pairs, triples, etc.) that are qualified as frequent given a support threshold. You need to create a basket for each business containing the user ids that commented on this business. Similar to case 1, the user ids within each basket are unique. The generated baskets are similar to:

> **business1: [user11, user12, user13, ...]**
>
> **business2: [user21, user22, user23, ...]**
>
> **business3: [user31, user32, user33, ...]**



You need to **print the runtime in the console** with the “Duration” tag: “Duration: <time_in_seconds>”, eg: “Duration: 100.00”

## Task 2: Yelp data

In task2, you will explore the Yelp dataset to find the frequent business sets (**only case 1**). You will jointly use the business.json and review.json to generate the input user-business CSV file yourselves.

**(1) Data preprocessing**

You need to generate a sample dataset from business.json and review.json with following steps:

1. The state of the business you need is Nevada, i.e., **filtering ‘state’== ‘NV’**.

2. Select “user_id” and “business_id” from review.json whose “business_id” is from Nevada. Each line in the CSV file would be “user_id1, business_id1”.

3. The header of CSV file should be “user_id,business_id”. You need to save the dataset in CSV format. The figure below shows an example of the output file

![](https://github.com/lzl12051/INF553-Foundations-and-Applications-of-Data-Mining/raw/master/A2/pic/Snip20200512_2.png)

**(2) Apply SON algorithm**

The requirements for task 2 are similar to task 1. However, you will test your implementation with the large dataset you just generated. For this purpose, you need to report the total execution time. For this execution time, we take into account also the time from reading the file till writing the results to the output file. You are asked to find the frequent business sets (**only case 1**) from the file you just generated. The following are the steps you need to do:

1. Reading the user_business CSV file in to RDD and then build the case 1 market-basket model;

2. Find out qualified users who reviewed more than *k* businesses. (*k* is the filter threshold); 
3. Apply the SON algorithm code to the filtered market-basket model;

### Evaluation Metric
**Task 1:**

| Input File | Case | Support | Runtime (sec) |
| ---------- | ---- | ------- | ------------- |
| small2.csv | 1    | 4       | <=200         |
| small2.csv | 2    | 9       | <=200         |

**Task2:**

| Input File|Filter Threshold|Support|Runtime (sec)|
| ----------------- | ---------------- | ------- | ------------- |
| user_business.csv | 70               | 50      | <=2,000       |

