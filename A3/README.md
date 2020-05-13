# Overview of the Assignment

In Assignment 3, you will complete three tasks. The goal is to let you be familiar with Min-Hash, Locality Sensitive Hashing (LSH), and various types of **recommendation systems**.

# Tasks

## Task1: Min-Hash + LSH

In this task, you will implement the Min-Hash and Locality Sensitive Hashing algorithms with Jaccard similarity to find similar business pairs in the train_review.json file. We focus on **the 0 or 1 ratings** rather than the actual ratings/stars in the reviews. Specifically, if a user has rated a business, the user’s contribution in the characteristic matrix is 1. If the user hasn’t rated the business, the contribution is 0. Table 1 shows an example. **Your task is to identify business pairs whose Jaccard similarity is >= 0.05.**

![](https://github.com/lzl12051/INF553-Foundations-and-Applications-of-Data-Mining/raw/master/A3/pics/Snip20200512_3.png)

You can define any collection of hash functions that you think would result in a consistent permutation of the row entries of the characteristic matrix. Some potential hash functions are:

**𝑓(𝑥) = (𝑎𝑥 + 𝑏) % 𝑚**

**𝑓(𝑥) = ((𝑎𝑥 + 𝑏) % 𝑝) % 𝑚**

where 𝑝 is any **prime number**; 𝑚 is the number of bins. You can define any combination for the parameters (𝑎, 𝑏, 𝑝, or 𝑚) in your implementation.

After you have defined all the hash functions, you will build the signature matrix using Min-Hash. Then you will divide the matrix into 𝒃 bands with 𝒓 rows each, where 𝒃 × 𝒓 = 𝒏 (𝒏 is the number of hash functions). You need to set 𝒃 and 𝒓 properly to balance the number of candidates and the computational cost. Two businesses become a candidate pair if their signatures are identical in at least one band.

Lastly, you need to verify the candidate pairs using their original Jaccard similarity. The table below shows an example of calculating the Jaccard similarity between two businesses. Your final outputs will be the business pairs whose Jaccard similarity is >= 0.05.

| |user1| user2|user3|user4|
|----------|----------|----------|----------|----------|
| business1|0|1|1|1|
| business2 |0|1|0|0|

## Task2: Content-based Recommendation System

In this task, you will build a content-based recommendation system by generating profiles from review texts for users and businesses in the train review set. Then you will use the system/model to predict if a user prefers to review a given business, i.e., computing the cosine similarity between the user and item profile vectors.

During the training process, you will construct business and user profiles as the model:

1. Concatenating all the review texts for the business as the document and parsing the document, such as removing the punctuations, numbers, and stopwords. Also, you can remove extremely rare words to reduce the vocabulary size, i.e., the count is **less than 0.0001%** of the total words.
2. Measuring word importance using TF-IDF, i.e., term frequency * inverse doc frequency
3. Using **top 200 words** with highest TF-IDF scores to describe the document
4. Creating a Boolean vector with these significant words as the business profile
5. Creating a Boolean vector for representing the user profile by aggregating the profiles of the items that the user has reviewed

During the predicting process, you will estimate if a user would prefer to review a business by computing the cosine distance between the profile vectors. **The (user, business) pair will be considered as a valid pair if their cosine similarity is >= 0.01. You should only output these valid pairs.**

## Task3: Collaborative Filtering Recommendation System

In this task, you will build collaborative filtering recommendation systems with train reviews and use the models to predict the ratings for a pair of user and business. You are required to implement 2 cases:

* **Case 1**: Item-based CF recommendation system

  In Case 1, during the training process, you will build a model by computing the Pearson correlation for the business pairs that have **at least three co-rated users**. During the predicting process, you will use the model to predict the rating for a given pair of user and business. You must use **at most N business neighbors** that are most similar to the target business for prediction (you can try various N, e.g., 3 or 5).

* **Case 2**: User-based CF recommendation system with Min-Hash LSH

  In Case 2, during the training process, since the number of potential user pairs might be too large to compute, you should combine the Min-Hash and LSH algorithms in your user-based CF recommendation system. You need to:

  1. Identify user pairs who are similar using their co-rated businesses without considering their rating scores (similar to Task 1). This process reduces the number of user pairs you need to compare for the final Pearson correlation score. 
  2. Compute the Pearson correlation for the user pair candidates that have **Jaccard similarity >= 0.01 and at least three co-rated businesses**. The predicting process is similar to Case 1.