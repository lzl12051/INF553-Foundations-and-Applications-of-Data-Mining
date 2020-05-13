# Overview of the Assignment

In Assignment 3, you will complete three tasks. The goal is to let you be familiar with Min-Hash, Locality Sensitive Hashing (LSH), and various types of **recommendation systems**.

# Tasks

## Task1: Min-Hash + LSH

In this task, you will implement the Min-Hash and Locality Sensitive Hashing algorithms with Jaccard similarity to find similar business pairs in the train_review.json file. We focus on **the 0 or 1 ratings** rather than the actual ratings/stars in the reviews. Specifically, if a user has rated a business, the user’s contribution in the characteristic matrix is 1. If the user hasn’t rated the business, the contribution is 0. Table 1 shows an example. **Your task is to identify business pairs whose Jaccard similarity is >= 0.05.**

