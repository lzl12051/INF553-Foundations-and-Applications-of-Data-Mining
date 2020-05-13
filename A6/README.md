# Overview of the Assignment

In this assignment, you are going to implement three algorithms: **the Bloom filtering, Flajolet-Martin algorithm, and reservoir sampling**. For the first task, you will implement Bloom Filtering for off-line Yelp business dataset. The “off-line” here means you do not need to take the input as streaming data. For the second and the third task, you need to deal with on-line streaming data directly. In the second task, you need to generate a simulated data stream with the Yelp dataset and implement Flajolet-Martin algorithm with Spark Streaming library. In the third task, you will do some analysis on Twitter stream using fixed size sampling (Reservoir Sampling).

# Tasks 

##  Task1: Bloom Filtering

You will implement the Bloom Filtering algorithm to estimate whether the city of a business in business_second.json has shown before in business_first.json. The details of the Bloom Filtering Algorithm can be found at the streaming lecture slide. You need to find proper bit array size, hash functions and the number of hash functions in the Bloom Filtering algorithm. Some possible the hash functions are:

> f(x)= (ax + b) % m or f(x) = ((ax + b) % p) % m 

Where p is any prime number and m is the length of the filter bit array. You can use any combination for the parameters (a, b, p). The hash functions should keep the same once you created them. Since the city of a business is a string, you need to convert it into an integer and then apply hash functions to it. The following code shows one possible solution:

```python
import binascii 
int(binascii.hexlify(s.encode('utf8')),16)
```

## Task2: Flajolet-Martin algorithm

In task2, you will implement the Flajolet-Martin algorithm (including the step of combining estimations from groups of hash functions) to estimate the number of unique cities within a window in the data stream. The details of the Flajolet-Martin Algorithm can be found at the streaming lecture slide. You need to find proper hash functions and the proper number of hash functions in the Flajolet-Martin algorithm.

![Snip20200513_3](/Users/zhilong/git/INF553-Foundations-and-Applications-of-Data-Mining/A6/pics/Snip20200513_3.png)

**Execution Details**

In Spark Streaming, set the batch duration to 5 seconds:

```python
ssc=StreamingContext(sc , 5) 
```

You will get a batch of data in spark streaming every 5 seconds. The window length should be 30 seconds and the sliding interval should be 10 seconds. 

## Task3: Fixed Size Sampling on Twitter Streaming

You will use Twitter API of streaming to implement the fixed size sampling method (Reservoir Sampling Algorithm) and find popular tags on tweets based on the samples. In  this  task,  we  assume  that  the  memory  can  only  save  100  tweets,  so  we  need  to  use  the fixed  size  sampling  method  to  only  keep  part  of  the  tweets  as  a  sample  in  the  streaming. When  the  streaming  of  the  Twitter  coming,  for  the  first  100  tweets,  you  can  directly  save them in a list. After that, for the nth twitter, you will keep the nth tweet with the probability of  100/n,  otherwise  discard  it.  If  you  keep  the  nth tweet, you need to randomly pick one in the list to be replaced. If the coming tweet has no tag, you can directly ignore it. You also need to keep a global variable representing the sequence number of the tweet. If the coming  tweet  has  no  tag,  the  sequence  number  will  not  increase,  otherwise  the  sequence number increases by one. Every time you receive a new tweet, you need to find the tags in the sample list with the top 3 frequencies.

