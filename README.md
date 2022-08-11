# DeutscheBahnDataChallanges
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

This project was created in the context of the Data Challange 2022, set by the DB Systel GmbH in corporation with the
Goethe University Frankfurt.

## Team Twitter Sentiment
We focused on the **Twitter user sentiment development** during the corona crisis with focus on the **9€-Ticket**. 
In more detail we compared the user sentiment before the 9€-Ticket and the sentiment during the 9€-Ticket in relation to
strong represented railway routes in Germany. 

The following questions were evaluated:
* What impact has the 9€-Ticket on the sentiment of DB customers?
* Is the customer sentiment related to the geo-location?
* Is there a connection between the €9 ticket and the sentiment on certain railway lines?

## Workflow
1. Tweet collection via Twitter API 2.0 with queries
2. Quality and quantity analysis
3. Evaluation of geo-data
4. Text-based generation of geo-data
5. Detection of relevant users
6. Deep user history search
7. Sentiment analysis

## Results

* 40.000 9€-Ticket related tweets
* 21.000 individual users
* Tweettext-based geo-data mining resulted in 15.000 Tweets with geo-location
* Additional 1.000.000 tweets evaluated during user-history search.
* **480 users** with hometown + travel destination + 9€-Ticket related tweet + DB related history tweet identified

### DB Customer Sentiment
* The sentiment was manually evaluated for the top 12 city-city connections based on relevant of the 480 identified
 customers.

**&#8594;** After resolving not representative sentiments we could detect a consistent slightly sentiment shift towards 
positive for 11 of the 12 city-city connections.

**&#8594;** This resulted in the assumption that the 9€-Ticket has a **positive impact** on Twitter user, which actually use the 
ticket. 

**&#8594;** Most of the negative tweets related to the 9€-Ticket are not related to travel routes or were tweeted by user
 with a bad DB-related sentiment even before the 9€-Ticket release.


## How to use
This project was only created as a prototype to demonstrate the possibilities of Twitter data mining for the Deutsche 
Bahn. The workflow can be adapted to other topics and used as framework. For this many adaptions are necessary, it 
is not recommended.
