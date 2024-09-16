# Project overview 
The social network is planning to introduce a friend recommendation system. The application will suggest the user to write to a person if the user and the addressee:
- Are in the same channel,
- have never corresponded before,
 - are within 1 kilometer of each other.
At the same time, the team wants to better study the audience of the social network in order to launch monetization in the future. For this purpose, it was decided to conduct geoanalytics:
- Find out where the majority of users are based on the number of posts, likes and subscriptions from one location.
- See where the most new users sign up.
- Determine how often users travel and which cities they choose.

# Storage Structure:
Layer raw_data - raw data partitioned by date in parquet format /user/master/data
ODD layer - sandbox for analysts /user/analytics/project/
          - parquet data partitioned by date and type of events /user/analytics/project/data/events
          - directory with coordinates of cities and time zones /user/analytics/project/data/cities/geo.csv
Datamart /user/analytics/mart/users_mart - datamart by users
                            /zones_mart - datamart by zones
                            /friends_recommendation_mart - datamart for friends recommendation
Running ETL once a day
- copying data into ODD c layer with partitioning by date and type of events

- datamart calculation by users
To determine in which city the event took place, the distance of the message sending point to the city center is calculated. The event refers to the city with the shortest distance.

- datamart calculation by zones
Geolayer - distribution of event-related attributes by geographical zones (cities)

- calculating a datamart for friend recommendations
Friend recommendation works as follows: if users are subscribed to the same channel, have never corresponded before and the distance between them does not exceed 1 km, they will both be asked to add the other as a friend.

At this stage DAGs are not scheduled to run, to implement ETL you will need to apply the parameter cathcup = True and use the 
'{{ ds }}' 

The function total_repartition_func.py was used to manually copy data to ODDs