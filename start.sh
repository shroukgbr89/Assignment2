##### part 2 ####
docker pull mongodb/mongodb-community-server

## Step 1: Copy the transactions.json to the Docker Container
docker cp transactions.json mangodb-container:/transactions.json

# Step 2: execute mangodb-container bash >> Switches to Docker Shell
docker exec -it mangodb-container bash

## Step 3: Import the JSON Data into MongoDB
mongoimport --db TransDB --collection TransColl --file /transactions.json --jsonArray

mongosh # Switches to Mongo Shell

# Mongo Shell to switched to db TransDB
use TransDB

#Run Aggregation Queries
db.TransColl.createIndex({ category: 1 })
db.TransColl.aggregate([
    { $group: { _id: "$category", totalPurchases: { $sum: "$quantity" } } },
    { $sort: { totalPurchases: -1 } },
    { $limit: 5 }
])
db.TransColl.find({ $and: [{ quantity: { $gt: 2 } }, { price: { $gt: 100 } }] })


#### part 2 #####

#Copy assig2.jar into the Docker container
docker cp "C:\Users\lenovo\Downloads\Telegram Desktop\assig2 (2)\assig2\out\artifacts\assig2\assig2.jar" namenode:/home
docker cp "C:\Users\lenovo\Downloads\uouo\Assignment2\transactions.csv" namenode:/home/transactions.csv

#Access the Docker container
docker exec -it namenode bash
cd home
ls
# To put assi2.jar into HDFS:
hdfs dfs -put /home/assi2.jar /ass2/
#To put transactions.csv into HDFS
hdfs dfs -put /home/transactions.csv /ass2/

#Verify that the files are now in HDFS:
hdfs dfs -ls /ass2

#runs a Hadoop MapReduce job using a JAR file (assi2.jar) and processes an input file (transactions.csv) in HDFS
hadoop jar /home/assi2.jar CategoryRevenueDriver /ass2/transactions.csv /ass2/total_output/

#lists the contents of the HDFS directory /ass2/total_output/
hdfs dfs -ls /ass2/total_output/

#display the contents of a file stored in HDFS.
hadoop fs -cat /ass2/total_output/output

#copy a file from HDFS to the local filesystem.
hdfs dfs -get /ass2/total_output/output "C:/Users/lenovo/Downloads/Telegram Desktop/assig2 (2)/assig2/out/artifacts/assig2/"

#Copy the file from the container to your local machine using docker cp
docker cp namenode:/home/output "C:/Users/lenovo/Downloads/Telegram Desktop/assig2 (2)/assig2/out/artifacts/assig2/"
