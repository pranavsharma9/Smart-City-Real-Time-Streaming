# Smart-City-Real-Time-Streaming
This project generates real-time data of a Smart-City, where a vehicle travels from Champaign to Chicago.

The project generates and stores real-time data in different Kafka topics. Using Spark, we then write the data from the Kafka topics to an AWS S3 bucket. This data is subsequently crawled into AWS Glue and queried using AWS Athena. Finally, the data is inserted into AWS Redshift and visualized using Power BI connected to AWS Redshift.

![image](https://github.com/pranavsharma9/Smart-City-Real-Time-Streaming/assets/49152887/96cf64c1-34d8-4289-83a6-07ca573884e2)
