# Smart-City-Real-Time-Streaming
This project generates data real-time of a Smart-City, where a vehicle travels from Central London to Birmingham

This project generates data real-time and stores it inside different Kafka Topics. Using Spark, we then write the data from the Kafka Topics inside a AWS S3 bucket. Data from this bucket is then crawled into AWS Glue and queried using AWS Athena. The data is then finally inserted into AWS Redshift, and is visualized using Power BI connected to AWS Redshift

![image](https://github.com/pranavsharma9/Smart-City-Real-Time-Streaming/assets/49152887/96cf64c1-34d8-4289-83a6-07ca573884e2)
