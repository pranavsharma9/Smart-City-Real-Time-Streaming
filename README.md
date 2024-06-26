# Smart-City-Real-Time-Streaming
This project generates data real-time of a Smart-City, where a vehicle travels from Central London to Birmingham

This project generates data real-time and stores it inside different Kafka Topics. Using Spark, we then write the data from the Kafka Topics inside a AWS S3 bucket. Data from this bucket is then crawled into AWS Glue and queried using AWS Athena. The data is then finally inserted into AWS Redshift, and is visualized using Power BI connected to AWS Redshift
