# Streaming Processing with Pyspark 101

This simple pyspark code was one of the assignment from data-eng bootcamp and one of my learnin process to get hands-on dirty.

The kafka and Spark that is used in here is referenced from [here](https://github.com/thosangs/dibimbing_spark_airflow)

How  to run
1. open this [link](https://github.com/thosangs/dibimbing_spark_airflow) and run `make docker-build`
2. after that create the kafka cluster by running `make create-kafka` and spark `make spark`
3. running the event producer `make produce-spark` and `make consume-spark`

the objective of the assignemnt is to create running total from following events that produced from kafka and should be consume by spark that will show the running total like this:<br>
![output example](<Pasted Graphic.png>)

code explanation in `spark-event-consumer.py`:<br>
1. spark -> creating spark session and to reduce the logs to show the the LOG error(to minimize some random logs and easier to trace)
2. stream_df -> read from kafka topic that have name 'test-topic
3. parsed_df -> add the streaming as df and timestamp conversion fro windowing and based aggregations
4. running_total_df -> will set watermark on event_time (is used to handle late data) with 5 minute thershold. And it will group by(or windows in streaming process) based on event_time. Each window will covers a 10 minutes period and start every 5 minutes. after that it will aff the price as running_total_price and show in the output of terminal consisting the start_time,end_time, as well as running_total_price
5. query -> and lastly, it will write the the streaming dataframe into external sink(add batches to output). the output mode complete means entire resutl table will be written to sink after each trigger and it will runnig indefenilty because of awaitTermnination. will be stopped manually so the data also stremanig contionuslt  