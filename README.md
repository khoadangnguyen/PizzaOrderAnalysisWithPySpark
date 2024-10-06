# Pizza Order Analysis With PySpark
PySpark application designed to analyze pizza order dataset

### Description
This is a PySpark application utilizing pizza order dataset with about 21,000 orders of 48,000 items 
to extract below insights

- Retrieve the total number of orders placed.
- Retrieve the total number of ordered items.
- Calculate the average number of items per order.
- Calculate the total revenue generated from pizza sales.
- Identify the highest-priced pizza.
- List the top 5 most ordered pizza types along with their quantities.
- Find the total ordered quantity of each pizza category.
- Determine the distribution of orders by hour of the day.
- Find the category-wise distribution of pizzas.

### Setup

- This PySpark application runs with only one node which is also the driver itself.
- Driver and executor logs are configured to store under app/log folder.
- The event log is enabled and configured to store in ./SparkLogs
- The history server is configured to load event data from the same ./SparkLogs
```bash
spark.eventLog.enabled=true
spark.eventLog.dir=./SparkLogs
spark.history.fs.logDirectory=./SparkLogs
```
- Spark submit command
```bash
spark-submit \
--conf spark.driver.port=7077 \
--conf spark.blockManager.port=7078  \
--conf spark.ui.port=4041 \
--conf spark.port.maxRetries=100 \
--conf spark.driver.bindAddress=127.0.0.1 \
--files ./app/log4j.properties \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:./app/log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:./app/log4j.properties" \
./app/pizza_order.py
```
- History server start command
```bash
start-history-server.sh
```

### Future improvements
- Utilize Docker to simulate multiple executors and observe the logs output by multiple executors
- Improve data storage for multiple executors to access (simulate distributed environment)