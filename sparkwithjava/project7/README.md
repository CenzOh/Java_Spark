# Project 7 Lessons 21-24

### Quick Troubleshoot
Import Maven project worked for me.

### Reading the files
`com.jobreadyprogrammer.spark` main package for the spark apps<br>
`...Breakdown.java` my version of the code<br>
`StreamingSocketApplication.java` example of data streaming (streaming socket) into spark. breaking up words based on whitespace. output to console and looked at modes complete and update<br>
`StreamingFIleDirectoryApplication.java`example of streaming by pointing to a certain directory and having spark wait for files to enter the folder to be ingested<br>
`StremaingKafkaConsumer.java` using kafka to stream. Kafka is a distributed streaming platform.<br>

### lecture 22 streaming network socket examples
Intro to conetp of streaming in spark. Spark works well with big datasets like terabyte or petabytes worth. Works across machines.<br>
This is called batch jobs. The batch jobs can invovle paralyzed ingestion of data, various transformations, computations, etc.<br>
What if we want real time insight, visit a reporting dashboad lets say a 100 times a day. Not feasible to run 100 TB job every few minutes. Expensive!!!<br>
Not enough time to get job done too. Unnecessary work to do over and over. Spakr is capable of incremental processing of data.<br>
Spark streaming saves the day. CLuster has capacity to ingest and process smaller chunks such as 1GB of data every couple of minutes. FOr up to date info.<br>
Spark sql engine willt ake care of running it incrementally and continuously.<br>
Internally, queries processed using micro batch processing engine. Small batch jobs. SOme latency around one hundred milliseconds<br>

Documentation available on spark website. You can see a page of basic concept that explain it.<br>
Data stream -> unbound table -> new data in data stream = new rows appended to unbound table. <br>
THis is the input based on a query. Input grows as more data comes in. End result will be same size. <br>
Example is a microsocket where first record contains cat dog, second contains dog dog. Dog appears 3 times. Result of table worrd counts is cat 1, dog 3.<br>
Next incoming screen has input owl cat. Owl added as new result (df) so result df is cat 2, dog 3, owl 1. The input is UNBOUNDED. Input -> result.<br>
output complete mode reutrns entire ouptu. Kafka is a messagin system. <br>
Review first example `StreamingSocketApplication`

### Lecture 23 stock market files streaming example
So last time, we were connected to a directory and it listened for files to come in and then process the files such as CSV, json, etc<br>
This example we will download ONE directory with a bunch of files init<br>
Refer to `streamingFileDirectoryApplication.java` file

### Lecture 24 Kafka with Spark streaming
Kafka is a popular distributed streaming platform. Published and subscribed system.<br>
Messages can be published and consumed from. Kafka can be on one machine or multiple (being a cluster)<br>
Example:<br>
Producers - Kafka Cluster - - Consumers<br>
3 Apps ---> Sales. Brokers -----> 2 Sales -> App <br>
3 Apps ---> Customer. Zookeeper -> 1 customer -> App<br>

The example from lecture is a kafka cluster consisting on three notes / nodes.<br>
Example think online retail company with mobile apps,m website, desktop app, browser plugins.<br>
As customers, they use these different apps. Useful data is being generated such as how long has a customer looked at a given product, or did they view it before making a purchase.<br>
Would be useful for us to collect that app in our itnernal streeaming app toprocess the data in real time. Kafka can do this.<br>
Each producer app can produce messages of a particaulr cateogry (topic). In the ex, the first three apps are producing messages on SALES topic.<br>
Next three apps are producing messages on the CUSTOMER topic. THe topics are message cues on which messages c an be publsihed. <br>
THis means records like from a database table. Messages aree key value pairs, value is record taht we want to collect. <br>
Now on the right side, the first two consumer apps are subscribing to the sales topic and the third consumer app is subscribed to the customer topic<br>
The thre brokers are kafka software installed on three servers. There is also a cluster manager, it is configed with kafka cluster (zookeeper)<br>

In the java code we will make a spark consumer app, it can perform a computation onf the messages published on a certain topic.<br>
Spark consumer will subscrube to a certain topic, and recieve every message that ets published on that topic. <br>
We will have one node in our example.<br>

Download kafka here:<br>
`https://kafka.apache.org/quickstart`<br>
Make sure to extract it, then cd into the folder and start the server. Directions on website.<br>
In the example, we launch zookeeper with the starter script. Takes a minute.<br>
Then open a new termianl tab and start the kafka server with given command.<br>
Default port is 0.0.0.0.9092. <br>
Step three is create a topic, in the example we call the topic test. DO this in a third terminal window, and CD back to the kafka folder.<br>
Step 4 lets send messages to the topic. Again directions have the directions how to do it. Can do this on third terminal window.<br>
You know when it works when you see the angle bracket `>` ready for you to type. <br>
Then we start a consumer. SO any message we type in the terminal will be sent onto the topic called test and spark will be consuming the test topic.<br>
Now refer to `streamingKafkaConsumer.java` file.<br>