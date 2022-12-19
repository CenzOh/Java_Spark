# Project 7 Lessons 19-20

### Quick Troubleshoot
Import Maven project worked for me.

### Reading the files
`com.jobreadyprogrammer.spark` main package for the spark apps<br>
`StreamingSocketApplication.java`
`StreamingFIleDirectoryApplication.java`
`StremaingKafkaConsumer.java`

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