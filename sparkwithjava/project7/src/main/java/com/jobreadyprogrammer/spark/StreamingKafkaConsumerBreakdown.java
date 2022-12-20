//lesson 24 my code
package com.jobreadyprogrammer.spark;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamingKafkaConsumerBreakdown {

	public static void  main(String args[]) throws StreamingQueryException {
		
		SparkSession spark = SparkSession.builder()
				.appName("StreamingKafkaConsumer")
				.master("local")
				.getOrCreate();
		
//kafka consumer
		Dataset<Row> messagesDf = spark.readStream() 				//create consumer here, we will call it message
				.format("kafka") 									//specify kafka to format
				.option("kafka.bootstrap.servers", "localhost:9092")//specify where the kafka cluster is located, localhost 9092 port.
				.option("subscribe", "test") 						//subscribe onto a certain topic. COnsumers subscribe to certain kinds of messages. Our case, it is test
				.load() 
				.selectExpr("CAST(value AS STRING"); 				//lines.selectExpr("CAST key AS STRING", "CAST value AS STRING") for key value pair. Treats key and value as given type.
		
//message.show() // <--- unable to do this whilest streaming
		Dataset<String> words = messagesDf 							//dataset row is a DF rememebr that
				.as(Encoders.STRING()) 								//each word is dumped into this words list which is a data set of STRINGS. A sentence being sent onto the queu ewill be split based on whitespace
				.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
		
		Dataset<Row> wordCountsDf = words.groupBy("value").count(); //this part simple, we just group by the values column and count occurances of words
		
		StreamingQuery query = wordCountsDf.writeStream() 			//after we get the word counts df, we want to write the stream
				.outputMode("complete") 							//use complete mode RATHER than append or update
				.format("console") 									//write stream to the console
				.start(); 											//execute writing the stream
		
		query.awaitTermination();

//now open up the terminal where we have the producer running (its the window with the >). Also right click and run as this app. We will see in eclipse conolse the JSON logging.
//this should be pinged every ten seconds or so. Here is our result at first:
//----------
//Batch: 0
//----------
//+-----+-----+
//|value|count|
//+-----+-----+

//no messages received yet. And the batch 0 means the table is empty. After submitting a message by writing in the producer terminal ">Hello there my name is imtiaz" hit enter
//consumer is trying to consume the message taht was placed on to the topic test
//----------
//Batch: 1
//----------
//+------+-----+
//| value|count|
//+------+-----+
//|imtiaz|    1|
//|  name|    1|
//| there|    1|
//|    is|    1|
//| Hello|    1|
//|    my|    1|
//+------+-----+

//sending a second command we write ">There are wonderful people in this world" and the output look slike this:
//----------
//Batch: 1
//----------
//+---------+-----+
//|    value|count|
//+---------+-----+
//|   imtiaz|    1|
//|     name|    1|
//|    there|    1|
//|       in|    1|
//|       is|    1|
//|   people|    1|
//|    Hello|    1|
//|       my|    1|
//|    There|    1|
//|      are|    1|
//|    world|    1|
//|     this|    1|
//|wonderful|    1|	
//+---------+-----+
		
//remember we are using complete mode so it prints the ENTIRE table EVERY TIME. This sentence had all unique words so nothing incremental. If we send the same sentence again those words would increment by one.
		
	}
}
