//lesson 22 my code
package com.jobreadyprogrammer.spark;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamingSocketApplicationBreakdown {

	public static void main(String[] args) throws StreamingQueryException {
		
		
		//first thing, start socket connection at 9999 use `nc -lk 9999` on your terminal. any info you feed to it can send info to whichever app is reading that port
		SparkSession spark = SparkSession.builder() //connection to unbounded DF
				.appName("StreamingSocketWordCount")
				.master("local")
				.getOrCreate();
		
		//next create the DF representing stream of input lines from localhost:9999 connection.
		Dataset<Row> lines = spark //lines is our unbounded DF
				.readStream()
				.format("socket")
				.option("host", "localhost")
				.option("port", 9999) //listen on this port. Every line typed in the console gets sent to this DF lines
				.load();
		
		Dataset<String> words = lines
				.as(Encoders.STRING())
				.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
//with flatmpa, we split the long sentence into a bunch of words that will be added to this data set. split based on whitespace, specify this in splot() fcn
		
		
		Dataset<Row> wordCounts = words.groupBy("value").count();
		
		StreamingQuery query = wordCounts.writeStream() //instead of df.show, we are writing a stream. writeStream() //output is a query of type query. output mode is complete, others are update and append. SInce we are doing aggregation, we can only do update or complete
				.outputMode("complete") //("update")
				.format("console") 
				.start();
		
		query.awaitTermination(); //if we do not have this, the program will terminate immediately. 
//should see json strings being printed that means everything is running successfully. In the console the instructor wrote "hello there my name is imtiaz" and in the output we can see:
//+------+-----+
//| value|count|
//+------+-----+
//|imtiaz|    1|
//|  name|    1|
//| there|    1|
//| hello|    1|
//|    is|    1|
//|    my|    1|
//+------+-----+
//so the program does this count and groupby and gives us this output dataframe called wordCounts (we specified this earlier in code, before the streamingQuery section) We streamed to console, we can stream to other outputs
//like kafka, or HDFS processing, or any SQL compatabile way. Also every TEN seconds this will listen to our application and present ANOTHER output! Ouputting info on an interval
//instructor types in "hello there my name is imtiaz" again and now in the eclipse console the spark code gets run on the new stream and all the counts increase by 1. 
//+------+-----+
//| value|count|
//+------+-----+
//|imtiaz|    2|
//|  name|    2|
//| there|    2|
//| hello|    2|
//|    is|    2|
//|    my|    2|
//+------+-----+
//next up he changes it up writing "hello there my name is taz". again, spark code gets run and in eclipse output we get
//+------+-----+
//| value|count|
//+------+-----+
//|imtiaz|    2|
//|  name|    3|
//| there|    3|
//| hello|    3|
//|    is|    3|
//|    my|    3|
//|   taz|    1|
//+------+-----+
//now he terminates the app. ANd now he changes the outputMode to update instead of compelete. Does something similar when running program. first writes "hello there my name is imtiaz"
//we get same value count. Then writes "hello there my name is taz", everything in count is 2 except for taz at 1. Imtiaz is missing, the ouput only showed WHAT was being updated. Imtiaz was NOT being updated, not displayed
//third example he types just bananas, output ONLY shows bananas. Again, because we are in UPDATE mode we ONLY get the values that were updated. Nothing was removed or nothing was changed. 
//since we are in aggreagete we cant change output mode to append. If we try to run, we get error saying append mode not supported when streaming aggregations. Watermarks, have to do with time sensitivity.
//this was an example of spark streaming with a socket!
	}
}
