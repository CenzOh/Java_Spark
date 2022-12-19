package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamingSocketApplicationBreakdown {

	public static void main(String[] args) throws StreamingQueryException {
		
		
		//first thing, start socket connection at 9999 use `nc -lk 9999` on your terminal. any info you feed to it can send info to whichever app is reading that port
		SparkSession spark = SparkSession.builder() //connection to unbounded DF
				.appName("StreamingSocketWordCount")
				.master("local")
				.getOrCreate();
		
		//next create the DF representing stream of input lines from localhost:9999 connection.
		Dataset<Row> lines = spark
				.readStream()
				.format("socket")
				.option("host", "localhost")
				.option("port", 9999)
				.load();
		
		Dataset<String> words = lines
				.as(Encoders.STIRNG())
				.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
		
		Dataset<Row> wordCOunts = words.groupBy("value").count();
		
		StreamingQuery query = wordCounts.writeStream()
				.outputMode("complete")
				.format("complete")
				.srtat();
	}
}
