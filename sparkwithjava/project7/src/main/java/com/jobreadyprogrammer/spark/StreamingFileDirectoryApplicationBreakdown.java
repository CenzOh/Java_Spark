// lesson 23 my code
package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*; //used for average function


public class StreamingFileDirectoryApplicationBreakdown {
	
//one of the CSVS is NASDAQ100, one col is date, other col is value/price. A lot of files have the similar setup. We can also create a folder and call itincoming stock files
	public static void main(String[] args) throws StreamingQueryException {
		
		SparkSession spark = SparkSession.builder()
				.appName("StreamingFileDirectoryWordCount")
				.master("local")
				.getOrCreate();
		
//read ALL csv files written automatically within a directory. Schema MUSt match format of file
		StructType userSchema = new StructType().add("date", "string").add("value", "float"); //we define the schema of having a date col with type string, and a value col with type floating point num (12.14)
		
		Dataset<Row> stockData = spark
				.readStream()
				.option("sep", ",") //CSV has cols separated with the commas
				.schema(userSchema) //specifying schmea of csv files
				.csv("C:\\Users\\Cenzo\\JavaSpark\\workspace\\sparkwithjava\\project7\\data\\incomingStockFiles"); // format("csv").load("/path/to/directory"). This is name of folder
//we'll take one file at a time and put it in the folder and see what happens
		
		
		Dataset<Row> resultDf = stockData = stockData.groupBy("date").agg(avg(stockData.col("value"))); //getting the average of the value column
		
		StreamingQuery query = resultDf.writeStream()
				.outputMode("complete")
				.format("console")
				.start();
		
		query.awaitTermination();

//after we hit play button, we see the JSOn logging. So we are ready to go. You can see the stream sources directory in the incoming files folder. We specified this.
//we take one file at a time and move it into the incoming stock files folder. The console starts outputting, we can see the average values based ont he date, for instance:
//+----------+----------+
//|      date|avg(value)|
//+----------+----------+
//|1993-02-11| 366.32998|
//|1986-09-03|149.820007|
//...
//Awesome! it works. COntinues to log in json while it waits for next file. Now we give another file and input it into the folder again. Since we have output mode as complete, we will see all dates even if not changed
//although, we will see the averages change over time with more data we input into the system.
		
	
		
	}
}