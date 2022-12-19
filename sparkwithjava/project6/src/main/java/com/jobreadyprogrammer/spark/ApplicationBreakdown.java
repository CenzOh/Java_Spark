//Lesson 19 my code
package com.jobreadyprogrammer.spark;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class ApplicationBreakdown {
	
	public static void main(String[] args) {
	
		SparkSession spark = SparkSession.builder()
				.appName("Learning Spark SQL Dataframe API")
				.master("local") //remove if using on AWS system. Master will be setup by spark on AWS
				.getOrCreate();
		
		String redditFile = "C:\\Users\\Cenzo\\JavaSpark\\workspace\\sparkwithjava\\project6\\RC_2007-12"; //as of now, file is not being able to be read?
//		String redditFile = "C:\\Users\\Cenzo\\JavaSpark\\workspace\\sparkwithjava\\project6\\RC_2011-12"; //large version
//		String redditFile = "S3n://spark-course-bucket/RC-2011-12"; //AWS bucket 

		
			Dataset<Row> redditDf = spark.read().format("json")
					.option("inferSchema", "true") //use string version of true
					.option("header", true)
					.load(redditFile);
			
			redditDf.show(10); //testing to see if i can read the file
//**** What output supposed to look like
//+---------+----------------------+-----------------+--------------------+----------------+-----------+-------------+------+
//|   author|author_flair_css_class|author_flair_text|                body|controversiality|created_utc|distinguished|edited|
//+---------+----------------------+-----------------+--------------------+----------------+-----------+-------------+------+
//| postullo|                  null|             null|TO VOICER SAMUEL ...|               0|  119116811|         null| false|

//lets focus on the body column, it contains all text someone wrote in comment. Lets find most popular set of words used during a given dataframe? 
			
			redditDf = redditDf.select("body"); //new DF will ONLY contain the comments. Lets extract each word from the comment and tabulate occurences of each word.
			Dataset<String> wordsDs = redditDf.flatMap((FlatMapFunction<Row, String>)
					r -> Arrays.asList(r.toString().replace("\n", "").replace("\r", "").trim().toLowerCase()
							.split(" ")).iterator(), //split based on whitespace, what returns the array
					Encoders.STRING()); //we did flatmpa in project 4 remember?

//for the flatMapFunction, take a row from a dataframe and CONVERT it into a string. the replace method replaces occurences of newlines. we also use trim to remove leading and trailing whitespace and make everyword lowercase with toLowerCase() method
//convert the dataset to a DF!!!
			Dataset<Row> wordsDf = wordsDs.toDF();

//look at word utils now, its an array of strings (and is static). Has a string variable called stop words. We dont want to work with the boring stuff so this stop words will filter out the boring stop words
			Dataset<Row> boringWordsDf = spark.createDataset(Arrays.asList(WordUtils.stopWords), Encoders.STRING()).toDF(); //toDf turns this DS to a DF. use spark.createDataset to creat the dataset out of the array as a list from words utils
			
//exclude boring words below. To find most common words we have to use an aggregate on the words in this words df.
//			wordsDf = wordsDf.except(boringWordsDf); //this will subtract all boring words in words dataframe and return data point. Issue with except is that it gets rid of duplicates
			wordsDf = wordsDf.join(boringWordsDf, wordsDf.col("value").equalTo(boringWordsDf.col("value")), "leftanti");//join approach similar to sql / database backlground. 
//left anti join gives us subtraction w/o removing duplicates. gives us all words in words df that do NOT exist in boring words
			
			wordsDf = wordsDf.groupBy("value").count(); //count occurances of each word. 
			wordsDf.orderBy(desc("count")).show(); //Top most used, sort in desc fashion. desc() comes from import static org.apache.spark.sql.functions.*;
			
//*** what output would be 
//+----------+-----+
//|     value|count|
//+----------+-----+
//|government| 6271|
//|        --| 3725|
//|      bush| 3634|
//|    aren't| 3474|
//|  american| 3406|
//|     means| 3403|
			
//pretty interesting words like government, bush, and american. Our data also has swear words so we can filter that out as well and add them to the word utils array.
//lots of political words during this timeframe of december 2007 when president bush was running with his campaign. 

//next up, running the large file from 2011, this file has 3 GBs worth of JSON strings. Lets also run it on the cloud
//in the lecture it takes about 5 minutes to compile the large file locally! 
//can go to `localhost:4040` to see progrerss of job and see what happened. Ingestion -> mapping -> mapping partitions. Input size 7 GBs, 58 tasks / threads. This file was divided by 58 partitions. 
//Split up to work with 128 MBs of data. 200K records in each partition. 
			
//*** What output would be
//+----------+-----+
//|     value|count|
//+----------+-----+
//|		 F***|96795|
//|      S***|90922|
//|      ****|86533|
//|       [if|85985|
//|     [it's|85430|
//|   looking|82000|
//|      post|79823|
			
//wow two swear words were most commonly used in the year 2011!
//lets find out if running on EMR (elastic map reduce) on amazon would be faster
//next up you would jar the program and ensure the pom.xml file has same structure such as `com.jobreadyprogrammer.spark`
//run as > maven clean. right click project > run as > maven install > jar file will get created here and gets saved in Target folder
			
	}
}