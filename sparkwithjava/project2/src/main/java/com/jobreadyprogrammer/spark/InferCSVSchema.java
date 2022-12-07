package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {
//our first example we will look at.
	
	public void printSchema() {
		SparkSession spark = SparkSession.builder() //first we create the spark session
		        .appName("Complex CSV to Dataframe") 
		        .master("local")
		        .getOrCreate();
		 
		    Dataset<Row> df = spark.read().format("csv") //read the format of a CSV file / expecting a CSV file. Dataset with Rows is a DataFrame!
		        .option("header", "true") //Our file has a header row that defines what each column name should be.
		        .option("multiline", true) //the csv (title field) goes onto multiple lines / one record can take up multiple lines
		        .option("sep", ";") //what is the separator? Usually it is a , but in the file its a ;, specify this here
		        .option("quote", "^") //whenever we see a ^, we want it to be interpreted as text/quote. Ex - ^This is a title^ interpret as "This is a title"
		        .option("dateFormat", "M/d/y") //Telling spark there will be month/day/year format. IT will look for this format
		        .option("inferSchema", true) //This means we do not hard code the schema and we tell spark to figure out what the data type is (string, int, date)
		        .load("src/main/resources/amazonProducts.txt"); //location of the file. Our data set has 5 columns
		 
		    System.out.println("Excerpt of the dataframe content:"); //simple print statement
//		    df.show(7); //display 7 rows
		    df.show(7, 90); // truncate after 90 chars
		    System.out.println("Dataframe's schema:");
		    df.printSchema(); //see how spark interpreted the columns. What data types did spark think the values have?
	}
	
}

//CONSOLE OUPUT of the dataframe schema. Looks like it took listing ID to be an integer. Thinks published date is a string. Being printed from df.printSchema()
//
//Dataframe's schema:
//root
// |-- listingId: integer (nullable = true)
// |-- productId: integer (nullable = true)
// |-- title: string (nullable = true)
// |-- publishDate: string (nullable = true)
// |-- url: string (nullable = true)
//
//The df.show(7) says to display 4 columns. The second param with 90 will truncate after 90 characters (default shows less chars). We have 4 records so we will print all records
//
//+---------+---------+------------------------------------------------------------------------------------------+-----------+---------------------+
//|listingId|productId|                                                                                     title|publishDate|                  url|
//+---------+---------+------------------------------------------------------------------------------------------+-----------+---------------------+
//|        1|       12|TP-Link AC750 Dual Band WiFi Range Extender, Repeater, Access Point w/Mini Housing  Des...|   09/10/17|http://a.co/d/3ivKXxI|
//|        2|       13| PHICOMM K3C AC 1900 MU-MIMO Dual Band Wi-Fi Gigabit Router – Powered by Intel  Technology|   11/15/18|http://a.co/d/eKdbr5P|
//|        3|       17|Tuft & Needle Queen Mattress, Bed in a Box, T&N Adaptive Foam, Sleeps Cooler with  More...|   08/22/16|http://a.co/d/efLN6jt|
//|        4|       22|     Queen Size SafeRest Premium Hypoallergenic Waterproof Mattress Protector - Vinyl Free|   08/22/16|http://a.co/d/fcgIGcG|
//+---------+---------+------------------------------------------------------------------------------------------+-----------+---------------------+
