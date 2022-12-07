package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLinesParser {
	
	  public void parseJsonLines() {
	    SparkSession spark = SparkSession.builder() //create spark session like before
	        .appName("JSON Lines to Dataframe")
	        .master("local")
	        .getOrCreate();
	    
	    Dataset<Row> df = spark.read().format("json") //now this time we are reading a JSON file instead of CSV
		        .load("src/main/resources/simple.json"); //location of the json file
//when looking at the data, each record is on a differnt line.
//{"name": "Top", "owns": [["car", "honda"], ["laptop", "Dell"]]}
//JSON line syntax where each entry is on ONE line. 
	    
	    df.show(5, 150); //5 records, 150 characters. We wil still see everything since this has 4 records. Will print like a table
	    df.printSchema();
	    
//CONSOLE OUTPUT - root shows the schema. It has a name and owns is an array of different elements. The actual output very simple output.
//Notice we have a multidimensional array. One array for an item inside an array of multiple items 
//	    +--------+----------------------------------+
//	    |    name|                              owns|
//	    +--------+----------------------------------+
//	    |     Top|    [[car, honda], [laptop, Dell]]|
//	    |   Frank|[[laptop, Macbook], [shoes, Nike]]|
//	    |   Peter|                                []|
//	    |Samantha|          [[home, 34 Morris Ave.]]|
//	    +--------+----------------------------------+
//
//	    root
//	     |-- name: string (nullable = true)
//	     |-- owns: array (nullable = true)
//	     |    |-- element: array (containsNull = true)
//	     |    |    |-- element: string (containsNull = true)

	 
//The more complex multiline JSON document
	    Dataset<Row> df2 = spark.read().format("json")
	    	.option("multiline", true) //Since we know this is multiline, set multiline to true.
	        .load("src/main/resources/multiline.json"); //parse the multiline JSON file instead.

//Below is ONE record in the multiline JSON
//	    {
//	        "id": "contract-11934",
//	        "buildingKey": "993839c8bh3fdgcc6734624ee8cc351050bn9shf93",
//	        "geo_location": {
//	          "type": "exact",
//	          "coordinates": [
//	            -78.8922549,
//	            36.0013755
//	          ]
//	        },
//	        "properties": {
//	          "permit_no": "110138",
//	          "lat_and_lon": [
//	            36.0013755,
//	            -78.8922549
//	          ],
//	          "address": "877 W CANAL ST",
//	          "year": "2009"
//	        },
//	        "timestamp": "2014-02-09T12:28:33-05:00"
//	      },
	 
	    df2.show(5, 150);
	    df2.printSchema();
	  }
	
	
}

//Console output for multiline JSON. The structure of schema does match. geo_location is another multidimensional array
//Also note how we are not getting field names in the record that is displayed below. This is normal way of Spark parsing our JSON document.
//What I mean by this is, in geo_location column we have two values and field names, type: exact, and coordinates: -78.89... Field names NOT displayed in output
//+------------------------------------------+----------------------------------+--------------+---------------------------------------------------------+-------------------------+
//|                               buildingKey|                      geo_location|            id|                                               properties|                timestamp|
//+------------------------------------------+----------------------------------+--------------+---------------------------------------------------------+-------------------------+
//|993839c8bh3fdgcc6734624ee8cc351050bn9shf93|[[-78.8922549, 36.0013755], exact]|contract-11934|[877 W CANAL ST, [36.0013755, -78.8922549], 110138, 2009]|2014-02-09T12:28:33-05:00|
//|     8fdn8rh3fdgcc6734624ee89wn350bn9shf93|[[-87.9872323, 36.0013755], exact]|contract-11984|  [923 YETTI ST, [36.0013755, -78.8922549], 110138, 2004]|2014-02-09T12:28:33-05:00|
//+------------------------------------------+----------------------------------+--------------+---------------------------------------------------------+-------------------------+
//
//root
// |-- buildingKey: string (nullable = true)
// |-- geo_location: struct (nullable = true)
// |    |-- coordinates: array (nullable = true)
// |    |    |-- element: double (containsNull = true)
// |    |-- type: string (nullable = true)
// |-- id: string (nullable = true)
// |-- properties: struct (nullable = true)
// |    |-- address: string (nullable = true)
// |    |-- lat_and_lon: array (nullable = true)
// |    |    |-- element: double (containsNull = true)
// |    |-- permit_no: string (nullable = true)
// |    |-- year: string (nullable = true)
// |-- timestamp: string (nullable = true)

