package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application2 { //MY NOTES CODE ALONG

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()//build our spark app
				.appName("Combine two datasets")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> durhamDf = buildDurhamParksDataFrame(spark); //pass the JSON to build a dataframe for durham parks
		durhamDf.printSchema(); //method to just simply display the schema of this DF. SHows structure of document
		durhamDf.show(10); //display 10 records
//OUTPUT of print schema
//		root
//		 |-- datasetid: string (nullable = true)
//		 |-- fields: struct (nullable = true)
//		 |    |-- acres: double (nullable = true)
//		 |    |-- address: string (nullable = true)
//		 |    |-- adlt_baseb: string (nullable = true)
//		 |    |-- adlt_softb: string (nullable = true)
//		 |    |-- athl_field: string (nullable = true)
//		 |    |-- basket_cnt: double (nullable = true)
//		 |    |-- basketball: string (nullable = true)
//		 |    |-- boating: string (nullable = true)
//		 |    |-- camping: string (nullable = true)
//		 |    |-- center_siz: string (nullable = true)
//		 |    |-- cw_id: string (nullable = true)
//		 |    |-- disc_golf: string (nullable = true)
//		 |    |-- district: string (nullable = true)
//		 |    |-- dog_park: string (nullable = true)
//		 |    |-- fishing: string (nullable = true)
//		 |    |-- geo_point_2d: array (nullable = true)
//		 |    |    |-- element: double (containsNull = true)
//		 |    |-- geo_shape: struct (nullable = true)
//		 |    |    |-- coordinates: array (nullable = true)
//		 |    |    |    |-- element: array (containsNull = true)
//		 |    |    |    |    |-- element: array (containsNull = true)
//		 |    |    |    |    |    |-- element: string (containsNull = true)
//		 |    |    |-- type: string (nullable = true)
//		 |    |-- globalid: string (nullable = true)
//		 |    |-- greenway: string (nullable = true)
//		 |    |-- grill: string (nullable = true)
//		 |    |-- grill_cnt: double (nullable = true)
//		 |    |-- lights: string (nullable = true)
//		 |    |-- location: string (nullable = true)
//		 |    |-- name: string (nullable = true)
//		 |    |-- notes: string (nullable = true)
//		 |    |-- objectid: long (nullable = true)
//		 |    |-- park_name: string (nullable = true)
//		 |    |-- picnic_tab: string (nullable = true)
//		 |    |-- picture: string (nullable = true)
//		 |    |-- playground: string (nullable = true)
//		 |    |-- rec_center: string (nullable = true)
//		 |    |-- shape_leng: double (nullable = true)
//		 |    |-- shape_starea: double (nullable = true)
//		 |    |-- shape_stlength: double (nullable = true)
//		 |    |-- shelter: string (nullable = true)
//		 |    |-- shelter_si: string (nullable = true)
//		 |    |-- spray_grnd: string (nullable = true)
//		 |    |-- table_cnt: double (nullable = true)
//		 |    |-- tennis_cnt: double (nullable = true)
//		 |    |-- tennis_crt: string (nullable = true)
//		 |    |-- water: string (nullable = true)
//		 |    |-- yth_baseb: string (nullable = true)
//		 |    |-- zip: double (nullable = true)
//		 |-- geometry: struct (nullable = true)
//		 |    |-- coordinates: array (nullable = true)
//		 |    |    |-- element: double (containsNull = true)
//		 |    |-- type: string (nullable = true)
//		 |-- record_timestamp: string (nullable = true)
//		 |-- recordid: string (nullable = true)

//OUTPUT of .show()
//		+----------+--------------------+--------------------+--------------------+--------------------+
//		| datasetid|              fields|            geometry|    record_timestamp|            recordid|
//		+----------+--------------------+--------------------+--------------------+--------------------+
//		|city-parks|[11.52028555, 110...|[[-78.88710172937...|2018-09-01T23:00:...|2ff6903baee78eca1...|
//		|city-parks|[12.25698762, 231...|[[-78.92181708385...|2018-09-01T23:00:...|de1a76509b5ab8adb...|
//		|city-parks|[101.74644709, 35...|[[-78.96868492509...|2018-09-01T23:00:...|1b08024879de55d7e...|
//		|city-parks|[11.97479141, 110...|[[-78.95585544233...|2018-09-01T23:00:...|dccd5b3f047428bd6...|
//		|city-parks|[0.43687249, 1100...|[[-78.88244500505...|2018-09-01T23:00:...|89dd42fdbf7ca5b6e...|
//		|city-parks|[20.08746668, 283...|[[-78.95247102687...|2018-09-01T23:00:...|0e33e274de76d8096...|
//		|city-parks|[14.62743857, 280...|[[-78.86574128098...|2018-09-01T23:00:...|5296f611cd5a85ae5...|
//		|city-parks|[7.7665654, 1500 ...|[[-78.87163711684...|2018-09-01T23:00:...|49c5073b72e3fec73...|
//		|city-parks|[1.82773177, 1900...|[[-78.92170094018...|2018-09-01T23:00:...|cc5c46c928f833bb2...|
//		|city-parks|[0.54514956999999...|[[-78.92189381037...|2018-09-01T23:00:...|a250f3ff1a608a2dd...|
//		+----------+--------------------+--------------------+--------------------+--------------------+
//		only showing top 10 rows
		
		
//		Dataset<Row> philDf = buildPhilParksDataFrame(spark); //same thing, returns the dataframe

//		combineDataFrames(philDf, durhamDf); //combine the dataframe methods. WIll come back to.
	}
	
//STEP 1
//dataset row is a dataframe.s
//CTRL SHIFT O to import the dataset row from SQL
	public static Dataset<Row> buildDurhamParksDataFrame(SparkSession spark) { //expects to return a dataframe and input is the spark session
		
		Dataset<Row> df = // NOW, make sure that we fit this whole spark read INTO A DF
			spark.read() //using spark to read a file
			.format("json") //read a JSON file
			.option("multiline", true) //we went over in previous lecture. We are saying that in the JSON file we can have one record span multiple lines.
			.load("src/main/resources/durham-parks.json"); //specify location. Since local file we just put file path
		
		return df; //test
		
	}
}
