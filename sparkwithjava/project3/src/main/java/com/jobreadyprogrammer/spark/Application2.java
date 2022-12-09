//LESSON 10
package com.jobreadyprogrammer.spark;

import static org.apache.spark.sql.functions.*; //all fcns available to us such as concat and lit

import org.apache.spark.Partition;
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
//		durhamDf.printSchema(); //method to just simply display the schema of this DF. SHows structure of document
//		durhamDf.show(10); //display 10 records

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
		
		
		Dataset<Row> philDf = buildPhilParksDataFrame(spark); //same thing, returns the dataframe. Uncommented in lecture 11
//		philDf.printSchema(); //print the schema.
//		philDf.show(10); //print 300 records, not dealing with big data so 300 records not an issue. Reverted to 10 record to print now

		
//		combineDataFrames(philDf, durhamDf); //combine the dataframe methods. Working once done with Philly DF. input the phillyDF and durhamDF
//IMPORTANMT NOTE - When we do this fcn ^ it takes the philly DF (first input) and DUMPS it ONTO the durham df (second input)
//if switched around, durham data will go on TOP of phully (or philly data goes UNDER durham) SO the col order will change
		combineDataFrames(durhamDf, philDf);
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
		
		df = df.withColumn("park_id", concat(df.col("datasetid"), lit("_"),
				df.col("fields.objectid"), lit("_Durham")))//;
//withCol() is used to create a new column. WE create a new col called park_id which is a unique identifier to identify each UNIQUE park
//we will create this new column with concat(), we will concat some values. First we grab the datasetid (ex is `city-parks`)
//next we put a lit() (literal) underscore afterthat. And then we grab the column fields.objectid (remember that fields has a ton of sub elements from schema)
//example of a field.objectid (can see in the dataset) is '7'. and now we add another literal text string after the objectId and write _durham.

//LIT IS NOT BEING RECOGNIZED - coming from spark.apache.sql. Provide the static import for it.
//Example of our parid_id column will display: `city-parks_7_Durham`
//Take note, we are NOT changing the df here, remember that we CANT change the df because they are immutable. We can create NEW ones. 
//When we assign it to itself it creates a new one. If you run again, you will see the park id column, I will display results below of the NEW dataframe:

//		+----------+--------------------+--------------------+--------------------+--------------------+--------------------+
//		| datasetid|              fields|            geometry|    record_timestamp|            recordid|             park_id|
//		+----------+--------------------+--------------------+--------------------+--------------------+--------------------+
//		|city-parks|[11.52028555, 110...|[[-78.88710172937...|2018-09-01T23:00:...|2ff6903baee78eca1...| city-parks_7_Durham|
//		|city-parks|[12.25698762, 231...|[[-78.92181708385...|2018-09-01T23:00:...|de1a76509b5ab8adb...|city-parks_35_Durham|
//		|city-parks|[101.74644709, 35...|[[-78.96868492509...|2018-09-01T23:00:...|1b08024879de55d7e...|city-parks_37_Durham|
//		|city-parks|[11.97479141, 110...|[[-78.95585544233...|2018-09-01T23:00:...|dccd5b3f047428bd6...|city-parks_27_Durham|
//		|city-parks|[0.43687249, 1100...|[[-78.88244500505...|2018-09-01T23:00:...|89dd42fdbf7ca5b6e...|city-parks_13_Durham|
//		|city-parks|[20.08746668, 283...|[[-78.95247102687...|2018-09-01T23:00:...|0e33e274de76d8096...|city-parks_67_Durham|
//		|city-parks|[14.62743857, 280...|[[-78.86574128098...|2018-09-01T23:00:...|5296f611cd5a85ae5...|city-parks_74_Durham|
//		|city-parks|[7.7665654, 1500 ...|[[-78.87163711684...|2018-09-01T23:00:...|49c5073b72e3fec73...|city-parks_50_Durham|
//		|city-parks|[1.82773177, 1900...|[[-78.92170094018...|2018-09-01T23:00:...|cc5c46c928f833bb2...|city-parks_45_Durham|
//		|city-parks|[0.54514956999999...|[[-78.92189381037...|2018-09-01T23:00:...|a250f3ff1a608a2dd...|city-parks_51_Durham|
//		+----------+--------------------+--------------------+--------------------+--------------------+--------------------+
//		only showing top 10 rows

//We will create ANOTHER new column called park name. Comes from fields.park_name
				.withColumn("park_name", df.col("fields.park_name"))

//next we create a city column. This doesnt exist yet / did not exist in the original. So 
				.withColumn("city", lit("Durham"))
				
// ADDED in lecture 11, keeping both DFs organized. address exists in the fields field
				.withColumn("address", df.col("fields.address"))
		
//next column will show if the park has a playground. COntains data coming from the playgrounds field (this exists inside the fields field haha)
				.withColumn("has_playground", df.col("fields.playground"))
				
//next column will display the zipcode, again comes from the fields field. Name of col in there is zip.
				.withColumn("zipcode", df.col("fields.zip"))
				
//now number of acres we will pull from fields field
				.withColumn("land_in_acres", df.col("fields.acres"))
				
//Next we will get the X coordinates. THis is inside geometry.coordinates field. Note that this is an array so we use .getItem to select the index. X coord is index 0
				.withColumn("geoX", df.col("geometry.coordinates").getItem(0))
				
//same idea with the y coordinate just getItem on index 1 this time.
				.withColumn("geoY", df.col("geometry.coordinates").getItem(1))
		
//REMINDER - dataframes are IMMUDTABLE so we have to assign all of this to the dataframe itself. WIll not modify original but create a new one.

//OUTPUT After new columns
//+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+-------+------------------+------------------+------------------+
//| datasetid|              fields|            geometry|    record_timestamp|            recordid|             park_id|           park_name|  city|has_playground|zipcode|     land_in_acres|              geoX|              geoY|
//+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+-------+------------------+------------------+------------------+
//|city-parks|[11.52028555, 110...|[[-78.88710172937...|2018-09-01T23:00:...|2ff6903baee78eca1...| city-parks_7_Durham|         BURTON PARK|Durham|             Y|27701.0|       11.52028555|  -78.887101729377| 35.97782539263909|
//|city-parks|[12.25698762, 231...|[[-78.92181708385...|2018-09-01T23:00:...|de1a76509b5ab8adb...|city-parks_35_Durham|       ROCKWOOD PARK|Durham|             Y|27707.0|       12.25698762| -78.9218170838534|35.972950743249214|
//|city-parks|[101.74644709, 35...|[[-78.96868492509...|2018-09-01T23:00:...|1b08024879de55d7e...|city-parks_37_Durham|    SANDY CREEK PARK|Durham|             N|27707.0|      101.74644709|-78.96868492509134|35.969520579760456|
//|city-parks|[11.97479141, 110...|[[-78.95585544233...|2018-09-01T23:00:...|dccd5b3f047428bd6...|city-parks_27_Durham|  MORREENE ROAD PARK|Durham|             Y|27705.0|       11.97479141|-78.95585544233714| 36.00703991938609|
//|city-parks|[0.43687249, 1100...|[[-78.88244500505...|2018-09-01T23:00:...|89dd42fdbf7ca5b6e...|city-parks_13_Durham|    DREW/GRANBY PARK|Durham|             Y|27701.0|        0.43687249|-78.88244500505519|36.001372698873865|
//|city-parks|[20.08746668, 283...|[[-78.95247102687...|2018-09-01T23:00:...|0e33e274de76d8096...|city-parks_67_Durham|CORNWALLIS ROAD PARK|Durham|             Y|27705.0|       20.08746668|-78.95247102687885| 35.97845591554436|
//|city-parks|[14.62743857, 280...|[[-78.86574128098...|2018-09-01T23:00:...|5296f611cd5a85ae5...|city-parks_74_Durham|   SPRUCE PINE LODGE|Durham|             Y|27503.0|       14.62743857| -78.8657412809851| 36.17446566800999|
//|city-parks|[7.7665654, 1500 ...|[[-78.87163711684...|2018-09-01T23:00:...|49c5073b72e3fec73...|city-parks_50_Durham|LITTLE RIVER FISH...|Durham|             N|27712.0|         7.7665654|-78.87163711684084| 36.12714426429574|
//|city-parks|[1.82773177, 1900...|[[-78.92170094018...|2018-09-01T23:00:...|cc5c46c928f833bb2...|city-parks_45_Durham|       WESTOVER PARK|Durham|             Y|27705.0|        1.82773177|-78.92170094018387| 36.02510393190939|
//|city-parks|[0.54514956999999...|[[-78.92189381037...|2018-09-01T23:00:...|a250f3ff1a608a2dd...|city-parks_51_Durham|      MAPLEWOOD PARK|Durham|             Y|27701.0|0.5451495699999991| -78.9218938103797| 35.99381294353847|
//+----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+-------+------------------+------------------+------------------+
//only showing top 10 rows

//Now, what we want to see ONLY the new fields? We have a method called .drop. Specify the column. Two ways to do this. df.col(name) or give col name as a "string"
				.drop("fields").drop(df.col("geometry")).drop("record_timestamp")
				.drop("recordid").drop("datasetid");

//We will now see the columns that we have defined and added to our dataframe and remove the older columns we don't want to see. We picked and chose!
//What if we droped BEFORE we added the .withCOlumns? Well, in some of the .withCOlumns, we are trying to access the columns
//So if you drop the col, and then you try to access it, it will not work since it is referencing a col we don't have access to.
	
//Output after drop
//+--------------------+--------------------+------+--------------+-------+------------------+------------------+------------------+
//|             park_id|           park_name|  city|has_playground|zipcode|     land_in_acres|              geoX|              geoY|
//+--------------------+--------------------+------+--------------+-------+------------------+------------------+------------------+
//| city-parks_7_Durham|         BURTON PARK|Durham|             Y|27701.0|       11.52028555|  -78.887101729377| 35.97782539263909|
//|city-parks_35_Durham|       ROCKWOOD PARK|Durham|             Y|27707.0|       12.25698762| -78.9218170838534|35.972950743249214|
//|city-parks_37_Durham|    SANDY CREEK PARK|Durham|             N|27707.0|      101.74644709|-78.96868492509134|35.969520579760456|
//|city-parks_27_Durham|  MORREENE ROAD PARK|Durham|             Y|27705.0|       11.97479141|-78.95585544233714| 36.00703991938609|
//|city-parks_13_Durham|    DREW/GRANBY PARK|Durham|             Y|27701.0|        0.43687249|-78.88244500505519|36.001372698873865|
//|city-parks_67_Durham|CORNWALLIS ROAD PARK|Durham|             Y|27705.0|       20.08746668|-78.95247102687885| 35.97845591554436|
//|city-parks_74_Durham|   SPRUCE PINE LODGE|Durham|             Y|27503.0|       14.62743857| -78.8657412809851| 36.17446566800999|
//|city-parks_50_Durham|LITTLE RIVER FISH...|Durham|             N|27712.0|         7.7665654|-78.87163711684084| 36.12714426429574|
//|city-parks_45_Durham|       WESTOVER PARK|Durham|             Y|27705.0|        1.82773177|-78.92170094018387| 36.02510393190939|
//|city-parks_51_Durham|      MAPLEWOOD PARK|Durham|             Y|27701.0|0.5451495699999991| -78.9218938103797| 35.99381294353847|
//+--------------------+--------------------+------+--------------+-------+------------------+------------------+------------------+
		
		return df; //test / print the df to console
		
	}
	
// *** LESSON 11 ***
// STEP 2
//WE are done extracting data from the durham json file. we will now extract from philly csv which is a bit different to do
			
	private static Dataset<Row> buildPhilParksDataFrame(SparkSession spark) { //created this method in lesson 11/ method definition pretty similar

//code is similar so I'll copy the first line from the other function
		Dataset<Row> df = spark.read()
				.format("csv") //read in format of csv, not a json file now with philly
				.option("header", true) //this csv has headers, lets specify that!
				.load("src/main/resources/philadelphia_recreations.csv"); //same thing, read the philly data this time

//remember that this csv has more than just parks. we are interested in parks so we will filter out to get ONLY parks
//remember to self assign the DF.
// another note, we cant assume that there is just capital Parks, so lets lowercase all the rows here so we KNOW it will only be park. park != Park remember that
		df = df.filter(lower(df.col("USE_")).like("%park%")); //the col that determines if its a park is called USE_. We want the row to say park to keep it. THis is one way to filter
		df = df.filter("lower(USE_) like '%park%' "); //second way to do this filter. Using SQL syntax 
		
// SCHEMA:
//root
// |-- OBJECTID: string (nullable = true)
// |-- ASSET_NAME: string (nullable = true)
// |-- SITE_NAME: string (nullable = true)
// |-- CHILD_OF: string (nullable = true)
// |-- ADDRESS: string (nullable = true)
// |-- TYPE: string (nullable = true)
// |-- USE_: string (nullable = true)
// |-- DESCRIPTION: string (nullable = true)
// |-- SQ_FEET: string (nullable = true)
// |-- ACREAGE: string (nullable = true)
// |-- ZIPCODE: string (nullable = true)
// |-- ALLIAS: string (nullable = true)
// |-- CHRONOLOGY: string (nullable = true)
// |-- NOTES: string (nullable = true)
// |-- DATE_EDITED: string (nullable = true)
// |-- EDITED_BY: string (nullable = true)
// |-- OCCUPANT: string (nullable = true)
// |-- TENANT: string (nullable = true)
// |-- LABEL: string (nullable = true)
	
// PRINTING THE RECORDS
//+--------+--------------------+--------------------+--------------------+--------------------+----+--------------------+------------------+----------------+-------------+-------+--------------------+----------+--------------------+-----------+--------------+--------------+--------------------+--------------------+
//|OBJECTID|          ASSET_NAME|           SITE_NAME|            CHILD_OF|             ADDRESS|TYPE|                USE_|       DESCRIPTION|         SQ_FEET|      ACREAGE|ZIPCODE|              ALLIAS|CHRONOLOGY|               NOTES|DATE_EDITED|     EDITED_BY|      OCCUPANT|              TENANT|               LABEL|
//+--------+--------------------+--------------------+--------------------+--------------------+----+--------------------+------------------+----------------+-------------+-------+--------------------+----------+--------------------+-----------+--------------+--------------+--------------------+--------------------+
//|       2|    Schuylkill Banks|    Schuylkill Banks|    Schuylkill Banks|   400-16 S TANEY ST|Land|Park- Linear\Parkway|              null| 658060.84696869|  15.10705875|  19103|                null|      null|                null|  11Feb2015|Nora Dougherty|           PPR|                null|    Schuylkill Banks|
//|       4|      Peter's Island|      Peter's Island|      Peter's Island|                null|Land|Park- Regional/Wa...|Regional\Watershed|  83679.29305106|   1.92101992|   null|                null|      null|                null|  22Aug2014|Nora Dougherty|           PPR|                null|      Peter's Island|
//|       5|Frankford Arsenal...|Frankford Arsenal...|Frankford Arsenal...|  5625 Tacony Street|Land|  Park- Neighborhood|              null| 477860.28847784|  10.97020661|   null|    PA Fish and Boat|      2014|                null|  22Aug2014|Nora Dougherty|           PPR|                null|Frankford Arsenal...|
//|       6|Orthodox Street P...|Orthodox Street P...|Orthodox Street P...|3101 Orthodox Street|Land|  Park- Neighborhood|              null| 457597.04880592|  10.50502478|   null|                null|      2014|                null|  22Aug2014|Nora Dougherty|          null|                null|            Orthodox|
//|       7|Wissahickon Valle...|Wissahickon Valle...|Wissahickon Valle...|                    |Land|Park- Regional/Wa...|                  |88949112.8413902|2043.81972119|  19128|                    |      1867|                    |  12Nov2013|Vanessa Miller|           PPR|                null|Wissahickon Valle...|
//|       8| West Fairmount Park| West Fairmount Park| West Fairmount Park|                    |Land|Park- Regional/Wa...|                  |61124064.2157099|1403.22104483|  19131|                    |      1866|                    |  12Nov2013|Vanessa Miller|           PPR|                null| West Fairmount Park|
//|       9|Pennypack Creek Park|Pennypack Creek Park|Pennypack Creek Park|                    |Land|Park- Regional/Wa...|                  |58572936.1393227|1344.65496859|  19152|                    | 1905-1929|                    |  12Nov2013|Vanessa Miller|           PPR|                null|Pennypack Creek Park|
//|      10|    Cobbs Creek Park|    Cobbs Creek Park|    Cobbs Creek Park|                    |Land|Park- Regional/Wa...|                  |37055823.2343341|    850.68805|  19143|                    | 1904-1928|                    |  12Nov2013|Vanessa Miller|           PPR|                null|    Cobbs Creek Park|
//|      11| East Fairmount Park| East Fairmount Park| East Fairmount Park|                    |Land|Park- Regional/Wa...|                  |28326588.4612519| 650.29159247|  19121|                    |      1844|                    |  12Nov2013|Vanessa Miller|           PPR|                null| East Fairmount Park|
//|      12|   Tacony Creek Park|   Tacony Creek Park|   Tacony Creek Park|                    |Land|Park- Regional/Wa...|                  | 13223017.770586| 303.55993257|  19120|                    |      1915|                    |  12Nov2013|Vanessa Miller|           PPR|                null|   Tacony Creek Park|
//|      13|Franklin D. Roose...|Franklin D. Roose...|Franklin D. Roose...|     3500 S BROAD ST|Land|  Park- Metropolitan|                  |8746429.90257898| 200.79120497|  19145|                    | 1922-1924|                    |  12Nov2013|Vanessa Miller|           PPR|                null|            FDR Park|
//|      14|        Hunting Park|        Hunting Park|        Hunting Park|1101 W HUNTING PA...|Land|     Park- Community|                  |3246603.33243125|  74.53205507|  19140|                    |      1854|                    |  12Nov2013|Vanessa Miller|           PPR|                null|        Hunting Park|
//|      15|         Awbury Park|         Awbury Park|         Awbury Park|    6101 ARDLEIGH ST|Land|     Park- Community|                  |1226461.75911391|  28.15580039|  19138|                null|      1920|                    |  12Nov2013|Vanessa Miller|           PPR|                null|              Awbury|
//|      16|         Fisher Park|         Fisher Park|         Fisher Park|      6000 N 05TH ST|Land|     Park- Community|                  |1067144.68896532|  24.49836909|  19120|                    |      1909|                    |  12Nov2013|Vanessa Miller|           PPR|                null|         Fisher Park|

// Notice how every record will contain the word park (in USE_ col name). You can see in object id that some records are being skipped since we skip the ones that are not parks 
//eventual goal: conjoin / union BOTH of these dataframes. Both dataframes are similar. We will use same column names when creating the columns of this dataframe.
		df = df.withColumn("park_id", concat(lit("phil_"), df.col("OBJECTID"))) //similar to what we did in the durham. DONT FORGET TO SELF ASSIGN TO CREATE NEW ONE
		.withColumnRenamed("ASSET_NAME", "park_name") //doesnt create new col but takes existing col and renames it
		.withColumn("city", lit("Philadelphia")) //similar to prior, give it literal value of philly
		.withColumnRenamed("ADDRESS", "address") //simply lowercasing this column, address is improtant field
		.withColumn("has_playground", lit("UNKNOWN")) //so in the other DF, we included this col. In this CSV we dont have this info so we'll just call it unknown (easier to have similar number of columns for the union)
		.withColumnRenamed("ZIPCODE", "zipcode") //zipcode info IS in here so we'll just rename it to be lowercase
//		.withColumn("land_in_acres", df.col("ACREAGE")) //also exists in this file
		.withColumnRenamed("ACREAGE", "land_in_acres") //changed to rename the column isntead
		.withColumn("geoX", lit("UNKNOWN")) //making this a literal because we do not have the geo location info in the CSV
		.withColumn("geoY", lit("UNKNOWN"))
	
//when looking @ our new things. Notice how the DF has the objectID, new park_name col which was changed and is reflected. Havent dropped any columns... yet.
//ACERAGE is also still here because we did NOT rename the acerage column, we CREATED a new column
//we can see that land_in_acres is present. So we can drop the ACERAGE column completely later or rename this. We will rename this.
		
// NEW SCHEMA
//root
// |-- OBJECTID: string (nullable = true)
// |-- park_name: string (nullable = true)
// |-- SITE_NAME: string (nullable = true)
// |-- CHILD_OF: string (nullable = true)
// |-- address: string (nullable = true)
// |-- TYPE: string (nullable = true)
// |-- USE_: string (nullable = true)
// |-- DESCRIPTION: string (nullable = true)
// |-- SQ_FEET: string (nullable = true)
// |-- ACREAGE: string (nullable = true)
// |-- zipcode: string (nullable = true)
// |-- ALLIAS: string (nullable = true)
// |-- CHRONOLOGY: string (nullable = true)
// |-- NOTES: string (nullable = true)
// |-- DATE_EDITED: string (nullable = true)
// |-- EDITED_BY: string (nullable = true)
// |-- OCCUPANT: string (nullable = true)
// |-- TENANT: string (nullable = true)
// |-- LABEL: string (nullable = true)
// |-- park_id: string (nullable = true)
// |-- city: string (nullable = false)
// |-- has_playground: string (nullable = false)
// |-- land_in_acres: string (nullable = true)
// |-- geoX: string (nullable = false)
// |-- geoY: string (nullable = false)
		
// NEW DF
//+--------+--------------------+--------------------+--------------------+--------------------+----+--------------------+------------------+----------------+-------------+-------+----------------+----------+-----+-----------+--------------+--------+------+--------------------+-------+------------+--------------+-------------+-------+-------+
//|OBJECTID|           park_name|           SITE_NAME|            CHILD_OF|             address|TYPE|                USE_|       DESCRIPTION|         SQ_FEET|      ACREAGE|zipcode|          ALLIAS|CHRONOLOGY|NOTES|DATE_EDITED|     EDITED_BY|OCCUPANT|TENANT|               LABEL|park_id|        city|has_playground|land_in_acres|   geoX|   geoY|
//+--------+--------------------+--------------------+--------------------+--------------------+----+--------------------+------------------+----------------+-------------+-------+----------------+----------+-----+-----------+--------------+--------+------+--------------------+-------+------------+--------------+-------------+-------+-------+
//|       2|    Schuylkill Banks|    Schuylkill Banks|    Schuylkill Banks|   400-16 S TANEY ST|Land|Park- Linear\Parkway|              null| 658060.84696869|  15.10705875|  19103|            null|      null| null|  11Feb2015|Nora Dougherty|     PPR|  null|    Schuylkill Banks| phil_2|Philadelphia|       UNKNOWN|  15.10705875|UNKNOWN|UNKNOWN|
//|       4|      Peter's Island|      Peter's Island|      Peter's Island|                null|Land|Park- Regional/Wa...|Regional\Watershed|  83679.29305106|   1.92101992|   null|            null|      null| null|  22Aug2014|Nora Dougherty|     PPR|  null|      Peter's Island| phil_4|Philadelphia|       UNKNOWN|   1.92101992|UNKNOWN|UNKNOWN|
//|       5|Frankford Arsenal...|Frankford Arsenal...|Frankford Arsenal...|  5625 Tacony Street|Land|  Park- Neighborhood|              null| 477860.28847784|  10.97020661|   null|PA Fish and Boat|      2014| null|  22Aug2014|Nora Dougherty|     PPR|  null|Frankford Arsenal...| phil_5|Philadelphia|       UNKNOWN|  10.97020661|UNKNOWN|UNKNOWN|
//|       6|Orthodox Street P...|Orthodox Street P...|Orthodox Street P...|3101 Orthodox Street|Land|  Park- Neighborhood|              null| 457597.04880592|  10.50502478|   null|            null|      2014| null|  22Aug2014|Nora Dougherty|    null|  null|            Orthodox| phil_6|Philadelphia|       UNKNOWN|  10.50502478|UNKNOWN|UNKNOWN|
//|       7|Wissahickon Valle...|Wissahickon Valle...|Wissahickon Valle...|                    |Land|Park- Regional/Wa...|                  |88949112.8413902|2043.81972119|  19128|                |      1867|     |  12Nov2013|Vanessa Miller|     PPR|  null|Wissahickon Valle...| phil_7|Philadelphia|       UNKNOWN|2043.81972119|UNKNOWN|UNKNOWN|
//|       8| West Fairmount Park| West Fairmount Park| West Fairmount Park|                    |Land|Park- Regional/Wa...|                  |61124064.2157099|1403.22104483|  19131|                |      1866|     |  12Nov2013|Vanessa Miller|     PPR|  null| West Fairmount Park| phil_8|Philadelphia|       UNKNOWN|1403.22104483|UNKNOWN|UNKNOWN|
//|       9|Pennypack Creek Park|Pennypack Creek Park|Pennypack Creek Park|                    |Land|Park- Regional/Wa...|                  |58572936.1393227|1344.65496859|  19152|                | 1905-1929|     |  12Nov2013|Vanessa Miller|     PPR|  null|Pennypack Creek Park| phil_9|Philadelphia|       UNKNOWN|1344.65496859|UNKNOWN|UNKNOWN|
//|      10|    Cobbs Creek Park|    Cobbs Creek Park|    Cobbs Creek Park|                    |Land|Park- Regional/Wa...|                  |37055823.2343341|    850.68805|  19143|                | 1904-1928|     |  12Nov2013|Vanessa Miller|     PPR|  null|    Cobbs Creek Park|phil_10|Philadelphia|       UNKNOWN|    850.68805|UNKNOWN|UNKNOWN|
//|      11| East Fairmount Park| East Fairmount Park| East Fairmount Park|                    |Land|Park- Regional/Wa...|                  |28326588.4612519| 650.29159247|  19121|                |      1844|     |  12Nov2013|Vanessa Miller|     PPR|  null| East Fairmount Park|phil_11|Philadelphia|       UNKNOWN| 650.29159247|UNKNOWN|UNKNOWN|
//|      12|   Tacony Creek Park|   Tacony Creek Park|   Tacony Creek Park|                    |Land|Park- Regional/Wa...|                  | 13223017.770586| 303.55993257|  19120|                |      1915|     |  12Nov2013|Vanessa Miller|     PPR|  null|   Tacony Creek Park|phil_12|Philadelphia|       UNKNOWN| 303.55993257|UNKNOWN|UNKNOWN|
//+--------+--------------------+--------------------+--------------------+--------------------+----+--------------------+------------------+----------------+-------------+-------+----------------+----------+-----+-----------+--------------+--------+------+--------------------+-------+------------+--------------+-------------+-------+-------+

// There are a bunch of columns we don't actually need. So, lets drop them.
		.drop("SITE_NAME") //all the columns we want to drop. Just look @ output to see which ones
		.drop("OBJECTID")
		.drop("CHILD_OF")
		.drop("TYPE")
		.drop("USE_")
		.drop("DESCRIPTION")
		.drop("SQ_FEET")
		.drop("ALLIAS")
		.drop("CHRONOLOGY")
		.drop("NOTES")
		.drop("DATE_EDITED")
		.drop("EDITED_BY")
		.drop("OCCUPANT")
		.drop("TENANT")
		.drop("LABEL");
		
//updated schema
//root
// |-- park_name: string (nullable = true)
// |-- address: string (nullable = true)
// |-- land_in_acres: string (nullable = true)
// |-- zipcode: string (nullable = true)
// |-- park_id: string (nullable = true)
// |-- city: string (nullable = false)
// |-- has_playground: string (nullable = false)
// |-- geoX: string (nullable = false)
// |-- geoY: string (nullable = false)
		
//Updated results
//+--------------------+--------------------+-------------+-------+-------+------------+--------------+-------+-------+
//|           park_name|             address|land_in_acres|zipcode|park_id|        city|has_playground|   geoX|   geoY|
//+--------------------+--------------------+-------------+-------+-------+------------+--------------+-------+-------+
//|    Schuylkill Banks|   400-16 S TANEY ST|  15.10705875|  19103| phil_2|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|      Peter's Island|                null|   1.92101992|   null| phil_4|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|Frankford Arsenal...|  5625 Tacony Street|  10.97020661|   null| phil_5|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|Orthodox Street P...|3101 Orthodox Street|  10.50502478|   null| phil_6|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|Wissahickon Valle...|                    |2043.81972119|  19128| phil_7|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//| West Fairmount Park|                    |1403.22104483|  19131| phil_8|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|Pennypack Creek Park|                    |1344.65496859|  19152| phil_9|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|    Cobbs Creek Park|                    |    850.68805|  19143|phil_10|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//| East Fairmount Park|                    | 650.29159247|  19121|phil_11|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|   Tacony Creek Park|                    | 303.55993257|  19120|phil_12|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//+--------------------+--------------------+-------------+-------+-------+------------+--------------+-------+-------+	
	
// NOTE - the order of the philly DF will NOT match the order of the durham DF. SQL union must have cols in smae order of the two queries.
//we can do this in spark api. We can match based on col names so even if order doesnt match as long as col NAME matches!
		
		return df;
	}		
	
//STEP 3 still in Lesson 11
	private static void combineDataFrames(Dataset<Row> df1, Dataset<Row> df2) { //versy simple method
// first, match by col names using the unionByName method. If we use the union() method only, it will match cols based on ORDER. Remember the order is not the same
//inputs are df1 and df2 to make them generic 
		Dataset<Row> df = df1.unionByName(df2); //creating new DF object and using unionByName
		df.show(20); //example does 500 records
		df.printSchema();
		System.out.println("We have " + df.count() + " records.");
//Unioned DF where philly is first
//+--------------------+--------------------+-------------+-------+-------+------------+--------------+-------+-------+
//|           park_name|             address|land_in_acres|zipcode|park_id|        city|has_playground|   geoX|   geoY|
//+--------------------+--------------------+-------------+-------+-------+------------+--------------+-------+-------+
//|    Schuylkill Banks|   400-16 S TANEY ST|  15.10705875|  19103| phil_2|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|      Peter's Island|                null|   1.92101992|   null| phil_4|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|Frankford Arsenal...|  5625 Tacony Street|  10.97020661|   null| phil_5|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|Orthodox Street P...|3101 Orthodox Street|  10.50502478|   null| phil_6|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|Wissahickon Valle...|                    |2043.81972119|  19128| phil_7|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//| West Fairmount Park|                    |1403.22104483|  19131| phil_8|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|Pennypack Creek Park|                    |1344.65496859|  19152| phil_9|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|    Cobbs Creek Park|                    |    850.68805|  19143|phil_10|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//| East Fairmount Park|                    | 650.29159247|  19121|phil_11|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|   Tacony Creek Park|                    | 303.55993257|  19120|phil_12|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|Franklin D. Roose...|     3500 S BROAD ST| 200.79120497|  19145|phil_13|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|        Hunting Park|1101 W HUNTING PA...|  74.53205507|  19140|phil_14|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|         Awbury Park|    6101 ARDLEIGH ST|  28.15580039|  19138|phil_15|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|         Fisher Park|      6000 N 05TH ST|  24.49836909|  19120|phil_16|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|Eastwick Regional...|   80TH ST & MARS PL|  23.36277824|  19153|phil_17|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|   82nd & Lyons Park|      8200 LYONS AVE|   0.91161843|  19153|phil_38|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|60th & Baltimore ...|  5961 BALTIMORE AVE|   0.27262624|  19143|phil_45|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|47th & Grays Ferr...|   4700 PASCHALL AVE|   0.16961917|  19143|phil_48|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|      Community Park|854-56 N LAWRENCE ST|   0.08173681|  19123|phil_53|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//|71st & Dicks Tria...|      2700 S 71ST ST|   0.06807288|  19153|phil_54|Philadelphia|       UNKNOWN|UNKNOWN|UNKNOWN|
//+--------------------+--------------------+-------------+-------+-------+------------+--------------+-------+-------+
		
// unioned schema where philly is first
//root
// |-- park_name: string (nullable = true)
// |-- address: string (nullable = true)
// |-- land_in_acres: string (nullable = true)
// |-- zipcode: string (nullable = true)
// |-- park_id: string (nullable = true)
// |-- city: string (nullable = false)
// |-- has_playground: string (nullable = true)
// |-- geoX: string (nullable = true)
// |-- geoY: string (nullable = true)
		
//record count
//We have 290 records.
		
//unioned df when Durham is first
//+--------------------+--------------------+------+--------------------+--------------+-------+------------------+------------------+------------------+
//|             park_id|           park_name|  city|             address|has_playground|zipcode|     land_in_acres|              geoX|              geoY|
//+--------------------+--------------------+------+--------------------+--------------+-------+------------------+------------------+------------------+
//| city-parks_7_Durham|         BURTON PARK|Durham|       1100 SIMA AVE|             Y|27701.0|       11.52028555|  -78.887101729377| 35.97782539263909|
//|city-parks_35_Durham|       ROCKWOOD PARK|Durham|     2310 WHITLEY DR|             Y|27707.0|       12.25698762| -78.9218170838534|35.972950743249214|
//|city-parks_37_Durham|    SANDY CREEK PARK|Durham| 3510 SANDY CREEK DR|             N|27707.0|      101.74644709|-78.96868492509134|35.969520579760456|
//|city-parks_27_Durham|  MORREENE ROAD PARK|Durham|    1102 MORREENE RD|             Y|27705.0|       11.97479141|-78.95585544233714| 36.00703991938609|
//|city-parks_13_Durham|    DREW/GRANBY PARK|Durham|        1100 DREW ST|             Y|27701.0|        0.43687249|-78.88244500505519|36.001372698873865|
//|city-parks_67_Durham|CORNWALLIS ROAD PARK|Durham|        2830 WADE RD|             Y|27705.0|       20.08746668|-78.95247102687885| 35.97845591554436|
//|city-parks_74_Durham|   SPRUCE PINE LODGE|Durham|      2802 BAHAMA RD|             Y|27503.0|       14.62743857| -78.8657412809851| 36.17446566800999|
//|city-parks_50_Durham|LITTLE RIVER FISH...|Durham|1500 ORANGE FACTO...|             N|27712.0|         7.7665654|-78.87163711684084| 36.12714426429574|
//|city-parks_45_Durham|       WESTOVER PARK|Durham|   1900 MARYLAND AVE|             Y|27705.0|        1.82773177|-78.92170094018387| 36.02510393190939|
//|city-parks_51_Durham|      MAPLEWOOD PARK|Durham| 1530 CHAPEL HILL RD|             Y|27701.0|0.5451495699999991| -78.9218938103797| 35.99381294353847|
//|city-parks_49_Durham|     COLEY ROAD PARK|Durham|       2002 COLEY RD|             N|27703.0|        20.0682389|-78.74255634400343| 35.96472498394306|
//|city-parks_78_Durham|LAKE MICHIE OVERL...|Durham|      2527 BAHAMA RD|             N|27503.0|        0.07670119|-78.86065483202263|  36.1740268383518|
//|city-parks_75_Durham|LAKE MICHIE BOATI...|Durham|      2802 BAHAMA RD|             N|27503.0|       42.69697877|  -78.854126946916| 36.17085403189861|
//|city-parks_70_Durham|LAKE MICHIE RECRE...|Durham|      2235 BAHAMA RD|             N|27503.0|       52.47036586|-78.86791791671527|36.176032517608476|
//|city-parks_73_Durham|LAKE MICHIE - DUK...|Durham|      2802 BAHAMA RD|             N|27503.0|        3.77334478|-78.85286258575475|36.166228584958006|
//|city-parks_77_Durham|HOLTON ATHLETIC F...|Durham|     401 N DRIVER ST|             N|27703.0|        2.49891964|-78.88013531009233|35.989599899061645|
//|city-parks_21_Durham|HOLT SCHOOL ROAD ...|Durham| 4102 HOLT SCHOOL RD|             Y|27704.0|        4.68872463|-78.90478347540818|36.056402194997695|
//|city-parks_29_Durham|        OAKWOOD PARK|Durham|     411 HOLLOWAY ST|             Y|27701.0|        0.62369843|-78.89388190407396| 35.99486220007635|
//|city-parks_16_Durham|       EAST END PARK|Durham|   1200 N ALSTON AVE|             Y|27701.0|       13.53019344|-78.88558889319195| 35.99735648048076|
//|city-parks_17_Durham|  ELMIRA AVENUE PARK|Durham|      540 ELMIRA AVE|             Y|27707.0|       12.02700808| -78.9035875682868|35.963038140869195|
//+--------------------+--------------------+------+--------------------+--------------+-------+------------------+------------------+------------------+
		
//new schema when durham is first
//root
// |-- park_id: string (nullable = true)
// |-- park_name: string (nullable = true)
// |-- city: string (nullable = false)
// |-- address: string (nullable = true)
// |-- has_playground: string (nullable = true)
// |-- zipcode: string (nullable = true)
// |-- land_in_acres: string (nullable = true)
// |-- geoX: string (nullable = true)
// |-- geoY: string (nullable = true)

// ok so notice. philly schema has park name then address. But with durham, park id is first!!

// NOTE ABOUT PARTITIONS - when we load small data set into DF (like 128 MB) spark creates ONE partition. SInce we have 2 DFs, we have 2 patitions.
//we combine the two DFs together and assign to new DF, the new DF still relies on original partitions of one or more for each of the data sets.
//with large DFs we would have multiple partitions for EACH DF. 
		df = df.repartition(5); //this fcn specifies that we want to repartition this DF to have 5 partitions. even after running, we will still only hav 2 because DFs are IMMUTABLE! Reasign it to itself and now we see 5 partitions
		
		Partition[] partitions = df.rdd().partitions(); //rdd, resilient distributed dataset. WIll talk about this in a future lecture, foundational datastructure spark uses
		System.out.println("Total num of Partitions: " + partitions.length);
		
//output for partitions (before the df.repartition()):
//Total num of Partitions: 2
	}
	
	
}

