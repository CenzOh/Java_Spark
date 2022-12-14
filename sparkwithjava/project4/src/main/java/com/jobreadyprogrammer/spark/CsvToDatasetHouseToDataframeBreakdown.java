//lecture 14, my version
package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import com.jobreadyprogrammer.mappers.HouseMapperBreakdown;
import com.jobreadyprogrammer.pojos.HouseBreakdown;

public class CsvToDatasetHouseToDataframeBreakdown {
	
	public void start() {
		
		SparkSession spark = SparkSession.builder() //CTRL SHIFT O on spark session to import it
				.appName("CSV to dataframe to Dataset<House> and back") //boilerplate
				.master("local")
				.getOrCreate();
		
		String filename = "src/main/resources/houses.csv"; //grabbing file, string has the path. Inject this file.
		
		Dataset<Row> df = spark.read().format("csv") //creating a dataframe, make sure when we read it knows we are reading a CSV
				.option("inferScmea",  "true") //telling spark to guess what the schema is, we are not providing one
				.option("header",  true) //the CSV has headers rows
				.option("sep", ";") // the separator in the CSV is not a comma but a semi colon, specify that here 
				.load(filename); //telling spark WHAT we want to load. Remembr, filename has the path to the CSV file
		
		System.out.println("House ingested in a dataframe:");
		df.show(5); //first 5 records
		df.printSchema(); //print schema like usual.
		
		Dataset<HouseBreakdown> houseDS = df.map(new HouseMapperBreakdown(), Encoders.bean(HouseBreakdown.class));//NOT A DF. each fild coming from file gets mapped to the POJO. main work being done here
//new instance of house mapper, implements funcitonality of HOW to map the field coming from the csv
// import the housemapper class. It needs to implemetn map function interface so lets define it in its file
		
		System.out.println("***********House ingested in a dataset:");
		houseDS.show(5); //first 5 records
		houseDS.printSchema(); //print schema like usual.
		
//House ingested in a dataframe: 
//+---+--------------------+----+--------+-------------------+
//| id|             address|sqft|   price|           vacantBy|
//+---+--------------------+----+--------+-------------------+
//|  1|609 Bayway Rd Vir...|1531|300000.0|2018-10-31 00:00:00|
//|  2|3220 Kenmore Rd R...|2776|125000.0|2019-04-11 00:00:00|
//|  3|400 W 29th St Nor...|2164| 54900.0|2019-02-01 00:00:00|
//|  4|3223 Park Ave Ric...|1740|390000.0|2019-03-22 00:00:00|
//|  5|3645 Barn Swallow...|1800|212950.0|2019-08-20 00:00:00|
//+---+--------------------+----+--------+-------------------+
//only showing top 5 rows
//
//root
// |-- id: integer (nullable = true)
// |-- address: string (nullable = true)
// |-- sqft: integer (nullable = true)
// |-- price: double (nullable = true)
// |-- vacantBy: timestamp (nullable = true)
//
//*******House ingested in a dataset: 
//+--------------------+---+--------+----+--------------------+
//|             address| id|   price|sqft|            vacantBy|
//+--------------------+---+--------+----+--------------------+
//|609 Bayway Rd Vir...|  1|300000.0|1531|[31, 0, 10, 0, 0,...|
//|3220 Kenmore Rd R...|  2|125000.0|2776|[11, 0, 4, 0, 0, ...|
//|400 W 29th St Nor...|  3| 54900.0|2164|[1, 0, 2, 0, 0, 1...|
//|3223 Park Ave Ric...|  4|390000.0|1740|[22, 0, 3, 0, 0, ...|
//|3645 Barn Swallow...|  5|212950.0|1800|[20, 0, 8, 0, 0, ...|
//+--------------------+---+--------+----+--------------------+
//only showing top 5 rows
//
//root
// |-- address: string (nullable = true)
// |-- id: integer (nullable = true)
// |-- price: double (nullable = true)
// |-- sqft: integer (nullable = true)
// |-- vacantBy: struct (nullable = true)
// |    |-- date: integer (nullable = true)
// |    |-- hours: integer (nullable = true)
// |    |-- minutes: integer (nullable = true)
// |    |-- month: integer (nullable = true)
// |    |-- seconds: integer (nullable = true)
// |    |-- time: long (nullable = true)
// |    |-- year: integer (nullable = true)
		
//notice the difference? Df takes each of cols as is from file and makes basic schema. It figures out sqft integer, etc.
//however, for the dataset, the vacnatby is much differnt. It is turned into a structure. Not just a timestamp. An array of all teh elemetns that make up the date.
//you can pick and choose the elements of the date here. vacnatby.hours to get the hours for example!! But now lets convert the DS back to DF
		
		Dataset<Row> df2 = houseDS.toDF(); //as simple as that and coverting it to a second DF.
		df2 = df2.withColumn("formattedDate", concat(df2.col("vacantBy.date"), lit("_"), df2.col("vacantBy.year"))); 
//this is making a new col (using withcol) called formatted date. We concatenate the vacantby date (by calling the df.column fcn) with the cavantby year and put a _ in the middle with literal (lit) fcn
//make sure to import lit. Have to manual import with `import static org.apache.spark.sql.functions.*;`
//and make sure you reference the NEW DF not the old one
		df2.show(6);

//output
//+--------------------+---+--------+----+--------------------+------------+
//|             address| id|   price|sqft|            vacantBy|formatedDate|
//+--------------------+---+--------+----+--------------------+------------+
//|609 Bayway Rd Vir...|  1|300000.0|1531|[31, 0, 10, 0, 0,...|      31_118|
//|3220 Kenmore Rd R...|  2|125000.0|2776|[11, 0, 4, 0, 0, ...|      11_119|
//|400 W 29th St Nor...|  3| 54900.0|2164|[1, 0, 2, 0, 0, 1...|       1_119|
//|3223 Park Ave Ric...|  4|390000.0|1740|[22, 0, 3, 0, 0, ...|      22_119|
//|3645 Barn Swallow...|  5|212950.0|1800|[20, 0, 8, 0, 0, ...|      20_119|
//|3020 Scarsborough...|  6|349000.0|2340|[22, 0, 11, 0, 0,...|      22_118|
//+--------------------+---+--------+----+--------------------+------------+

//now we have this incorrect format but it does what we want. We could tweak on our own of course. 
//RECAP AND REMEMBER: When we work with USER DEFINED TYPEs, we have to switch gears and go into the dataset and define it with the user defined type.
//if working with jsut strings or just text data, we can create a data set of type string and that is okay!
//just make sure you convert BACK to the dataframe. again DF is more generic type of data set where type parameter is type row. DF allows for tungsten optimizatio and memory management
		
	}
}
