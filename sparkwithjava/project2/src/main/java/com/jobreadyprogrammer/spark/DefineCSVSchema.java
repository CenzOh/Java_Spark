package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DefineCSVSchema {

	public void printDefinedSchema() { //this method does the same idea as before
	
        SparkSession spark = SparkSession.builder() //define spark session
        .appName("Complex CSV with a schema to Dataframe")
        .master("local")
        .getOrCreate();
        
        StructType schema = DataTypes.createStructType(new StructField[] { //struct type comes from import org.apache.spark.sql.type. This allows us to define our own data types
	            DataTypes.createStructField( //specify array of all the fields to define our schema
	                "id", //First field, we want to call the first field ID instead of listingID
	                DataTypes.IntegerType, //here we define the data type to be integer
	                false), //if you open the createStructField definition by hovering, you will see this argument is asking if the field can be NULL or not.
	            DataTypes.createStructField(
	                "product_id",
	                DataTypes.IntegerType,
	                true), //for product id, nullable is true. We are NOT required to provide a product id. THe product will still display w/o an ID
	            DataTypes.createStructField(
	                "item_name",
	                DataTypes.StringType,
	                false),
	            DataTypes.createStructField(
	                "published_on",
	                DataTypes.DateType, //setting pulished on to be of date type.
	                true),
	            DataTypes.createStructField(
	                "url",
	                DataTypes.StringType,
	                false) });
	     
	        Dataset<Row> df = spark.read().format("csv") //same ideas as before in the InferCSVSchema class
	            .option("header", "true")
	            .option("multiline", true) 
	            .option("sep", ";")
	            .option("dateFormat", "M/d/y")
	            .option("quote", "^")
	            .schema(schema) //here, we are including the custom schema we just set above. Very important to include in the spark.read() option!!!!
	            .load("src/main/resources/amazonProducts.txt");
	     
	        df.show(5, 15);
	        df.printSchema();
		
	}

}

//CONSOLE OUTPUT - look how date column is of date data type!!
//
//+---+----------+---------------+------------+---------------+
//| id|product_id|      item_name|published_on|            url|
//+---+----------+---------------+------------+---------------+
//|  1|        12|TP-Link AC75...|  2017-09-10|http://a.co/...|
//|  2|        13|PHICOMM K3C ...|  2018-11-15|http://a.co/...|
//|  3|        17|Tuft & Needl...|  2016-08-22|http://a.co/...|
//|  4|        22|Queen Size S...|  2016-08-22|http://a.co/...|
//+---+----------+---------------+------------+---------------+
//
//root
// |-- id: integer (nullable = true)
// |-- product_id: integer (nullable = true)
// |-- item_name: string (nullable = true)
// |-- published_on: date (nullable = true)
// |-- url: string (nullable = true)

