// Lesson 12, writing my notes here
package com.jobreadyprogrammer.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ArrayToDatasetBreakdown {
	
	public void start() {
		SparkSession spark = new SparkSession.Builder() //simply creating a new spark session here
				.appName("Array To Dataset<String>") //simple name of the class
				.master("local")
				.getOrCreate();
		
		String [] stringList = new String[] {"Banana", "Car", "Glass", "Banana", "Computer", "Car"}; //simple array of random words. Car and banana are repeated words
		
		List<String> data = Arrays.asList(stringList); //we are converting the array into a list. Next, inject this into a dataset
		
		Dataset<String> ds = spark.createDataset(data, Encoders.STRING()); //first argument is the actual list (data). Second arg is the encoder, the data type we want each of these elements to be considered as OK

//Since we have strings we type in Encoders.STRING(). You can check out the Encoders.class and see that it is written in Scala code and see how it is broken down.
//Again, we want to interpret each of these elements in the list as a string. Think of this as the schema for the series of elements.
//next, assign this. NOTE THAT if we do Dataset<Row> df = spark.createDataset..., this will NOT work because of TYPE MISMATCH. Can not convert type dataset of STRING to ROW
//We have to type in Dataset<String> df = ... and THIS IS A DATA SET not a frame. Remember dataset<row> is a dataframe. Lets rename the variable to DS
		
		ds.printSchema(); //print the schema
		ds.show(); //show the contents

//values:
//+--------------+
//|         value|
//+--------------+
//|  word: Banana|
//|     word: Car|
//|   word: Glass|
//|  word: Banana|
//|word: Computer|
//|     word: Car|
//+--------------+
//basic shcmea:
//word: Bananaword: Carword: Glassword: Bananaword: Computerword: Car
	
//lets do some processing such as a groupby		
//		ds = ds.groupBy("value").count(); //we expect this to show us a chart or table and give another col called count that counts each occurance of the words
		
//remember, datasets are immutable so reasign it to itself but theres an error! Cant convert dataset ROW to STRING. The count method returns a dataset ROW. All fancy fncs all like to work with DFs!!!!
//we lose the type safety in the conversion but thats ok, we expect that. DF is a table like structure, can add cols, remove cols, modify them.
// so think about it, we call count on the dataset, that count will tag on another ocl called count. But we cant tag that onto a dataset. So it expects a DF to be output
//how to fix? We can create a dataframe,
		Dataset<Row> df = ds.groupBy("value").count(); 
		
//since count returns a DS row, it gives us a DF and we can print the contents of the DF.
		df.show(10); 
		
//output: exactly what we expect, banana and car appear twice
//+--------+-----+
//|   value|count|
//+--------+-----+
//|  Banana|    2|
//|   Glass|    1|
//|     Car|    2|
//|Computer|    1|
//+--------+-----+
		
//Way number 2 to convert a DS into a DF. We will use a method called toDF
		Dataset<Row> df2 = ds.toDF(); //this will expect us to assign to a dataframe. We change the DS to a DF using a fcn
		
//we can also make a DF into a DS. Use df.as(Encoder)
		ds = df.as(Encoders.STRING()); //hover over this and you can see it returns dataset as a string. assign back to DF does not work, expecting to return to a DS
		
//now remember the array items are being treated as string. What if they were each an object? We can use encoder to choose lets say the vehicle class but it wouldnt work well
//we would have to map out what each row represents. Convert DF to DS, we would have to specify the mapping between the properties of the object rate the fields that are on the object class to the columns in a dataframe or that file.
//ex - first col is VIN number, you would need to map out the VIN num to class property which would be a diff name like vehicle ID.
//so if you want to work with POJOs in spark, need to use fcn called MAP. We will look at this next lecture.
		
//RECAP - we ultimately want our DSs to be converted to DFs for further processing, more performance optimizations with DFs. But we lose type safety, a row is just a generic row. Each col will have a primitive data type tho. 
		
	}
}
