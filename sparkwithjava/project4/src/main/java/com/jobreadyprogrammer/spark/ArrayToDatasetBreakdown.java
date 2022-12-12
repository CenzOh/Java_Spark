// Lesson 12, writing my notes here
package com.jobreadyprogrammer.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
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
//		ds = df.as(Encoders.STRING()); //hover over this and you can see it returns dataset as a string. assign back to DF does not work, expecting to return to a DS
		
//now remember the array items are being treated as string. What if they were each an object? We can use encoder to choose lets say the vehicle class but it wouldnt work well
//we would have to map out what each row represents. Convert DF to DS, we would have to specify the mapping between the properties of the object rate the fields that are on the object class to the columns in a dataframe or that file.
//ex - first col is VIN number, you would need to map out the VIN num to class property which would be a diff name like vehicle ID.
//so if you want to work with POJOs in spark, need to use fcn called MAP. We will look at this next lecture.
//RECAP - we ultimately want our DSs to be converted to DFs for further processing, more performance optimizations with DFs. But we lose type safety, a row is just a generic row. Each col will have a primitive data type tho. 
		
//LECTURE 13. The map method can be invoked on a dataset object. Lets show an example with ds.map() Remember map each of these items inside the DS into something else
//when we do ds.map, select the second one that inputs argument MapFunction. The first ver is Scala. So in this fcn, first arg is map function interface. Second arg is encoder
//so for our second arg, we must define a class that implements our map fcn. We can also use lambdas since they are serializable. 
		ds = ds.map(new StringMapper(), Encoders.STRING());
		ds.show(10); //still works like on the DF

//output
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
		
//let us do the same thing w/o defining a new class. DO this with lambda. Simply need an implementation of whats going on here and the mapping fcn
		ds = ds.map((MapFunction<String, String>) row -> "word: " + row, Encoders.STRING());
//with the lambda, we also say each row of the dataset will concat with the row value with word (this part, row -> "word: " + ...)
//we did this WITHOUT defining a seperate class. THats awesome! Lambda fcns auto seralizable. We implemented the string mapper fcn. Code will still run.
		
//next fcn, the REDUCE fcn. WE will input the 6 elements in the DS and output only one element. What is type of that element? Type String, 6 string to one string
		String stringValue = ds.reduce(new StringReducer()); //make sure to pick java representation of the reduce function. Remember, it returns a string
	
		System.out.println(stringValue); //after the reduce method
//output: word: word: Bananaword: word: Carword: word: Glassword: word: Bananaword: word: Computerword: word: Car
//the entire dataset concenated together. Remember each element in the dataset became `word: Banana`, etc.
	
	}
	
//lets define a clas outside of ArrayToDataset class, we'll mkae this class static. This implements the map function with a string and another type we'll define below
	static class StringMapper implements MapFunction<String, String>, Serializable { //this means input is String and output is string. We also want this to be serializable
//if this string mapper was not seralized and NOT static, we would hit an excpetion. Seralization stack, exception thread main, TASK NOT SERIALIZABLE that means you must seralize your mapper and make the class static

		private static final long serialVersionUID = 1L; //default serial version we added

		//make sure to import for map func and add unimplemented methods for string mapper
		@Override
		public String call(String value) throws Exception {
			return "word: " + value;
		}
	
	}
	
	static class StringReducer implements ReduceFunction<String>, Serializable { //import reduce fcn and add the unimplemented method. Similar to string mapper

		private static final long serialVersionUID = 1L;

		@Override
		public String call(String v1, String v2) throws Exception { //iterates through entire list of elements, assigns value 1 to first one, value 2 to the other.
			return v1 + v2; //how we return. WIll concatenate one string with the next for ALL strings. Expect one string output with concatenated words
		} 
		
	}
	
}
