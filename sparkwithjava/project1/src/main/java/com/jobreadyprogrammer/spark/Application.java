//#Typing this version up myself and my own comment.s
//LESSON 6 First Spark APp
package com.jobreadyprogrammer.spark;

//concat
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession; //make sure to hover mouse over spark session to grab correct import

public class Application {
	public static void main(String args[]) {
//This application is a good end to end ex, read data from CSV and load into DB.
//will see how it runs differently when running on cluster (spark). Will run locally for now
		
		//establish a Session first
		SparkSession spark = new SparkSession.Builder() //function to build the session. Spark session var is called `spark`
				.appName("CSV to DB") //give the session a name
				.master("local") //for local host
				.getOrCreate(); 
		
		//where do we want to get the data from? 
		Dataset<Row> df = spark.read().format("csv") //format of data is csv data type. Other formats like xml, json. Save into a DATA FRAME
			.option("header", true) //specify if we have header, key and value pair. Our file DOES have headers
			.load("src/main/resources/name_and_comments.txt"); //specify where the actual file is. Grab name_and_comments.txt file. Specify FULL PATH
		
//Take note, in production we would be loading files from distributed file system like HDFS3, amazon, hadoop
// CTRL + SHIFT + O to select imports. Dataset row import is `org.apace.spark.sql.row`
//what is a dataframe? identified with type. Like database table with rows and columns. Can talk to it like relational table.
//ID | Name | Address | Phone | Salary
//...| ...  | ...     | ...   | ...
		
		df.show(); //displays contents of dataframe in console. Right click > Run as > Java app.
		
//To show certain number of rows, specify number inside show function. show(3) displays 3 rows.
		
//		+---------+----------+--------------------+
//		|last_name|first_name|             comment|
//		+---------+----------+--------------------+
//		|      Lon|       Jim|There are plenty ...|
//		|   Ingram|   Milford|I've been using t...|
//		|   Gideon|     Elmer|Social media has ...|
//		|     Dong|       Fen|The body is 70% w...|
//		+---------+----------+--------------------+
		
//Spark works with batch apps, concept to transform data with this API to do that. THink first last name. What if we want a new col called full name to
//concat first and last name columns?
		
		df = df.withColumn("full_name", concat(df.col("last_name"), lit(", "), df.col("first_name")));
		
//withCOlumn, creates new col. First param: specify name of col. Second param: specify which columns. We want second param to have last name AND first name in ONE column
//we do this with concat function. Put last name and first name TOGETHER. Returns a column object.
//CTRL SHIFT O does not work with importing concat fcn since it is a STATIC import.
//lit function, short for a little. Put comma and space between last name and first name! Updated import for spark sql functions to include ALL
		
		df.show();

//if we try to show, doesnt work. Why? Datasets are immunable structures, they can not be changed. They can be recreated. So assign with column back to the DF
//Now it works!
//		+---------+----------+--------------------+---------------+
//		|last_name|first_name|             comment|      full_name|
//		+---------+----------+--------------------+---------------+
//		|      Lon|       Jim|There are plenty ...|       Lon, Jim|
//		|   Ingram|   Milford|I've been using t...|Ingram, Milford|
//		|   Gideon|     Elmer|Social media has ...|  Gideon, Elmer|
//		|     Dong|       Fen|The body is 70% w...|      Dong, Fen|
//		+---------+----------+--------------------+---------------+
		
		//Next transformation. See comments with numbers. Ex - look in dataset, one comment has the number 70, another has number 10.
		df = df.filter(//filter, specify condition. 
				df.col("comment").rlike("\\d+")) //rLike, Match against regular expressions. \\d+ search for number values
				.orderBy(df.col("last_name") //orderBy, just like in SQL, select column to order the output by
						.asc()); //ascending, so Dong will come before Ingram
		df.show(); //our results will be the two records with comments including numerical values.

//First output w/o the asc and orderby:
//		+---------+----------+--------------------+---------------+
//		|last_name|first_name|             comment|      full_name|
//		+---------+----------+--------------------+---------------+
//		|   Ingram|   Milford|I've been using t...|Ingram, Milford|
//		|     Dong|       Fen|The body is 70% w...|      Dong, Fen|
//		+---------+----------+--------------------+---------------+
		
//second output with orderby and asc
//		+---------+----------+--------------------+---------------+
//		|last_name|first_name|             comment|      full_name|
//		+---------+----------+--------------------+---------------+
//		|     Dong|       Fen|The body is 70% w...|      Dong, Fen|
//		|   Ingram|   Milford|I've been using t...|Ingram, Milford|
//		+---------+----------+--------------------+---------------+
		
		//if you want, you can simply chain everything including the first transformation like so and assign to a new DF instead:
		Dataset<Row> transformedDF; //again this part totally optional, you can still asign DF to itself
		transformedDF = df.withColumn("full_name",
				concat(df.col("last_name"), lit(", "), df.col("first_name")))
				.filter(df.col("comment").rlike("\\d+"))
				.orderBy(df.col("last_name").asc());
		
//last thing, save this into a database
		String dbConnectionURL = "jdbc:postgresql://localhost/course-data"; //using postgress commands. Pom file will have postgres sql driver and dependencies
		Properties prop = new Properties(); //comes from java.utils
		prop.setProperty("driver",  "org.postgresql.Driver");
		prop.setProperty("user",  "postgress"); //default username
		prop.setProperty("password", "password"); //when setup, you will choosen an admin system.
		
		df.write() //writing to the database
			.mode(SaveMode.Overwrite) //will keep overwriting the database
			.jdbc(dbConnectionURL, "project1", prop); //connection URL, name, connection properties
		
//can check on PgAdmin tool to look at it. course_data is the name of database (we know this from the connection URL which says the same)
//if we do select * from project1, we will get our output of the two records
		
	}
}