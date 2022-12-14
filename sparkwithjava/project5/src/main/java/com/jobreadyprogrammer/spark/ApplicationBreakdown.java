//lecture 16 my code
package com.jobreadyprogrammer.spark;

import static org.apache.spark.sql.functions.*; //describing below

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ApplicationBreakdown { //most work will be done herer
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder()
				.appName("Learning Spark SQL Dataframe API")
				.master("local")
				.getOrCreate();
		
		String studentsFile = "src/main/resources/students.csv"; //first file, loading into df
		
			Dataset<Row> studentDf = spark.read().format("csv") //CTRL SHIFT O to import dataset
					.option("inferSchema", "true") //have to use string verion here
					.option("header", true)
					.load(studentsFile);
			
		String gradeChartFile = "src/main/resources/grade_chart.csv"; //second file loading into another dataframe
			
			Dataset<Row> gradesDf = spark.read().format("csv")
					.option("inferSchmea", "true")
					.option("header", true)
					.load(gradeChartFile);
			
//we'll learn how to join two DFs together. We need to unserstand whats going on with data first. look at source main resources.
//All above was boilerplate pretty much
//let us jopin students DF with the grades DF
			
//			studentDf.join(gradesDf, studentDf.col("GPA").equalTo(gradesDf.col("gpa"))).show(); 
//one way to do this. First param is which DF to join. Second param is which column to join on (since the cols can be named differnrlt)
//we reference the col with .col, we are grabbing GPA from student DF. use .equalTo so we can say join on the grades gpa col too.
//OUTPUT:
//+----------+--------------+-----+---+--------------------+-------+---+------------+
//|student_id|  student_name|State|GPA| favorite_book_title|working|gpa|letter_grade|
//+----------+--------------+-----+---+--------------------+-------+---+------------+
//|      1100|   Royce Piche|   NJ|1.5|To Kill a Mocking...|   true|1.5|           D|
//|      1120|Alexis Morriss|   NJ|3.0| Pride and Prejudice|  false|3.0|           B|
//|      1130|   Len Tarbell|   NJ|3.5|The Diary of Anne...|  false|3.5|          B+|
//|      1140|Alejandro Dory|   NY|2.5|Harry Potter and ...|  false|2.5|          C+|
//|      1150|Ricky Tremaine|   NY|3.0|The Lord of the R...|   true|3.0|           B|
//|      1160|   Monika Gift|   NY|3.0|    The Great Gatsby|   true|3.0|           B|
//|      1170| Kristeen Line|   CA|4.0|         Animal Farm|  false|4.0|           A|
//|      1180| Sonia Rickard|   CA|4.0|Harry Potter and ...|  false|4.0|           A|
//|      1190| Dan Iacovelli|   CA|3.5|    The Hunger Games|  false|3.5|          B+|
//|      1200|     Ned Alvin|   CA|1.0|                null|   true|1.0|           F|
//|      1210| Sidney Ducote|   FL|1.5|   The Secret Garden|  false|1.5|           D|
//|      1220|Bobbie Shrader|   FL|2.0|    The Color Purple|  false|2.0|           C|
//+----------+--------------+-----+---+--------------------+-------+---+------------+
		
//it works! but notice that from student_id - working, this is all the student DF. from gpa - letter_grade, this is from grades df.
//both gpa columns are correct but we are seeing GPA twice, this makes the DF col larger. SO now we can pick and choose which values we want.
//lets define this to a NEW dataframe with WHICH cols we want.
			
			Dataset<Row> joinedDf = studentDf.join(gradesDf, studentDf.col("GPA").equalTo(gradesDf.col("gpa")))
					.filter(gradesDf.col("gpa").between(2.0, 3.5)) //read below to see about the filter
					.select(studentDf.col("student_name"),//lets say we want student name in the console, can select it here
							studentDf.col("favorite_book_title"), //also displaying book title
							gradesDf.col("letter_grade")); //now remember, not joined yet so we grab letter grade from grades df
//					.filter(gradesDf.col("gpa").between(2.0, 3.5));//we can filter the dataset further with .filter
//above filter results in error. Says the gpa col could NOT be found in student name, fav book, and letter grade col.
//remember that this new df we selected only the three cols AFTER we joined and AFTER we did the select statemetn. So we REDUCED the DF. 
//filter AFTER select in this case doesnt work. Not exactly like SQL. So best to filter and joining first!!
			
			joinedDf.show(); //will stil be correct. Doesnt matter if GPA is capital or lowercase. 
//OUTPUT before the .filter:
//+--------------+--------------------+------------+
//|  student_name| favorite_book_title|letter_grade|
//+--------------+--------------------+------------+
//|   Royce Piche|To Kill a Mocking...|           D|
//|Alexis Morriss| Pride and Prejudice|           B|
//|   Len Tarbell|The Diary of Anne...|          B+|
//|Alejandro Dory|Harry Potter and ...|          C+|
//|Ricky Tremaine|The Lord of the R...|           B|
//|   Monika Gift|    The Great Gatsby|           B|
//| Kristeen Line|         Animal Farm|           A|
//| Sonia Rickard|Harry Potter and ...|           A|
//| Dan Iacovelli|    The Hunger Games|          B+|
//|     Ned Alvin|                null|           F|
//| Sidney Ducote|   The Secret Garden|           D|
//|Bobbie Shrader|    The Color Purple|           C|
//+--------------+--------------------+------------+

//OUTPUT AFTER 	.filter. Only see Bs and Cs because we filtered for this range
//+--------------+--------------------+------------+
//|  student_name| favorite_book_title|letter_grade|
//+--------------+--------------------+------------+
//|Alexis Morriss| Pride and Prejudice|           B|
//|   Len Tarbell|The Diary of Anne...|          B+|
//|Alejandro Dory|Harry Potter and ...|          C+|
//|Ricky Tremaine|The Lord of the R...|           B|
//|   Monika Gift|    The Great Gatsby|           B|
//| Dan Iacovelli|    The Hunger Games|          B+|
//|Bobbie Shrader|    The Color Purple|           C|
//+--------------+--------------------+------------+
			
//supoer cool thing with spark SQL APi, we dont even need to say studentdf.col("student_name"). We can simply do col("student_name") itll recognize it
//will complain at first so you must do a static import
//import static org.apache.spark.sql.functions.*; 
			
			studentDf.join(gradesDf, studentDf.col("GPA").equalTo(gradesDf.col("gpa")))
				.filter(gradesDf.col("gpa").between(2.0, 3.5)) //will come back to this
				.select(col("student_name"), //see, using col works here!
						col("favorite_book_title"), 
						col("letter_grade")); 

//we can go further, and REMOVE the col entirely!!
			
			studentDf.join(gradesDf, studentDf.col("GPA").equalTo(gradesDf.col("gpa")))
				.filter(gradesDf.col("gpa").between(2.0, 3.5)) //will come back to this, refer to note section below
				.select("student_name", //using NO COL fcn. Col names specified in STRINGs
						"favorite_book_title", 
						"letter_grade"); 

// TAKE NOTE - we can do the above as LONG as the cols are NOT AMBIGUOUS. Recall when sql would say ambiguous, this means SQL doesnt know WHERE it is coming form
//so an example is, if letter_grade existed in TWO DIFFERENT DFs, how would spark know which one we are referring to? It wont.
//now for gpa col, it exists in BOTH Dfs. so we can NOT do this for the filter condition. If we do, we'll get an error (even tho spark doesnt complain rihgt now)
			
			studentDf.join(gradesDf, studentDf.col("GPA").equalTo(gradesDf.col("gpa")))
				.where(gradesDf.col("gpa") //where method, similar to filter. Works similar to SQL. I
						.gt(3.0) //nstead of .between, can use greater than eqla to. GT - greater than
						.and(gradesDf.col("gpa") //.and, have to specify col name
								.lt(4.5))); //LT - less than

//OUTPUT: 
//+--------------+--------------------+------------+
//|  student_name| favorite_book_title|letter_grade|
//+--------------+--------------------+------------+
//|Alexis Morriss| Pride and Prejudice|           B|
//|   Len Tarbell|The Diary of Anne...|          B+|
//|Ricky Tremaine|The Lord of the R...|           B|
//|   Monika Gift|    The Great Gatsby|           B|
//| Kristeen Line|         Animal Farm|           A|
//| Sonia Rickard|Harry Potter and ...|           A|
//| Dan Iacovelli|    The Hunger Games|          B+|
//+--------------+--------------------+------------+

//what if we want to see top students AND failing students? lets add to it
			studentDf.join(gradesDf, studentDf.col("GPA").equalTo(gradesDf.col("gpa")))
				.where(gradesDf.col("gpa").gt(3.0).and(gradesDf.col("gpa").lt("4.5"))
												  .or(gradesDf.col("gpa").equalTo(1.0))) //.or - the or condition
				.select("student_name",			// .equalTo, checking if the condition is equal to
						"favorite_book_title", 	//so we are checking: GPA is > 3.0 and < 4.5 OR gpa == 1.0
						"letter_grade").show(); 
				
//OUTPUT:
//+-------------+--------------------+------------+
//| student_name| favorite_book_title|letter_grade|
//+-------------+--------------------+------------+
//|  Len Tarbell|The Diary of Anne...|          B+|
//|Kristeen Line|         Animal Farm|           A|
//|Sonia Rickard|Harry Potter and ...|           A|
//|Dan Iacovelli|    The Hunger Games|          B+|
//|    Ned Alvin|                null|           F|
//+-------------+--------------------+------------+
			
//now of course, best practice is to save this complex query into a df. If we had more filtering we needed to do, saving to another DF def helpful.
//again, the where method can be interchangeable with filter since they both pretty much do the same thing
			
	}

}
