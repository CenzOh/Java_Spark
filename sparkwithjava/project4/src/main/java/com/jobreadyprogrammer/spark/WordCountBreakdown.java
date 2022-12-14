//lecture 15 my code
package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.jobreadyprogrammer.mappers.LineMapperBreakdown;

public class WordCountBreakdown {
	
	public void start() {
		
		 String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" + 
			  		"'for', 'if', 'in', 'into', 'is', 'it',\r\n" + 
			  		"'no', 'not', 'of', 'on', 'or', 'such',\r\n" + 
			  		"'that', 'the', 'their', 'then', 'there', 'these',\r\n" + 
			  		"'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," + 
			  		"'your', 'you', 'I', "
			  		+ " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";
//will be ignoring this ^ for now
		 
		 SparkSession spark = SparkSession.builder()
				 .appName("unstructured text to flatmap") //boilerplate
				 .master("local")
				 .getOrCreate();
		 
		 String filename = "src/main/resources/shakespeare.txt"; //loading text file into a DF 
		 
		 Dataset<Row> df = spark.read().format("text") //load up into DF with just one column. Typical format for unstructred format.  In the course we dealth with structured data like JSOn and CSV
				 .load(filename); //this is an unstructred file, read the file!
//GOAL - parse the shakespear TXT which has a ton of shakespear works, and find the most commonly used word by shakespeare.
//cool thing, the code we right here can work with even BIGGER data then the shakespeare one. THink if we want to find most popualr word on the internet? We can do it with the same program we'll be writing
		 
		 df.printSchema();
		 df.show(10);

// schema:
// root
// |-- value: string (nullable = true)
// 
//ten rows of DF:
// +--------------------+
// |               value|
// +--------------------+
// |This is the 100th...|
// |is presented in c...|
// |Library of the Fu...|
// |often releases Et...|
// |                    |
// |         Shakespeare|
// |                    |
// |*This Etext has c...|
// |                    |
// |<<THIS ELECTRONIC...|
// +--------------------+
		 
//for us to find the word that occurs most frequent, we need to get the count of occurences for each word. So we need to break up the rows to be a list of words
// so we will use a new fcn, flatMap!! takes one input, produces many outputs. Recall how map() fcn is one to one, takes many outputs produce many outputs
//and the reduce function takes many outputs and produces ONE output. So flatmap is the final piece of the puzzle and OPPOSITE of REDUCE.
		 
		 Dataset<String> wordsDS = df.flatMap(new LineMapperBreakdown(), Encoders.STRING()); //we can split text based on twhitespace and store every word. Each row will contain ONE word
//dataset of STRINGS. flatmap expects an instance of a class that implements the flat map fcn interface. Encoders is just string
//import line mapper after we create it. Make sure its spelled correct.

// what does it do? Takes the DF, invokes the flat map function, we tell it how to parse each of the rows (by whitespace) in the mapper we defeined.
//encoder, datatype for this set is JUST string, no user defiend thing. Each row split on whitespace, flatmap flatterns out all words into one large dataset. 
		 
		 wordsDS.show(10); //testing it out to see if it worked
// +----------+
// |     value|
// +----------+
// |     [This|
// |        is|
// |       the|
// |     100th|
// |     Etext|
// |      file|
// | presented|
// |        by|
// |   Project|
// |Gutenberg,|
// +----------+
		 
//it works!!! THis is the hard part and we are done. Easy part is to invoke group by with count to get the counts of each of those words and order them
//rememebr we can invoke sql fcns but it wont use tungston optimization, so make sure you turn this back into a DF
		 
		 Dataset<Row> df2 = wordsDS.toDF();
		 
		 df2 = df2.groupBy("value").count(); //this will return another dataset and will now have two columns.
		 
		 df2 = df2.orderBy(df2.col("count").desc()); //order by fcn, we want to order by count col, make it desc so we see highest count first
		 
//		 df2.show(20);
		 
// +-----+------+
// |value| count|
// +-----+------+
// |     |392445|
// |    [|109856|
// |  the| 22655|
// |    I| 19163|
// |  and| 17818|
// |   to| 15322|
// |   of| 15204|
// |    a| 12147|
// |   my| 10613|
// |   []|  9455|
// |   in|  9343|
// |  you|  8708|
// |   is|  7658|
// | that|  7332|
// |  And|  7065|
// |  not|  6737|
// | with|  6564|
// |  his|  6066|
// | your|  5844|
// |   be|  5774|
// +-----+------+
//okay cool it works. WE have two cols which is awesome. And its in descending order so highest number is at top. A lot of these words are honestly boring.
//lets filter out. Thats where the borign words string comes in handy
		 
		 df2 = df2.filter("lower(value) NOT IN " + boringWords); //like a sql condition where we can tell what should NOT be included here. The string will be concatenated toegether.
//another problem is that a lot of words are capital and lowercase, so lets normalize it to make all words lowercase. Add lower() fcn		 
		 
		 df2.show(20);
// +-----+------+
// |value| count|
// +-----+------+
// |     |392445|
// |   my| 10613|
// | have|  5097|
// |   me|  4508|
// | thou|  4159|
// |  thy|  3561|
// |shall|  2949|
// |   so|  2857|
// |  all|  2820|
// |   do|  2766|
// |  her|  2704|
// | from|  2254|
// | what|  1981|
// | good|  1968|
// |   am|  1950|
// |would|  1913|
// | What|  1804|
// | I'll|  1713|
// | thee|  1687|
// |   My|  1622|
// +-----+------+
//ok better but there are still some boring words. We can always define more. We can also show top 500 words to find more interesting words and filter on our own.
				 
//can the query we are using be optimized? We do groupby, order, then filter. WOuldnt make sense to do filter first? Makes sense we can do that
//cool thing is we converted the dataset into a DF. SPark is smart enough to see we are doing group by here and understands it should do filter op first. Has different sequence of isntructions.
//So DF uses query optimzier called catalyst optimizer as well as tungsten to run ops in more efficient manner
 //flatmap is a lower level fucn used for handlign these kidnds of parsing scenarios as well as map and reduced fncs.
		 
		 
	}

}
