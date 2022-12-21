//lesson 27 my code
package com.jobreadyprogrammer.spark;

//imports for logging and level
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
//boilerplate spark imports
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LinearMarketingVsSalesBreakdown {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR); //turning off logger so we only see error messages
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkSession spark = new SparkSession.Builder() //boilerplate spark
			.appName("LinearREgressionExample")
			.master("local")
			.getOrCreate();
		
		Dataset<Row> markVsSalesDf = spark.read() 	//spark will read our csv file
				.option("header", "true") 			//our data has headers, marketing and sales columns
				.option("inferSchema",  "true") 	//spark can guess the schema, two cols with ints not hard for it to figure out
				.format("csv") 						//reading a csv file
				.load("C:\\Users\\Cenzo\\JavaSpark\\workspace\\sparkwithjava\\project9\\data\\marketing_vs_sales.csv"); //locaiton of file / file path 9make sure its a csv
		
//so our csv file is simple, same data as last lecture. Marketing spent $ amount and sales amount. As markeing spent goes up, sales goes up too
		
		markVsSalesDf.show(); //just printing the Df atm
//output
//+---------+------+
//|marketing| sales|
//+---------+------+
//|    30000|280000|
//|    40000|279100|
//|    40000|220000|
//|    27000|168000|
//|    50000|250200|
//|    60000|382800|
//|    70000|450400|
//|    70000|412000|
//|    70000|410500|
//|    80000|450400|
//|    90000|505300|
//+---------+------+
//this current structure of the DF is NOT compatible with ML algorithm. They expect it to be a certain format, we will go over shortly.
//Column we want to predict / dependent variable is sales. Independent variable is marketing spent. Also known as a feature! marketing spent is a feature column. Predicts sales
//so note that the column that will be predicted (sales) needs to be called a diff name so and be consistent across all ML algorithms. 
//spark wants us to name this to be label. Again so it can be compatible with all ML algorithms. 
//same idea with the marketing spent col, this needs to be called features. If we have multiple features / independent variables, they all go into a col named features and all put together like an array
//we just have marketing spent so itll be easy for us
//ordering is dfiffenrt oo, label is FIRST col and features is SECOND col. Lets transform the dataframe.
		
		Dataset<Row> mldf = markVsSalesDf.withColumnRenamed("sales", "label") //first change name. mldf means machine learning dataframe
//			.select("label", "marketing_spend"); //reassign order so label is first
			.select("label", "marketing_spend", "bad_day"); //updated version
		
		mldf.show(); //testing for now
		
//first test, so far good
//+------+---------------+
//| label|marketing_spend|
//+------+---------------+
//|280000|          30000|
//|279100|          40000|
//|220000|          40000|
//|168000|          27000|
//|250200|          50000|
//|382800|          60000|
//|450400|          70000|
//|412000|          70000|
//|410500|          70000|
//|450400|          80000|
//|505300|          90000|
//+------+---------------+
		
//		String[] featureColumns = {"marketing_spend"}; //input, turning the marketing spend into an array
		String[] featureColumns = {"marketing_spend", "bad_day"}; //revision version

//next thing we want to do is make the second col name features and put them in arrays. SO every row of feature col will contian an array of all the features (we have one feat in this example) use vector assembler
		VectorAssembler assembler = new VectorAssembler() // CTRL SHIFT O to import. set input and outtput cols
				.setInputCols(featureColumns) //input will be the array called feature columns
				.setOutputCol("features"); //setting output to be called features
		
//use the assembler to transform the df
		Dataset<Row> lblFeaturesDf = assembler.transform(mldf).select("label", "features"); // transform our new df. first col is label. second column will now be called features from the vector assembler
		
// ^^ this is correct form ML algorithm expects. New df is called `label features dataframe`.
//next thing we should do is to drop any rows with no values
		
		lblFeaturesDf = lblFeaturesDf.na().drop(); //this checks for nulls, then drops the nulls
		
		lblFeaturesDf.show();

//+------+---------+
//| label| features|
//+------+---------+
//|280000|[30000.0]|
//|279100|[40000.0]|
//|220000|[40000.0]|
//|168000|[27000.0]|
//|250200|[50000.0]|
//|382800|[60000.0]|
//|450400|[70000.0]|
//|412000|[70000.0]|
//|410500|[70000.0]|
//|450400|[80000.0]|
//|505300|[90000.0]|
//+------+---------+
//correct format! features comment is an array too.  NOTE - linear regression not going on yet we were just setting up DF to be compatible with ML libraries expect data to be

//creating the linear regression model object now
		LinearRegression lr = new LinearRegression(); 					//ensure to import. Instance of linear regression class. We can create actual model now
		LinearRegressionModel learningModel = lr.fit(lblFeaturesDf); 	//put the DF into the linear regression, this returns actual linear regression model
		
//many other ML models we'lll use but this is the structure. Create model, fit the data, ensure data is compatible with this kind of structure.
		learningModel.summary().predictions().show(); //grab the summary, we can invoke some statistical methods on it with .r2(). we also have .residuals() like how far it is away from a point. .mean() and mean abs error
		
//we want predictions, so use .predictions() to show the dataset

//+--------+---------+------------------+
//|   label| features|        prediction|
//+--------+---------+------------------+
//|280000.0|[30000.0]|211589.25864568225|
//|279100.0|[40000.0]|261461.92379374604|
//|220000.0|[40000.0]|261461.92379374604|
//|168000.0|[27000.0]| 196627.4591012631|
//|250200.0|[50000.0]|311334.58894180984|
//|382800.0|[60000.0]|361207.25408987363|
//|450400.0|[70000.0]| 411079.9192379374|
//|412000.0|[70000.0]| 411079.9192379374|
//|410500.0|[70000.0]| 411079.9192379374|
//|450400.0|[80000.0]| 460952.5843860012|
//|505300.0|[90000.0]|  510825.249534065|
//+--------+---------+------------------+
//so to explain what this means, for the first row, the model predicts 211K while ACTUAL value is 280K. Overall, pretty close even tho we dont have that much data

		System.out.println("R Squared: " + learningModel.summary().r2()); //89%, very good. Stands for 89 percent of variation of dependnet variable (sales) depends on indpendent variable (marketing bugdet)

//in other words, 89% of the variation in sales is accounted for by marketing budget which is high! Marketing budget goes up -> sales go up
//lets say we had a feature called bad day, 0 or 1 value. 1 represents bad day. We can have multi features and see how model changes.
//to update, we have to change the line that says select label marketing spend. We need to select the bad day col ALSO. We also add bad day to feature COlumns. both cols dumped into an array
	
//what model looks like now. We changed sales number too on the bad days to output literal AWFUL sales
//+---------------+-------+------+
//|marketing_spend|bad_day| sales|
//+---------------+-------+------+
//|          30000|      0|280000|
//|          40000|      1|     4|
//|          40000|      0|220000|
//|          27000|      0|168000|
//|          50000|      0|250200|
//|          60000|      1|     6|
//|          70000|      0|450400|
//|          70000|      1|     4|
//|          70000|      0|410500|
//|          80000|      0|450400|
//|          90000|      0|505300|
//+---------------+-------+------+
	
//after turning it into label and features. Remember, features has the marketing spent col AND the bad day col
//+------+-------------+
//| label|     features|
//+------+-------------+
//|280000|[30000.0,0.0]|
//|     4|[40000.0,1.0]|
//|220000|[40000.0,0.0]|
//|168000|[27000.0,0.0]|
//|250200|[50000.0,0.0]|
//|     6|[60000.0,1.0]|
//|450400|[70000.0,0.0]|
//|     4|[70000.0,1.0]|
//|410500|[70000.0,0.0]|
//|450400|[80000.0,0.0]|
//|505300|[90000.0,0.0]|
//+------+-------------+
		
//prediction output now
//+--------+-------------+------------------+
//|   label|     features|        prediction|
//+--------+-------------+------------------+
//|280000.0|[30000.0,0.0]|219542.67491860362|
//|     4.0|[40000.0,1.0]| -75145.7634908734|
//|220000.0|[40000.0,0.0]|264632.93301312765|
//|168000.0|[27000.0,0.0]| 206015.5974902464|
//|250200.0|[50000.0,0.0]| 309723.1911076516|
//|     6.0|[60000.0,1.0]|15034.752698174634|
//|450400.0|[70000.0,0.0]| 399903.7072966997|
//|     4.0|[70000.0,1.0]| 60125.01079269867|
//|410500.0|[70000.0,0.0]| 399903.7072966997|
//|450400.0|[80000.0,0.0]| 444993.9653912237|
//|505300.0|[90000.0,0.0]| 490084.2234857477|
//+--------+-------------+------------------+
//we can see we spent 30K (first row), we got 280K, predicted 220K not bad. Also not a bad day
//HOWEVER, in row 2, this was a bad day. We spent 40K on marketing, we ONLY received $4 in sales and the prediction is -75K wow! It works
//for the row with $6 sale, this was bad day, predicted 15k so model is aware bad days get lower sales. significantly lower than it would have been if we DID NOT have a bad day
//more data would make the model more accurate. model seems to be working. We are running the model on the actual data we have so model can be skewed. Not good training data. 
//need more data to prevent too much skewing. More data, better algorithm will perform over time
		
//R Squared: 93%! this is higher

		
	}
}
