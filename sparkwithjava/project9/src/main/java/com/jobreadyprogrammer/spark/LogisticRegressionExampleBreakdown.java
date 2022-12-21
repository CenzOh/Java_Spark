//lecture 29 my code
package com.jobreadyprogrammer.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LogisticRegressionExampleBreakdown {
	
	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkSession spark = new SparkSession.Builder()
				.appName("LogisticRegressionExample")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> treatmentDf = spark.read()
				.option("header",  "true")
				.option("inferSchema",  "true")
				.format("csv")
				.load("C:\\Users\\Cenzo\\JavaSpark\\workspace\\sparkwithjava\\project9\\data\\cryotherapy.csv"); //have the full file in this pathS
//above is pretty much boilerplate
//csv is from https://archive.ics.uci.edu/ml/datasets.Cryotherapy+Dataset+#
//we will be using this dataset to classify if pateints have been treated sucessfully or not. we have 90 records. This is a classification algorithm remember, true or false binary option

//our data has their sex, age, time they were undergoing treatment, num of warts this treatment eliminates warts, type of treatemetn, area wher it was treated, and finally result of treatement, success of fail.
//result is the label, all the other cols are features. Difference between this and the other cols is that the first col, sex, is actually a cateogrical col NOT numerical.
//for log reg to work, we have to turn this col into a numerical value. 
		
		treatmentDf.show(); //just to show what it looks like so far. remember, result of treatment is label col rest are features
	
//+------+---+-----+---------------+----+----+-------------------+
//|   sex|age| Time|Number_of_Warts|Type|Area|Result_of_Treatment|
//+------+---+-----+---------------+----+----+-------------------+
//|  MALE| 35| 12.0|              5|   1| 100|                  0|
//|  MALE| 29|  7.0|              5|   1|  96|                  1|
//|  MALE| 50|  8.0|              1|   3| 132|                  0|
//|  MALE| 32|11.75|              7|   3| 750|                  0|
//|  MALE| 67| 9.25|              1|   1|  42|                  0|
//|  MALE| 41|  8.0|              2|   2|  20|                  1|
//|  MALE| 36| 11.0|              2|   1|   8|                  0|
//|  MALE| 59|  3.5|              3|   3|  20|                  0|
//|  MALE| 20|  4.5|             12|   1|   6|                  1|
//|FEMALE| 34|11.25|              3|   3| 150|                  0|
//|FEMALE| 21|10.75|              5|   1|  35|                  0|
//|FEMALE| 15|  6.0|              2|   1|  30|                  1|
//|FEMALE| 15|  2.0|              3|   1|   4|                  1|
//|FEMALE| 15| 3.75|              2|   3|  70|                  1|
//|FEMALE| 17| 11.0|              2|   1|  10|                  0|
//|FEMALE| 17| 5.25|              3|   1|  63|                  1|
//|FEMALE| 23|11.75|             12|   3|  72|                  0|
//|FEMALE| 27| 8.75|              2|   1|   6|                  0|
//|FEMALE| 15| 4.25|              1|   1|   6|                  1|
//|FEMALE| 18| 5.75|              1|   1|  80|                  1|
//+------+---+-----+---------------+----+----+-------------------+
//only showing top 20 rows
		
		Dataset<Row> lblFeatureDf = treatmentDf.withColumnRenamed("Result_of_Treatment", "label") //setting the label
			.select("label", "sex", "age", "Time", "Number_of_Warts", "Type", "Area"); //reordering so label is first
		
//check for nulls and drop em
		lblFeatureDf.na().drop();
		
		lblFeatureDf.show(); //what it looks like now
//+-----+------+---+-----+---------------+----+----+
//|label|   sex|age| Time|Number_of_Warts|Type|Area|
//+-----+------+---+-----+---------------+----+----+
//|    0|  MALE| 35| 12.0|              5|   1| 100|
//|    1|  MALE| 29|  7.0|              5|   1|  96|
//|    0|  MALE| 50|  8.0|              1|   3| 132|
//|    0|  MALE| 32|11.75|              7|   3| 750|
//|    0|  MALE| 67| 9.25|              1|   1|  42|
//...		
		
		StringIndexer genderIndexer = new StringIndexer() 				//make sure to import
				.setInputCol("sex").setOutputCol("sexIndex"); 			//numeric representation of the cateogry sex. Male 0, female 1
		
		VectorAssembler assembler = new VectorAssembler() //import this guy too
				.setInputCols(new String[] {"sexIndex", "age", "Time", "Number_of_Warts", "Type", "Area"}) //we changed sex col to sexindex so write that in then write inr est of cols
				.setOutputCol("features"); 								//wait, how does assemlber know sexIndex exists in genderIndexer? We will fix this with pipeline shortly
		
//training and testing datasets
		Dataset<Row> [] splitData = lblFeatureDf.randomSplit(new double[] {.7, .3}); //70% data is training, 30% is testing. contains two dataframes
		Dataset<Row> trainingDf = splitData[0]; 						//first part is the training
		Dataset<Row> testingDf = splitData[1]; 							//and of course this df will be testing
		
//instance of log reg
		LogisticRegression logReg = new LogisticRegression(); 			//make sure to import
		Pipeline pl = new Pipeline(); //import too
		
		pl.setStages(new PipelineStage [] {genderIndexer, assembler, logReg }); //setstages accepts pipleine stage array. import pipelinestage. 
//this pl auto knos how to set indexer, assembler, and run data on log reg
//fit training data now
		
		PipelineModel model = pl.fit(trainingDf); 						//import pipeline model
		Dataset<Row> results = model.transform(testingDf); 				//this returns the results has label and prediction
		results.show();
		
//+-----+------+---+-----+---------------+----+----+--------+--------------------+--------------------+--------------------+----------+
//|label|   sex|age| Time|Number_of_Warts|Type|Area|sexIndex|            features|       rawPrediction|         probability|prediction|
//+-----+------+---+-----+---------------+----+----+--------+--------------------+--------------------+--------------------+----------+
//|    0|FEMALE| 17| 11.0|              2|   1|  10|     1.0|[1.0,17.0,11.0,2....|[0.18951050797151...|[0.54723634013232...|       0.0|
//|    0|FEMALE| 21|10.75|              5|   1|  35|     1.0|[1.0,21.0,10.75,5...|[0.72017647870772...|[0.67264587769852...|       0.0|
//|    0|FEMALE| 23|11.75|             12|   3|  72|     1.0|[1.0,23.0,11.75,1...|[4.50457870306455...|[0.98906269937130...|       0.0|
//|    0|FEMALE| 32| 12.0|              4|   3| 750|     1.0|[1.0,32.0,12.0,4....|[2.40193181537759...|[0.91697449551616...|       0.0|
//|    0|FEMALE| 34| 12.0|              3|   3|  95|     1.0|[1.0,34.0,12.0,3....|[4.88480103866439...|[0.99249610626320...|       0.0|
//|    0|FEMALE| 34| 12.0|              3|   3|  95|     1.0|[1.0,34.0,12.0,3....|[4.88480103866439...|[0.99249610626320...|       0.0|
//|    0|FEMALE| 35| 8.25|              8|   3| 100|     1.0|[1.0,35.0,8.25,8....|[2.73907161214542...|[0.93929318019956...|       0.0|
//|    0|FEMALE| 36| 10.5|              4|   1|   8|     1.0|[1.0,36.0,10.5,4....|[2.27826747793292...|[0.90706109604975...|       0.0|
//|    0|FEMALE| 40| 8.75|              6|   2|  80|     1.0|[1.0,40.0,8.75,6....|[2.47501061990578...|[0.92237129785858...|       0.0|
//|    0|FEMALE| 50|  9.5|              4|   3| 132|     1.0|[1.0,50.0,9.5,4.0...|[4.86970935726995...|[0.99238287018987...|       0.0|
//|    0|  MALE| 23|10.25|              7|   3|  72|     0.0|[0.0,23.0,10.25,7...|[1.93379584164659...|[0.87366896720258...|       0.0|
//|    0|  MALE| 29|11.75|              5|   1|  96|     0.0|[0.0,29.0,11.75,5...|[1.30564761571432...|[0.78678393311990...|       0.0|
//|    0|  MALE| 34| 12.0|              1|   3| 150|     0.0|[0.0,34.0,12.0,1....|[3.57796556233019...|[0.97282655403929...|       0.0|
//|    0|  MALE| 50|  8.0|              1|   3| 132|     0.0|[0.0,50.0,8.0,1.0...|[2.52458744161443...|[0.92584761608082...|       0.0|
//|    0|  MALE| 67| 9.25|              1|   1|  42|     0.0|[0.0,67.0,9.25,1....|[3.63703955377591...|[0.97434531465308...|       0.0|
//|    1|FEMALE| 15|  8.0|             12|   1|  30|     1.0|[1.0,15.0,8.0,12....|[-1.2364598932705...|[0.22505279361479...|       1.0|
//|    1|FEMALE| 19|  8.0|              9|   1| 160|     1.0|[1.0,19.0,8.0,9.0...|[-1.5739606025329...|[0.17165250931055...|       1.0|
//|    1|FEMALE| 22|  4.5|              2|   1|  70|     1.0|[1.0,22.0,4.5,2.0...|[-4.3091648159645...|[0.01326640988724...|       1.0|
//|    1|FEMALE| 22|  5.0|              9|   1|  70|     1.0|[1.0,22.0,5.0,9.0...|[-3.1447845916209...|[0.04129727237459...|       1.0|
//|    1|FEMALE| 28|  4.0|             11|   1| 100|     1.0|[1.0,28.0,4.0,11....|[-3.0720370818814...|[0.04427554794574...|       1.0|
//+-----+------+---+-----+---------------+----+----+--------+--------------------+--------------------+--------------------+----------+
//only showing top 20 rows
		
//take note that this training model is only in label col and features col, did not use original cols. prediction is at the end.
//most of the labels are 0, most predictions are also 0
		
		
	}
}