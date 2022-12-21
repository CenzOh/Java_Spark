//lecture 31 my code
package com.jobreadyprogrammer.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KmeansClusteringBreakdown {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkSession spark = new SparkSession.Builder()
				.appName("kmeans Clustering")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> wholeSaleDf = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.format("csv")
				.load("C:\\Users\\Cenzo Oh\\JavaSpark\\workspace\\sparkwithjava\\project9\\data\\wholesale_customers_data.csv");
		
//what is in the csv? annual spending beased on different categories like fresh, milk, grocery,f rozen. we want to group them. 400 records. There is a region col already but we'll ignore it and run as unsupervised algorithm
		wholeSaleDf.show();
//+-------+------+-----+-----+-------+------+----------------+----------+
//|Channel|Region|Fresh| Milk|Grocery|Frozen|Detergents_Paper|Delicassen|
//+-------+------+-----+-----+-------+------+----------------+----------+
//|      2|     3|12669| 9656|   7561|   214|            2674|      1338|
//|      2|     3| 7057| 9810|   9568|  1762|            3293|      1776|
//|      2|     3| 6353| 8808|   7684|  2405|            3516|      7844|
//|      1|     3|13265| 1196|   4221|  6404|             507|      1788|
//|      2|     3|22615| 5410|   7198|  3915|            1777|      5185|
//|      2|     3| 9413| 8259|   5126|   666|            1795|      1451|
//|      2|     3|12126| 3199|   6975|   480|            3140|       545|
//|      2|     3| 7579| 4956|   9426|  1669|            3321|      2566|
//|      1|     3| 5963| 3648|   6192|   425|            1716|       750|
//|      2|     3| 6006|11093|  18881|  1159|            7425|      2098|
//|      2|     3| 3366| 5403|  12974|  4400|            5977|      1744|
//|      2|     3|13146| 1124|   4523|  1420|             549|       497|
//|      2|     3|31714|12319|  11757|   287|            3881|      2931|
//|      2|     3|21217| 6208|  14982|  3095|            6707|       602|
//|      2|     3|24653| 9465|  12091|   294|            5058|      2168|
//|      1|     3|10253| 1114|   3821|   397|             964|       412|
//|      2|     3| 1020| 8816|  12121|   134|            4508|      1080|
//|      1|     3| 5876| 6157|   2933|   839|             370|      4478|
//|      2|     3|18601| 6327|  10099|  2205|            2767|      3181|
//|      1|     3| 7780| 2495|   9464|   669|            2518|       501|
//+-------+------+-----+-----+-------+------+----------------+----------+
//only showing top 20 rows
		
//select cols we are interested in our features
		Dataset<Row> featuresDf = wholeSaleDf.select("channel", "fresh", "milk", "grocery", "frozen", "detergents_paper", "delicassen"); //leave out region
		
//assembler now
		VectorAssembler assembler = new VectorAssembler(); //.import me
		assembler.setInputCols(new String [] {"channel", "fresh", "milk", "grocery", "frozen", "detergents_paper", "delicassen"}) //accepts string
				 .setOutputCol("features"); //set the output to features
		
		Dataset<Row> trainingData = assembler.transform(featuresDf).select("features"); //dont need to split to training testing cuz we dont have a label col to work with
		
//k means here
		KMeans kmeans = new KMeans(); //import me
		kmeans.setK(10); //(3); //how many clusters do we want? We'll try three. 
//note could instantiate everything on one line like this KMeans kmean = new KMeans().setK(3);
		KMeansModel model = kmeans.fit(trainingData); //fit the feature data. import me too
		
		System.out.println(model.computeCost(trainingData)); //determines how well the centroid was palced in cluster. want min distance between centroids and resto of data points in the three clusters
		model.summary().predictions().show(); //which particualr record should go and which region in cluster

//8.03332656909754E10 //sum of square errors
//+--------------------+----------+
//|            features|prediction|
//+--------------------+----------+
//|[2.0,12669.0,9656...|         1|
//|[2.0,7057.0,9810....|         1|
//|[2.0,6353.0,8808....|         1|
//|[1.0,13265.0,1196...|         1|
//|[2.0,22615.0,5410...|         2|
//|[2.0,9413.0,8259....|         1|
//|[2.0,12126.0,3199...|         1|
//|[2.0,7579.0,4956....|         1|
//|[1.0,5963.0,3648....|         1|
//|[2.0,6006.0,11093...|         0|
//|[2.0,3366.0,5403....|         1|
//|[2.0,13146.0,1124...|         1|
//|[2.0,31714.0,1231...|         2|
//|[2.0,21217.0,6208...|         1|
//|[2.0,24653.0,9465...|         2|
//|[1.0,10253.0,1114...|         1|
//|[2.0,1020.0,8816....|         1|
//|[1.0,5876.0,6157....|         1|
//|[2.0,18601.0,6327...|         1|
//|[1.0,7780.0,2495....|         1|
//+--------------------+----------+
//only showing top 20 rows
		
//what if we made k to 10 / 10 clusters / 10 centroids? cost fcn will reduce with sum of square errors
//3.494449570241699E10 //wow we got three now!!
//+--------------------+----------+
//|            features|prediction|
//+--------------------+----------+
//|[2.0,12669.0,9656...|         7|
//|[2.0,7057.0,9810....|         5|
//|[2.0,6353.0,8808....|         5|
//|[1.0,13265.0,1196...|         7|
//|[2.0,22615.0,5410...|         7|
//|[2.0,9413.0,8259....|         5|
//|[2.0,12126.0,3199...|         7|
//|[2.0,7579.0,4956....|         5|
//|[1.0,5963.0,3648....|         5|
//|[2.0,6006.0,11093...|         0|
//|[2.0,3366.0,5403....|         0|
//|[2.0,13146.0,1124...|         7|
//|[2.0,31714.0,1231...|         8|
//|[2.0,21217.0,6208...|         7|
//|[2.0,24653.0,9465...|         8|
//|[1.0,10253.0,1114...|         5|
//|[2.0,1020.0,8816....|         0|
//|[1.0,5876.0,6157....|         5|
//|[2.0,18601.0,6327...|         7|
//|[1.0,7780.0,2495....|         5|
//+--------------------+----------+
//only showing top 20 rows
	}
}
