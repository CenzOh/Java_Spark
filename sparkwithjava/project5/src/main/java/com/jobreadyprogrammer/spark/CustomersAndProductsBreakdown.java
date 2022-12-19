//Lesson 17 AND 18 my code
package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CustomersAndProductsBreakdown {
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder() //boilerplate
				.appName("Learning Spark SQL Dataframe API")
				.master("local")
				.getOrCreate();
		
//Dataframe number 1, customers
		String customers_file = "src/main/resources/customers.csv";
		
		Dataset<Row> customersDf = spark.read().format("csv")
				.option("interSchema", "true") //have to use string for true here
				.option("header", true)
				.load(customers_file);
		
//Dataframe number 2, products
		String products_file = "src/main/resources/products.csv";
		
		Dataset<Row> productsDf = spark.read().format("csv")
				.option("interSchema", "true") //have to use string for true here
				.option("header", true)
				.load(products_file);
		
//Dataframe number 3, purchases
		String purchases_file = "src/main/resources/purchases.csv";
		
		Dataset<Row> purchasesDf = spark.read().format("csv")
				.option("interSchema", "true") //have to use string for true here
				.option("header", true)
				.load(purchases_file);
		
//To Do: Inspect the three CSV files and UNDERSTAND the relationship
//Ok so I see customers have an ID and Products have an ID as well. THe purcahses DF is just the combo of customer ID and product ID
// Customers --> Purchases <-- Products
// So the way we would have to join them is via customers.customer_id with purchases.customer_id AND products.product_id with purchases.product_id
		
		System.out.println("***** Finished loading all files in the datafraems *******");
		
		Dataset<Row> joinedData = customersDf.join(purchasesDf, 
				customersDf.col("customer_id").equalTo(purchasesDf.col("customer_id"))) //will have joined customersDf and the purchases DF
				.join(productsDf, purchasesDf.col("product_id").equalTo(productsDf.col("product_id")))
		
				.drop("favorite_website").drop(purchasesDf.col("customer_id"))
				.drop(purchasesDf.col("product_id")).drop("product_id");

//when dropping a col that has the same name in TWO columns, make sure to specify WHICH df to drop the column from as seen with customer_id above
//as for product id, when we specify it the second time, spark would have already removed product id col coming from purchases df. so there would be on product id
		
		
		joinedData.show(); //will hagve all three types of data. Run this by hitting the play button on the top left
	
//OUTPUT before DROPPED we joined custoemr and purcahse df. Joined purchase wit product dataframe!! Relationship formed correctly, lets edit the values
//+-----------+---------+----------+--------------------+-----------+----------+----------+------------+-------------+
//|customer_id|last_name|first_name|    favorite_website|customer_id|product_id|product_id|product_name|product_price|
//+-----------+---------+----------+--------------------+-----------+----------+----------+------------+-------------+
//|       4000|  Jackson|       Joe|techonthenet.com ...|       4000|         3|         3|      Orange|         0.75|
//|       5000|    Smith|      Jane|digminecraft.com ...|       5000|         6|         6|  Sliced Ham|         3.00|
//|       5000|    Smith|      Jane|digminecraft.com ...|       5000|         7|         7|     Kleenex|         4.00|
//|       5000|    Smith|      Jane|digminecraft.com ...|       5000|         7|         7|     Kleenex|         4.00|
//|       5000|    Smith|      Jane|digminecraft.com ...|       5000|         6|         6|  Sliced Ham|         3.00|
//|       6000| Ferguson|  Samantha|bigactivities.com...|       6000|         6|         6|  Sliced Ham|         3.00|
//|       6000| Ferguson|  Samantha|bigactivities.com...|       6000|         3|         3|      Orange|         0.75|
//|       6000| Ferguson|  Samantha|bigactivities.com...|       6000|         1|         1|        Pear|         0.95|
//|       6000| Ferguson|  Samantha|bigactivities.com...|       6000|         1|         1|        Pear|         0.95|
//|       7000| Reynolds|     Allen|checkyourmath.com...|       7000|         2|         2|      Banana|         0.75|
//|       7000| Reynolds|     Allen|checkyourmath.com...|       7000|         1|         1|        Pear|         0.95|
//|       7000| Reynolds|     Allen|checkyourmath.com...|       7000|         1|         1|        Pear|         0.95|
//|       8000| Anderson|     Paige|                null|       8000|         3|         3|      Orange|         0.75|
//|       8000| Anderson|     Paige|                null|       8000|         3|         3|      Orange|         0.75|
//|       8000| Anderson|     Paige|                null|       8000|         2|         2|      Banana|         0.75|
//+-----------+---------+----------+--------------------+-----------+----------+----------+------------+-------------+
	
//OUTPUT after drop
//+-----------+---------+----------+------------+-------------+
//|customer_id|last_name|first_name|product_name|product_price|
//+-----------+---------+----------+------------+-------------+
//|       4000|  Jackson|       Joe|      Orange|         0.75|
//|       5000|    Smith|      Jane|  Sliced Ham|         3.00|
//|       5000|    Smith|      Jane|     Kleenex|         4.00|
//|       5000|    Smith|      Jane|     Kleenex|         4.00|
//|       5000|    Smith|      Jane|  Sliced Ham|         3.00|
//|       6000| Ferguson|  Samantha|  Sliced Ham|         3.00|
//|       6000| Ferguson|  Samantha|      Orange|         0.75|
//|       6000| Ferguson|  Samantha|        Pear|         0.95|
//|       6000| Ferguson|  Samantha|        Pear|         0.95|
//|       7000| Reynolds|     Allen|      Banana|         0.75|
//|       7000| Reynolds|     Allen|        Pear|         0.95|
//|       7000| Reynolds|     Allen|        Pear|         0.95|
//|       8000| Anderson|     Paige|      Orange|         0.75|
//|       8000| Anderson|     Paige|      Orange|         0.75|
//|       8000| Anderson|     Paige|      Banana|         0.75|
//+-----------+---------+----------+------------+-------------+
		
//aggregates, how can we compute the amount for each given product? We can do similar to SQL aggregates. FIrst lets groupby
//		joinedData.groupBy("first_name").count().show(); //first name and amount of purchases they made
//+----------+-----+
//|first_name|count|
//+----------+-----+
//|  Samantha|    4|
//|     Allen|    3|
//|       Joe|    1|
//|      Jane|    4|
//|     Paige|    3|
//+----------+-----+
		
//instead we can do .agg
		Dataset<Row> aggDf = joinedData.groupBy("first_name", "product_name").agg( //instantiate aggregates here
				count("product_name").as("number_of_purchases"), //counting the occurances of product name, renaming this new col with .as()
				max("product_price").as("most_expen_purchase"),
				sum("product_price").as("total_spent"));
//				).show(); //doing the show is the action. The count max sum are transformations.

//count, max, and sum come from this package: import static org.apache.spark.sql.functions.*;
//output before grouping with product_name
//+----------+-------------------+-------------------+-----------+
//|first_name|number_of_purchases|most_expen_purchase|total_spent|
//+----------+-------------------+-------------------+-----------+
//|  Samantha|                  4|               3.00|       5.65|
//|     Allen|                  3|               0.95|       2.65|
//|       Joe|                  1|               0.75|       0.75|
//|      Jane|                  4|               4.00|       14.0|
//|     Paige|                  3|               0.75|       2.25|
//+----------+-------------------+-------------------+-----------+

//if we want to groupby another column, just add a comma. .groupby("first_name", "product_name")
//OUTPUT after groupby product_name too
//+----------+------------+-------------------+-------------------+-----------+
//|first_name|product_name|number_of_purchases|most_expen_purchase|total_spent|
//+----------+------------+-------------------+-------------------+-----------+
//|     Paige|      Banana|                  1|               0.75|       0.75|
//|       Joe|      Orange|                  1|               0.75|       0.75|
//|      Jane|  Sliced Ham|                  2|               3.00|        6.0|
//|  Samantha|      Orange|                  1|               0.75|       0.75|
//|     Paige|      Orange|                  2|               0.75|        1.5|
//|     Allen|        Pear|                  2|               0.95|        1.9|
//|  Samantha|        Pear|                  2|               0.95|        1.9|
//|  Samantha|  Sliced Ham|                  1|               3.00|        3.0|
//|     Allen|      Banana|                  1|               0.75|       0.75|
//|      Jane|     Kleenex|                  2|               4.00|        8.0|
//+----------+------------+-------------------+-------------------+-----------+
//most exp purchase will be same for given products. An orange is still 75 cents. 
		
//lESSON 18 STARTS HERE
//SO remember that joining and dropping columns are TRANSFORMATIONS.
//The SHOW method is an ACTION
		aggDf.show();
		
		
	}

}
