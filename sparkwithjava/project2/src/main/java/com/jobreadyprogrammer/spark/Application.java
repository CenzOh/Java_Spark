package com.jobreadyprogrammer.spark;

public class Application {

	public static void main(String[] args) {
		
//		InferCSVSchema parser = new InferCSVSchema();
//		parser.printSchema();
//First example, we want to print the schema of this CSV file. If you highlight InferCSVSchema, CTRL + Left click, you will go to the class
		
//		DefineCSVSchema parser2 = new DefineCSVSchema();
//		parser2.printDefinedSchema();
//Second example. Here, we refer to a class called DefineCSVSchema. We will manually define the schema. Before, we asked Spark to interpret the schema.
		
		JSONLinesParser parser3 = new JSONLinesParser();
		parser3.parseJsonLines();
//Third example is NOT a CSV file. This is reading JSON file.

	}

}
