//lecture 15 my code
package com.jobreadyprogrammer.mappers;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

public class LineMapperBreakdown implements FlatMapFunction<Row, String> {//remember, we need to implement the flat map function 
//each row is a collection of words, the ENTIRE line. When we split, each row based on whitespace will put each row into a collection of words and we use iteration method to treat each word as an elemetn

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L; //make it serializable

	@Override
	public Iterator<String> call(Row row) throws Exception { //how to parse row and turn it into an iterator of string
		return Arrays.asList(row.toString().split(" ")).iterator();
		
//so this will take each row, convert to string (incase it isnt already). Split the string based on white space (.split(" "))
//invoke the iterator method which returns iterator of type string
		
	} 
	
	
	
	
}
