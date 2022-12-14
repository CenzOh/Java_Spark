//lecture 14, my code
package com.jobreadyprogrammer.mappers;

import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.jobreadyprogrammer.pojos.HouseBreakdown;

public class HouseMapperBreakdown implements MapFunction<Row, HouseBreakdown>{ //mapping EACH row coming from the file. Everything gets injested into a DF. Map to respect pojo property
//we need to implement the map function interface. Types are House type and Row
//map row of DF to the house POJO
//add unimplemetned methods
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public HouseBreakdown call(Row value) throws Exception {
//create new isntance of house
		HouseBreakdown h = new HouseBreakdown();
		
		h.setId(value.getAs("id")); //spark is smart enough to figure out that this is an int etc
		h.setAddress(value.getAs("address"));
		h.setSqft(value.getAs("sqft"));
		h.setPrice(value.getAs("price"));
		
		String vacancyDateString = value.getAs("vacantBy").toString();//get everythin as a string but we know its a string
//we dont want to assign a string to date field on the house, spark cant really figure out that this is supposed to be a date field
//also look at the file again, the date is YEAR-MONTH-DAY such as 2018-10-31
				
		if(vacancyDateString != null) { //check if the date string exists, if so do the followign to convert to date type
			SimpleDateFormat parser = new SimpleDateFormat("yyyy-mm-dd"); //CTRL SHIFT O to import simple datae format
			h.setVacantBy(parser.parse(vacancyDateString)); //vacancy date string will pass it out, assign as date to vacant by field

		}
		
		return h; //return the house
	} 
}
