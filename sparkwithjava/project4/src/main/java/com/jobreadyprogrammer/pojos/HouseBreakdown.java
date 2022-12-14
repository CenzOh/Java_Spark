//lecture 14 my code
package com.jobreadyprogrammer.pojos; //make sure to look at the houses.csv so we know what variables to use

import java.util.Date;

public class HouseBreakdown {
	
	private int id;
	private String address;
	private int sqft;
	private double price;
	private Date vacantBy;  // CTRL SHIFT O to import. Date field need to make sure we map correctly
	
	
	
	public int getId() { //right click > Source > generate getters and setters for all
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public int getSqft() {
		return sqft;
	}
	public void setSqft(int sqft) {
		this.sqft = sqft;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}
	public Date getVacantBy() {
		return vacantBy;
	}
	public void setVacantBy(Date vacantBy) {
		this.vacantBy = vacantBy;
	}

}
