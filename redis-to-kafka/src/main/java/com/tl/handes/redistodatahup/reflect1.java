package com.tl.handes.redistodatahup;

public class reflect1 {
	
	private int id;
	private String name;
	public reflect1(){};
	public reflect1(String name1,int id1){
		 this.id = id1;
		this.name = name1;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	
	
	
	public void add(int a ,int b){
		System.out.println(a+b);
	}
}
