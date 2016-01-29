package com.weatherupdate;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable{
	
	private IntWritable store;
	private Text  countryCode ;
	
	CustomKey() 
	{
	this.store=new IntWritable();
	this.countryCode=new Text();
	}
	
	CustomKey(IntWritable store ,Text countryCode)
	{
		this.store=store;
		this.countryCode=countryCode;
	}
	

		
	

public IntWritable getStore() {
		return store;
	}

	public void setStore(IntWritable store) {
		store = store;
	}

	public Text getcountryCode() {
		return countryCode;
	}

	public void setcountryCode(Text countryCode) {
		countryCode = countryCode;
	}

public void readFields(DataInput dataInput) throws IOException {
		
		// TODO Auto-generated method stub
		store.readFields(dataInput);
		countryCode.readFields(dataInput);
		
		
		
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method s
		store.write(dataOutput);
		countryCode.write(dataOutput);
	}

	@Override
	public int compareTo(Object obj) {
		// TODO Auto-generated method stub
		CustomKey myobj=(CustomKey)obj;
		int result = store.compareTo(myobj.store);
		if (result == 0)
			return countryCode.compareTo(myobj.countryCode);	
		else
			return result;
	
	}

	
	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof CustomKey)
		{
			CustomKey c=(CustomKey)obj;
			return store.equals(c.store) && countryCode.equals(c.countryCode); 	
		}
		return false;
	
	}
	}

