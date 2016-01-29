

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<CustomKey,Text,CustomKey,Text> {

	public void reduce(CustomKey key, Iterable<Text> value,Context context) throws IOException,InterruptedException
	{
		Date maxDate=null;
		String temp=null;
		for(Text str: value){
			Date dateNow=null;
			String[] val=str.toString().split(",");
		    SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		    try {
				 dateNow = s.parse(val[0]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    if ((maxDate.compareTo(dateNow))<0 || maxDate.equals(dateNow)==true)
		    	maxDate=dateNow;
		    	temp=val[1];
		}
		
		context.write(key,new Text(maxDate.toString()+temp));
	}
}
		
	
			