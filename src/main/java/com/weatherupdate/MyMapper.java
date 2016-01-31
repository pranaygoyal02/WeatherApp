
import java.io.IOException;
import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.weatherupdate.model.Pattern;

public class MyMapper extends Mapper<LongWritable, Text,CustomKey,Text>{
	
 public void map(LongWritable key,Text value,Context context)
 {
	 
	 String patternString = ",";
     Pattern pattern = Pattern.compile(patternString);
     
     String[] row = pattern.split(value);
     WeatherTable(row);
     
	 String[] myattr=value.toString().split(",",3);
	 try {
		context.write(new CustomKey(new IntWritable(Integer.parseInt(myattr[0])),new Text(myattr[1])),new Text(myattr[2]));
	} catch (NumberFormatException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
 }
		

}