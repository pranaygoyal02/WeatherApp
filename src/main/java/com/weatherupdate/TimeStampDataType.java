




import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

public class TimeStampDataType implements WritableComparable{
	
	private TimestampWritable t1;
	private IntWritable   t2;
	
	TimeStampDataType() 
	{
	this.t1=new TimestampWritable();
	this.t2=new IntWritable ();
	}
	
	TimeStampDataType(TimestampWritable t1 ,IntWritable  t2)
	{
		this.t1=t1;
		this.t2=t2;
	}
	

		
	

public TimestampWritable gett1() {
		return t1;
	}

	public void sett1(IntWritable t1) {
		t1 = t1;
	}

	public IntWritable gett2() {
		return t2;
	}

	public void sett2(Text t2) {
		t2 = t2;
	}

public void readFields(DataInput dataInput) throws IOException {
		
		// TODO Auto-generated method stub
		t1.readFields(dataInput);
		t2.readFields(dataInput);
		
		
		
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method s
		t1.write(dataOutput);
		t2.write(dataOutput);
	}

	@Override
	public int compareTo(Object obj) {
		// TODO Auto-generated method stub
		TimeStampDataType myobj=(TimeStampDataType)obj;
		return (t1.compareTo(myobj.t1));
		
			
	
	}

	
	

}
