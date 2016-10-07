import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class TextsIntPair implements WritableComparable<TextsIntPair> {
	private Text first, second;
	private IntWritable number;
	
	public TextsIntPair(){
		first = new Text();
		second = new Text();
		number = new IntWritable();
	}

	public TextsIntPair(Text a, Text b, IntWritable c) {
		first = a;
		second = b;
		number = c;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
		number.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
		number.readFields(in);
	}

	@Override
	public int compareTo(TextsIntPair o) {
		// TODO Auto-generated method stub
		int compare = first.compareTo(o.first);
		if (compare !=0){
			return compare;
		}
		compare = second.compareTo(o.second);
		if (compare!=0){
			return compare;
		}
		return number.compareTo(o.number);
	}
	
	public String toString(){
		return first.toString()+" "+ second.toString()+" "+number.toString();
	}
}
