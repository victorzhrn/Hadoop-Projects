import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.*;


public class IDtag implements WritableComparable<IDtag>{
	
	public Text id;
	public IntWritable tag;
	
	public IDtag(){
		set(new Text(), new IntWritable());
	}
	
	public IDtag(String s, int i){
		set(new Text(s), new IntWritable(i));
	}
	
	public void set(Text id, IntWritable tag){
		this.id = id;
		this.tag = tag;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		id.write(out);
		tag.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id.readFields(in);
		tag.readFields(in);
	}

	@Override
	public int compareTo(IDtag o) {
		// TODO Auto-generated method stub
		int comp = id.compareTo(o.id);
		if (comp!=0) return comp;
		comp = tag.compareTo(o.tag);
		return comp;
	}
	

}
