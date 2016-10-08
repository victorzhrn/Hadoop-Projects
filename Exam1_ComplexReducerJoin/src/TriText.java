import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.*;

public class TriText implements WritableComparable<TriText>{
	
	public Text name;
	public IDtag idt;
	
	public TriText(){
		set(new Text(), new IDtag());
	}
	
	public TriText(String n, String c, int t){
		set(new Text(n), new IDtag(c,t));
	}
	
	public void set(Text n, IDtag i){
		name = n;
		idt = i;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		name.write(out);
		idt.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		name.readFields(in);
		idt.readFields(in);
	}

	@Override
	public int compareTo(TriText o) {
		// TODO Auto-generated method stub
		int comp = idt.id.compareTo(o.idt.id);
		if (comp!=0) return comp;
		comp =name.compareTo(o.name);
		if (comp!=0) return comp;
		return idt.tag.compareTo(o.idt.tag);
	}

}
