import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;

public class TriText implements WritableComparable<TriText> {

	public Text name, id;
	public IntWritable tag;

	public TriText() {
		set(new Text(), new Text(), new IntWritable());
	}

	public TriText(String n, String c, int t) {
		set(new Text(n), new Text(c), new IntWritable(t));
	}

	public void set(Text n, Text i, IntWritable t) {
		name = n;
		id = i;
		tag = t;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		id.write(out);
		name.write(out);
		tag.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id.readFields(in);
		name.readFields(in);
		tag.readFields(in);
	}

	@Override
	public int compareTo(TriText o) {
		// TODO Auto-generated method stub
		//System.out.println("normal comparator run");
		int comp = id.compareTo(o.id);
		if (comp != 0)
			return comp;
		comp = name.compareTo(o.name);
		if (comp != 0)
			return comp;
		return tag.compareTo(o.tag);
	}

	public static class FirstComparator extends WritableComparator {

		private static final Text.Comparator myComparator = new Text.Comparator();
		private int count;

		public FirstComparator() {
			super(TriText.class);
			//System.out.println("initialize");
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// System.out.println("raw comparator get run");
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				//System.out.println("in raw comparator:  "+ new String(Arrays.copyOfRange(b1, s1, s1+firstL1)));
				return myComparator.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}

		}

	}

}
