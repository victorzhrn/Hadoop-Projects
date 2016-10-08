import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class IntPair implements WritableComparable<IntPair> {

	public IntWritable id, tag;

	public IntPair() {
		set(new IntWritable(), new IntWritable());
	}

	public IntPair(int x, int y) {
		set(new IntWritable(x), new IntWritable(y));
	}

	public void set(IntWritable x, IntWritable y) {
		this.id = x;
		this.tag = y;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		id.write(out);
		tag.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id.readFields(in);
		tag.readFields(in);
	}

	@Override
	public int compareTo(IntPair IP) {
		// TODO Auto-generated method stub
		System.out.println("inside map sort");
		int comp = id.compareTo(IP.id);
		if (comp != 0)
			return comp;
		comp = tag.compareTo(IP.tag);
		return comp;
	}

	public boolean equals(Object o) {
		if (o instanceof IntPair) {
			IntPair tp = (IntPair) o;
			return id.equals(tp.id) && tag.equals(tp.tag);
		}
		return false;
	}

	public static class FirstComparator extends WritableComparator {
		
		private static final IntWritable.Comparator myComparator = new IntWritable.Comparator();

		public FirstComparator() {
			super(IntPair.class);
			System.out.println("first comparator construct");
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			System.out.println("raw comparator get run");
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				return myComparator.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}

		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			System.out.println("comparision occur");
			if (a instanceof IntPair && b instanceof IntPair) {
				return ((IntPair) a).id.compareTo(((IntPair) b).id);
			}
			return super.compare(a, b);
		}

	}

	// static{
	// WritableComparator.define(IntPair.class, new FirstComparator());
	// }

}
