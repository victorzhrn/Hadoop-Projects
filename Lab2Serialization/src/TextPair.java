
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair> {

	private Text firstName;
	private Text lastName;

	public TextPair() {
		set(new Text(), new Text());
	}

	public TextPair(String first, String last) {
		
		set(new Text(first), new Text(last));
	}

	public TextPair(Text first, Text last) {
		set(first, last);
	}

	public void set(Text first, Text last) {
		firstName = first;
		lastName = last;
	}

//	public Text getFirst() {
//		return firstName;
//	}
//
//	public Text getLast() {
//		return lastName;
//	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		firstName.readFields(in);
		lastName.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		firstName.write(out);
		lastName.write(out);
	}

//	public int hashCode() {
//		return firstName.hashCode() * 163 + lastName.hashCode();
//	}

//	public boolean equals(Object o) {
//		if (o instanceof TextPair) {
//			TextPair tp = (TextPair) o;
//			return firstName.equals(tp.firstName)
//					&& lastName.equals(tp.lastName);
//		}
//		return false;
//	}

	@Override
	public String toString() {
		return firstName + "\t" + lastName;
	}

	public int compareTo(TextPair tp) {
		int cmp = firstName.compareTo(tp.firstName);
		if (cmp != 0) {
			return cmp;
		}
		return lastName.compareTo(tp.lastName);
	}

}
