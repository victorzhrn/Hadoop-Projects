import org.apache.hadoop.io.*;
import org.apache.hadoop.hive.ql.exec.*;;

public class Strip extends UDF{
	public Text evaluate(Text str){
		if (str == null) return null;
		String original = str.toString();
		String cleaned = original.replaceAll("\\W", "");
		return new Text(cleaned.toUpperCase());
	}
}
