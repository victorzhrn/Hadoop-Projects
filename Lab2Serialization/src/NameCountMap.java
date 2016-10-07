


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

 public class NameCountMap extends Mapper<LongWritable, Text, TextPair,IntWritable>{
	
	 public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
	 {
			System.out.println("NameCountmap run");
			String line = value.toString();
			String[] fullname = line.split(",");
			TextPair TP = new TextPair(fullname[0], fullname[1]);
			context.write(TP, new IntWritable(1));
		}
	 }

}
