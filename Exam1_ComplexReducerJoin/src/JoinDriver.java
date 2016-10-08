import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JoinDriver {
	
	public static class GradeMapper extends Mapper<LongWritable, Text, TriText, LongWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			System.out.println("grade mapper run");
			String line = value.toString();
			String[] info = line.split(",");
			String name = info[0];
			String course = info[1];
			long score = Integer.parseInt(info[2]);
			context.write(new TriText(name,course,2), new LongWritable(score));
		}
	}
	
	public static class CourseMapper extends Mapper<LongWritable, Text, IDtag, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			System.out.println("course mapper run");
			String line = value.toString();
			String[] info = line.split(",");
			context.write(new IDtag(info[0],1), new Text(info[1]));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
	}

}
