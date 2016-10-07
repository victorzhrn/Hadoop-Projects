import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NameSprintCount {
	
	public static class Map extends Mapper<LongWritable, Text, TextsIntPair, DoubleWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			System.out.println("mapper run");
			String line = value.toString();
			String[] info = line.split(",");
			
			Text firstName = new Text(info[0]);
			System.out.println(firstName);
			Text lastName = new Text(info[1]);
			System.out.println(lastName);
			IntWritable sprint = new IntWritable(Integer.parseInt(info[2]));
			System.out.println(sprint.get());
			System.out.println(info[3]);
			
			DoubleWritable hours = new DoubleWritable(Double.parseDouble(info[3]));
			System.out.println(hours.get());
			context.write(new TextsIntPair(firstName, lastName,sprint), hours);
		}
	}
	
	public static class Reduce extends Reducer<TextsIntPair, DoubleWritable, TextsIntPair, DoubleWritable>{
		
		public void reduce(TextsIntPair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			System.out.println("reducer run");
			long sum = 0;
			for (DoubleWritable val : values){
				sum+=val.get();
			}
			context.write(key, new DoubleWritable(sum));
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		System.out.println("job initialized");
		job.setJarByClass(NameSprintCount.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(TextsIntPair.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(TextsIntPair.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.waitForCompletion(true);
		
	}
}
