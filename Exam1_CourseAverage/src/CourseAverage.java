import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CourseAverage {
	
	public static class CourseAvgMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] info = line.split("\t");
			float score = Float.parseFloat(info[3]);
			context.write(new Text(info[1]),new FloatWritable(score));
		}
	}
	
	public static class CourseAvgReducer extends Reducer<Text,FloatWritable,Text, FloatWritable>{
		public void reduce(Text key,Iterable<FloatWritable> values, Context context ) throws IOException, InterruptedException{
			int count = 0;
			float sum = 0;
			for (FloatWritable val : values){
				sum+=val.get();
				count++;
			}
			context.write(key, new FloatWritable(sum/count));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(CourseAverage.class);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(CourseAvgMapper.class);
		job.setReducerClass(CourseAvgReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.waitForCompletion(true);
		
	}

}
