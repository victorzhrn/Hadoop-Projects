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
	
	public static class JoinSprintMapper extends Mapper<LongWritable, Text, IntPair, Text>{
		public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] info = line.split(",");
			int id = Integer.parseInt(info[0]);
			context.write(new IntPair(id,1), new Text(info[1]));
		}	
	}
	
	public static class JoinWorkMapper extends Mapper<LongWritable, Text, IntPair, Text>{
		public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] info = line.split(",");
			int id = Integer.parseInt(info[2]);
			context.write(new IntPair(id,2), new Text(info[0]+" "+info[1]));
		}
	}
	
	public static class JoinReducer extends Reducer<IntPair, Text , IntWritable, Text>{
		
		public void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			System.out.println("reducer run");
			Iterator<Text> iter = values.iterator();
			Text className = new Text(iter.next());
			while(iter.hasNext()){
				Text instructor = iter.next();
				Text outValue = new Text(className.toString()+"\t"+instructor.toString());
				context.write(key.id, outValue);
			}
		}
	}
	
	public static class IDPartitioner extends Partitioner<IntPair,Text>{
		@Override
		public int getPartition(IntPair key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			System.out.println("get partition run");
			return ((key.id.hashCode() & Integer.MAX_VALUE) % numPartitions);
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(JoinDriver.class);
		
		Path classInput = new Path(args[0]);
		Path instructorInput = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		MultipleInputs.addInputPath(job, classInput ,TextInputFormat.class, JoinSprintMapper.class);
		MultipleInputs.addInputPath(job, instructorInput, TextInputFormat.class, JoinWorkMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(JoinReducer.class);
		//job.setSortComparatorClass(IntPair);
		
		job.setPartitionerClass(IDPartitioner.class);
		job.setGroupingComparatorClass(IntPair.FirstComparator.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
	}
}
