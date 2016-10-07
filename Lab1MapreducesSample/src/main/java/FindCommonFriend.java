import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.StatisticalMultivariateSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Sets;

public class FindCommonFriend {

	public static class FCFmapper extends
			Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			System.out.println("mapper run");
			
			String line = value.toString();
			String[] char_list = line.split(",");
			String subject = char_list[0];
			HashSet<String> set = Sets.newHashSet(char_list);
			set.remove(subject);
			for (String object:set){
				HashSet<String> valueSet = Sets.newHashSet(set);
				valueSet.remove(object);
				
				
				HashSet<String> keySet = Sets.newHashSet(subject,object);
				Text tkey = new Text(keySet.toString());
				Text vkey = new Text(valueSet.toString());
				System.out.println(tkey +":  "+vkey);
				context.write(tkey, vkey);
			}
		}
	}
	
	public static class FCFreducer extends Reducer<Text, Text, Text, Text>{


		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			System.out.println("reducer run");
			HashSet<String> resultSet = new HashSet<String>();
			
			int count=0;
			for (Text value : values){
				String valueString = value.toString();
				count++;
				valueString = valueString.substring(1, valueString.length()-1);
				System.out.println(valueString);
				HashSet<String> valueset = Sets.newHashSet(valueString.split(", "));
				if (resultSet.isEmpty())  resultSet.addAll(valueset);
				else{
					for (String friend : resultSet){
						if (!valueset.contains(friend)) resultSet.remove(friend);
					}
				}			
			}
			if (count>=2) context.write(new Text(key), new Text(resultSet.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJobName("Find Common Friend");
		job.setJarByClass(FindCommonFriend.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(FCFmapper.class);
		job.setReducerClass(FCFreducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}
