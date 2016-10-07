import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxTemperature {

	public static class MaxTemperatureReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			
			System.out.println(maxValue);
			context.write(key, new IntWritable(maxValue));
		}
	}

	public static class MaxTemperatureMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static final int missing = 9999; // the log store temperature =
													// 9999 when missing

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("in Map");
			String line = value.toString();
			String year = line.substring(15, 19);
			int airTemperature;
			if (line.charAt(87) == '+') {
				airTemperature = Integer.parseInt(line.substring(88, 92));
			} else {
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}

			if (airTemperature != missing) {
				System.out.println(year);
				context.write(new Text(year), new IntWritable(airTemperature));
			}
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
		job.setJarByClass(MaxTemperature.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.out.println("trying set map and reduce");
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}

}
