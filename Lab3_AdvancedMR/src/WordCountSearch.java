import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountSearch {

	public static String search_word;

	public static class WordCountSearchMapper extends Mapper<NullWritable, Text, Text, IntWritable> {
		private Text filenameKey;

		enum Counting_Status {
			EqualToTwo, LessThanTwo, GreaterThanTwo;
		}

		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.toString());
			System.out.println("set up run and filename: " + filenameKey);
		}

		@Override
		public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			System.out.println("contents: "+line);
			int count = StringUtils.countMatches(line, search_word);
			System.out.println("after count run");
			System.out.println("number of Mary: "+count);
			context.write(filenameKey, new IntWritable(count));
			if (count < 2) {
				context.getCounter(Counting_Status.LessThanTwo).increment(1);
			} else if (count == 2) {
				context.getCounter(Counting_Status.EqualToTwo).increment(1);
			} else {
				context.getCounter(Counting_Status.GreaterThanTwo).increment(1);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		search_word = args[2];
		System.out.println("search word added  " + search_word);
		job.setJarByClass(WordCountSearch.class);
		job.setInputFormatClass(WordCountInputFormat.class);
		job.setMapperClass(WordCountSearchMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.waitForCompletion(true);

		long byte_count = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_BYTES")
				.getValue();
		System.out.println("map output bytes value = " + byte_count);

	}
}
