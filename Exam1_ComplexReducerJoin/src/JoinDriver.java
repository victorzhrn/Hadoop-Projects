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

	public static class GradeMapper extends Mapper<LongWritable, Text, TriText, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// System.out.println("grade mapper run");
			String line = value.toString();

			String[] info = line.split(",");
			String name = info[0];
			String course = info[1];
			Text score = new Text(info[2]);

			//System.out.println(name + " " + course + " " + score);
			context.write(new TriText(name, course, 2), score);
		}
	}

	public static class CourseMapper extends Mapper<LongWritable, Text, TriText, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//System.out.println("course mapper run");
			String line = value.toString();
			String[] info = line.split(",");
			//System.out.println(info[0]);
			context.write(new TriText("", info[0], 1), new Text(info[1]));
		}
	}

	public static class JoinReducer extends Reducer<TriText, Text, Text, Text> {

		public void reduce(TriText key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("reducer run");

			String id = key.id.toString();
			Iterator<Text> iter = values.iterator();
			Text courseName = new Text(iter.next());
			// System.out.println(courseName);
			while (iter.hasNext()) {
				Text grade = iter.next();
				String name = key.name.toString();
				if(name!=""){
					// System.out.println("while loop run");
					Text outValue = new Text(courseName.toString() + "\t" + grade.toString());
					context.write(new Text(name + "\t" + id), outValue);
				}
			}

		}
	}

	public static class NameIDPartitioner extends Partitioner<TriText, Text> {

		@Override
		public int getPartition(TriText key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			return ((key.name.toString() + key.id.toString()).hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		Path gradeInput = new Path(args[0]);
		Path courseInput = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(job, gradeInput, TextInputFormat.class, GradeMapper.class);
		MultipleInputs.addInputPath(job, courseInput, TextInputFormat.class, CourseMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapOutputKeyClass(TriText.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(JoinReducer.class);
		job.setPartitionerClass(NameIDPartitioner.class);
		job.setGroupingComparatorClass(TriText.FirstComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}

}
