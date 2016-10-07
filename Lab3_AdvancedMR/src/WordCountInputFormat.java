import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

public class WordCountInputFormat extends FileInputFormat<NullWritable,Text>{


	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//System.out.println(split.toString()+" this is split");
		WordCountRecordReader reader = new WordCountRecordReader();
		reader.initialize(split, context);
		return reader;
	}

}
