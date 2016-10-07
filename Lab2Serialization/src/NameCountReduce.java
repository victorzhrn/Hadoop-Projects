

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NameCountReduce extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
	
	@Override
	public void reduce(TextPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		System.out.println("reducer run ");
		int result = 0;
		for (IntWritable val : values) {
			result += val.get();
		}
		context.write(key, new IntWritable(result));
	}
}