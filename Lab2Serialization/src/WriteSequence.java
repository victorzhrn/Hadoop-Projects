import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Writer;


public class WriteSequence {
	
	public static void main(String[] args) throws IOException{
		String dir = args[0];
		String output = args[1];
		Path path = new Path(dir);
		Configuration conf = new Configuration();
		
		Text key = new Text();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		
		File folder = new File(dir);
		File[] listOfFiles = folder.listFiles();
		
		String seq = "output.seq";
		
		try{
			writer = SequenceFile.createWriter(conf,Writer.file(new Path(output)),Writer.keyClass(key.getClass()),Writer.valueClass(value.getClass()));
			for (File f : listOfFiles){
				key.set(f.getName());
				System.out.println(f.getPath());
				System.out.println(f.getName());
				
				FileInputStream fis =new FileInputStream(f);
				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
				String line;
				while((line=br.readLine())!=null){
					value.set(line);
					System.out.println("line value:  "+line);
					System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
					writer.append(key, value);
				}
			}
			
		}finally{
			IOUtils.closeStream(writer);
		}
		
	}
}
