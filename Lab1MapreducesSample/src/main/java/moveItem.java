import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class moveItem {

	public static void main(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.err.println("incorrect input format");
			System.exit(-1);
		}
		String directory = args[0];
		String hdfs_dest = args[1];
		String local_dest = args[2];
		
		String c1 = "hadoop fs -put "+ directory +" "+ hdfs_dest;
		String c2 = "hadoop fs -get " + hdfs_dest + " "+ local_dest;
		
		Runtime.getRuntime().exec(c1);
		Runtime.getRuntime().exec(c2);
		
		
		
//		Configuration conf = new Configuration();
//		
//		String inputURI = args[0];
//		Path hdfsPath = new Path(args[1]);
//		FileSystem fs = FileSystem.get(URI.create(inputURI),conf);
//		OutputStream os = fs.create(hdfsPath);
//		InputStream in = new BufferedInputStream(new FileInputStream(inputURI));
//		IOUtils.copyBytes(in, os, 4096,true);
//		
//		fs.copyToLocalFile(false, hdfsPath, new Path(args[2]));
		
		
	}
}
