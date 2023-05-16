
import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FSDataInputStream;

public class IMDBStudent20180250 {

	public static class IMDBMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1); 
		Text genre = new Text(); 
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String s = value.toString().replace("::", "~");
			String [] token = s.split("~"); 
			int len = token.length; 
			StringTokenizer itr = new StringTokenizer(token[len-1],"|");
			
			while (itr.hasMoreTokens()) 
			{
				genre.set(itr.nextToken());
				context.write(genre, one);
			}
		}//map
		
	} //mapper
	
	public static class IMDBReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0; 
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum); 
			context.write(key, result); 
		}//reduce 
		
	}//reducer 
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage : IMDB <in> <out>"); 
			System.exit(2);
		}
		
		Job job = new Job(conf, "IMDB");
		job.setJarByClass(IMDBStudent20180250.class); 
		job.setMapperClass(IMDBMapper.class);
		job.setCombinerClass(IMDBReducer.class); 
		job.setReducerClass(IMDBReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	} //main 

} //IMDB
