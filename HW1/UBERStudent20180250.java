import java.io.IOException;
import java.util.*;
import java.io.*;
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FSDataInputStream;

public class UBERStudent20180250 {

	public static class UBERMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text BaseNumber = new Text(); //key
		private Text ActiveVehicles = new Text(); //value
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			Date date = null;
			SimpleDateFormat df = new SimpleDateFormat("mm/dd/yyyy"); 
			
			Calendar cal = Calendar.getInstance();
			
			StringTokenizer itr = new StringTokenizer(value.toString(),",");
			int dayNum = 0;

			while (itr.hasMoreTokens()) {
				
				String Basenum = itr.nextToken();
				try { 	
					date = df.parse(itr.nextToken());
					cal.setTime(date);

				} catch (Exception e) { 
					e.printStackTrace();
				}
			
				dayNum = cal.get(Calendar.DAY_OF_WEEK);	
				BaseNumber.set(Basenum+","+dayNum);
				
				String vehicle = itr.nextToken();
				String trip = itr.nextToken();
				ActiveVehicles.set(trip+","+vehicle);
				
				context.write(BaseNumber, ActiveVehicles);
			}
			
		}//map
		
	} //mapper
	
	public static class UBERReducer extends Reducer <Text, Text, Text, Text> {
		
		private Text Base_key = new Text();
		private Text Result_val = new Text();
		
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
			String[] week = {"SUN","MON","THU","WED","THR","FRI","SAT"};	

			int trip_sum = 0; 
			int vehicle_sum = 0; 
			String baseName = ""; 
			int dayNum = 0;


			for (Text val : values) {
			
				StringTokenizer itr = new StringTokenizer(val.toString(),",");
				StringTokenizer itr2 = new StringTokenizer(key.toString(),",");

				trip_sum += Integer.parseInt(itr.nextToken());
				vehicle_sum += Integer.parseInt(itr.nextToken());
				
				baseName = itr2.nextToken();
				dayNum = Integer.parseInt(itr2.nextToken());

			}
					
			Base_key.set(baseName+","+week[dayNum-1]);
			Result_val.set(trip_sum+","+vehicle_sum);
			context.write(Base_key, Result_val);
			
		}//reduce 
		
	}//reducer 
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage : UBER <in> <out>"); 
			System.exit(2);
		}
		
		Job job = new Job(conf, "UBER");
		job.setJarByClass(UBERStudent20180250.class); 
		job.setMapperClass(UBERMapper.class);
		job.setReducerClass(UBERReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}//UBER
