package hw1;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.StringTokenizer;
import java.util.random.RandomGenerator.ArbitrarilyJumpableGenerator;

import org.w3c.dom.Text;

import hw1.IMDBStudent20180250.IMDBMapper;
import hw1.IMDBStudent20180250.IMDBReducer;


public class UBERStudent20180250 {

	public static class UBERMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text BaseNumber = new Text(); //key
		private Text ActiveVehicles = new Text(); //value
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] week = {"SUN","MON","THU","WED","THR","FRI","SAT"}; 
			
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat df = new SimpleDateFormat("YYYY-MM-dd"); 
			Date date = null;
			
			StringTokenizer itr = new StringTokenizer(value.toString(),",");
			
			while (itr.hasMoreTokens()) {
				
				String Basenum = itr.nextToken();
				try { 
					
					date = df.parse(cal.setTime(itr.nextToken()));

				} catch (ParseException e) { 
					e.printStackTrace();
				}
				BaseNumber.set(Basenum+","+week[date]);
				
				String trip = itr.nextToken();
				String vehicle = itr.nextToken();
				ActiveVehicles.set(vehicle+","+trip);
				
				context.write(BaseNumber, ActiveVehicles);
			}
		}//map
		
	} //mapper
	
	public static class UBERReducer extends Reducer <Text, Text, Text, Text> {
		
		private Text Base_key = new Text();
		private Text Result_val = new Text();
		
		public void reduce (Text key, Text values, Context context) throws IOException, InterruptedException {
			
			int trip_sum; 
			int vehicle_sum; 
			
			for (Text val : values) {
			
				StringTokenizer itr = new StringTokenizer(val.toString(),",");
				
				trip_sum += Integer.parseInt(itr.nextToken().trim());
				vehicle_sum += Integer.parseInt(itr.nextToken().trim());
			
			}
					
			Base_key.set(key);
			Result_val.set(trip_sum+","+vehicle_sum);
			context.write(Base_key, Result_val);
			
		}//reduce 
		
	}//reducer 
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingsArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage : UBER <in> <out>"); 
			System.exit(2);
		}
		
		Job job = new Job(conf, "UBER");
		job.setJarByClass(UBERStudent20180250.class); 
		job.setMapperClass(UBERMapper.class);
		job.setCombinerClass(UBERReducer.class); 
		job.setReducerClass(UBERReducer.class);
		job.setOutpupKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
