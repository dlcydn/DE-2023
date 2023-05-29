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

class Video {
	public String category;
	public double rate;
	
	public Video(String category, double rate) {
		this.category = category;
		this.rate = rate;
	}
	
}//Video

public class YouTubeStudent20180250 {
	
	public static class AverageComparator implements Comparator<Video> {
		public int compare(Video x, Video y) {
			if ( x.rate > y.rate ) return 1;
			if ( x.rate < y.rate ) return -1;
			return 0;
		}
	}//comparator 
	
	public static void insertVideo(PriorityQueue q, String category, double rate, int topK) {
		Video video_head = (Video) q.peek();
		if ( q.size() < topK || video_head.rate < rate ) {
			Video video = new Video(category, rate);
			q.add(video);
			
			if(q.size() > topK) q.remove();
		}
	}//buffer 
	
	public static class YoutubeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			String category = "";
			String rate = "";
			
			int i = 0;
			while(itr.hasMoreTokens()) {
				
				rate = itr.nextToken();
				
				if(i == 3) {
					category = rate;
				}
				i++;
			}
			
			System.out.println("Genre : " + category + " Rate : " + rate);
			
			context.write(new Text(category), new DoubleWritable(Double.valueOf(rate)));
			
		}//map
		
	}//mapper 
	
	public static class YoutubeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
		private PriorityQueue<Video> queue;
		private Comparator<Video> comp = new AverageComparator();
		private int topK;
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			double sum = 0;
			int total = 0;
			
			for(DoubleWritable val : values) {
				sum += val.get();
				total++;
			}
			
			double average = sum / total;
			
			insertVideo(queue, key.toString(), average, topK);
		}//reduce 
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Video>( topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Video video = (Video) queue.remove();
				context.write(new Text(video.category), new DoubleWritable(video.rate));
			}
		}
		
	}//reducer 
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: YouTube <in> <out> <k>");
			System.exit(2);
		}
		
		conf.setInt("topK", Integer.valueOf(otherArgs[2]));
		Job job = new Job(conf, "YouTubeStudent20180250");
		job.setJarByClass(YouTubeStudent20180250.class);
		job.setMapperClass(YoutubeMapper.class);
		job.setReducerClass(YoutubeReducer.class);
		job.setNumReduceTasks(1);	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}//main 
		
			

}//Youtube

