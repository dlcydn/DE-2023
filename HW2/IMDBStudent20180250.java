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

class Movie{ 
	
	public String movie;
	public double rate;
	
	public Movie(String movie, double rate) {
		this.movie = movie; 
		this.rate = rate;
	}
	
	public String getString() { 
		return movie+" "+rate; 
	}	
}//Movie

//Composite Key
class DoubleString implements WritableComparable {
	
	String joinKey = new String();
	String tableName = new String();

	public DoubleString() {}
	public DoubleString( String _joinKey, String _tableName ) {
		joinKey = _joinKey;
		tableName = _tableName;
	}
	
	public void readFields(DataInput in) throws IOException {
		joinKey = in.readUTF();
		tableName = in.readUTF();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(joinKey);
		out.writeUTF(tableName);
	}
		
	public int compareTo(Object o1) {
		DoubleString o = (DoubleString) o1;
		int ret = joinKey.compareTo( o.joinKey );
		if (ret!=0) return ret;
		// sorting
		return tableName.compareTo( o.tableName );
	}
	
	public String toString() { 
		return joinKey + " " + tableName; 
	}
	
}//Double String 

public class IMDBStudent20180250 {
	
	public static class MovieComparator implements Comparator<Movie>{
		
		public int compare(Movie x, Movie y) {
			if (x.rate > y.rate) return 1;
			if (x.rate < y.rate) return -1; 
			return 0;
		}
	}//Comparator
	
	public static void insertMovie(PriorityQueue q, String movie, double rate, int topK) {
		Movie m_head = (Movie)q.peek();
		if(q.size() < topK || m_head.rate < rate) {
			Movie m = new Movie(movie, rate);
			q.add(m);
			
			if(q.size() > topK) q.remove();
		}
	}//Buffer

	public static class CompositeKeyComparator extends WritableComparator {
		protected CompositeKeyComparator() {
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			int result = k1.joinKey.compareTo(k2.joinKey);
			if(0 == result) {
				result = k1.tableName.compareTo(k2.tableName);
			}
			return result;
			}
	}//composite key
	
	public static class FirstPartitioner extends Partitioner<DoubleString, Text> {
		public int getPartition(DoubleString key, Text value, int numPartition) {
			return key.joinKey.hashCode()%numPartition;
		}
	}//first partitioner
	
	public static class FirstGroupingComparator extends WritableComparator {
		protected FirstGroupingComparator() {
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			return k1.joinKey.compareTo(k2.joinKey);
		}
	}//grouping
	
	
	public static class IMDBMapper extends Mapper < Object, Text, DoubleString, Text > {
		
		boolean movieFile = true;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String token = value.toString().replace("::","~");			
			String [] splitVal = token.split("~");
			
			DoubleString outputK = null;
			Text outputV = new Text();
			
			if(movieFile) {
				String id = splitVal[0];
				String title = splitVal[1];
				String genre = splitVal[2];
				
				StringTokenizer itr = new StringTokenizer(genre, "|");
				
				boolean isFantasy = false;
				while(itr.hasMoreElements()) {
					if(itr.nextToken().equals("Fantasy")) {
						isFantasy = true;
						break;
					}
				}
				
				if(isFantasy) {
					outputK = new DoubleString(id, "Movie");
					outputV.set("Movie," + title);
					context.write( outputK, outputV );
				}
			} else {
				String id = splitVal[1];
				String rate = splitVal[2];
				
				outputK = new DoubleString(id, "Rate");
				outputV.set("Rate," + rate);
				context.write( outputK, outputV );
			}
		
		}//map

		protected void setup(Context context) throws IOException, InterruptedException {
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "movies.dat" ) != -1 ) movieFile = true;
			else movieFile = false;
		}
		
	}//mapper 
	
		
	
	public static class IMDBReducer extends Reducer< DoubleString, Text, Text, DoubleWritable > {
		
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		
		public void reduce(DoubleString key, Iterable< Text > values, Context context) throws IOException, InterruptedException {
			
			String title = "";
			int total_rate = 0;
			
			int i = 0;
			for(Text val : values) {
				String data = val.toString();
				String [] splited = data.split(",");
				
				if(i == 0) {
					if(!splited[0].equals("Movie")) break;
					title = splited[1];
				} else {
					total_rate += Integer.valueOf(splited[1]);
				}
				i++;
			}
			
			if (total_rate != 0) {
				double average = ((double) total_rate) / (i - 1);
				insertMovie(queue, title, average, topK);
			}
		}//reduce
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>( topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Movie m = (Movie) queue.remove();
				context.write(new Text(m.movie), new DoubleWritable(m.rate));
			}
		}
		
	}//reducer
	
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Usage: IMDB <in> <out>");
			System.exit(2);
		}
		
		conf.setInt("topK", Integer.valueOf(otherArgs[2]));
		Job job = new Job(conf, "IMDBStudent20180250");
		job.setJarByClass(IMDBStudent20180250.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		job.setNumReduceTasks(1);	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}//main

}//IMDB
