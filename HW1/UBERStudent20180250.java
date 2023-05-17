import java.io.IOException;
import java.util.*;
import java.time.DayOfWeek;

import java.time.LocalDate;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20180250
{
        public static class UBERMapper extends Mapper<Object, Text, Text, Text>
        {
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException
                {
                        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
                        Text outputKey = new Text();
                        Text outputValue = new Text();
                        String baseN = "";
                        String o_value = "";
                        String dateS = "";
                        int year=0;
                        int month=0;
                        int day=0;
                        String answer="";
                        String vehicles="";
                        String trips="";

                        baseN=itr.nextToken();
                        dateS= itr.nextToken();
                        StringTokenizer itr3 = new StringTokenizer(dateS, "/");
                        month =Integer.parseInt(itr3.nextToken());
                        day =Integer.parseInt(itr3.nextToken());
                        year = Integer.parseInt(itr3.nextToken());
                        LocalDate date = LocalDate.of(year, month, day);
                        DayOfWeek dayOfWeek = date.getDayOfWeek();
                        if(dayOfWeek.getValue() == 1) {
                                answer = "MON";
                        }else if (dayOfWeek.getValue() == 2) {
                                answer = "TUE";
                        }else if (dayOfWeek.getValue() == 3) {
                                answer = "WED";
                        }else if (dayOfWeek.getValue() == 4) {
                                answer = "THR";
                        }else if (dayOfWeek.getValue() == 5) {
                                answer = "FRI";
                        }else if (dayOfWeek.getValue() == 6) {
                                answer = "SAT";
                        }else if (dayOfWeek.getValue() == 7) {
                                answer = "SUN";
                        }
                        baseN = baseN+","+answer;
			outputKey.set( baseN );
                        vehicle = itr.nextToken();
                        trip = itr.nextToken();
                        outputValue.set( trip+","+vehicle );
                        context.write( outputKey, outputValue );


                }
                                                                                           
	}
  	public static class UBERReducer extends Reducer<Text,Text,Text,Text>
        {
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException
                {
			Text reduce_key = new Text();
                        Text reduce_result = new Text();
                        int t_sum=0;
                        int v_sum=0;
                        String result="";
                        for (Text val : values)
                        {
                                StringTokenizer itr2 = new StringTokenizer(val.toString(), ",");
                                int tnum =Integer.parseInt(itr2.nextToken());
                                int vnum =Integer.parseInt(itr2.nextToken());
                                t_sum +=tnum;
                                v_sum +=vnum;
                        }
                        result= t_sum+","+v_sum;
                        reduce_result.set(result);
                        context.write(key, reduce_result);


                }
        }
        public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (otherArgs.length != 2)
                {
                        System.err.println("Usage: UBER <in> <out>");
                        System.exit(2);
                }
                Job job = new Job(conf, "UBERStudent20180250");
                job.setJarByClass(UBERStudent20180250.class);
                job.setMapperClass(UBERMapper.class);
                job.setReducerClass(UBERReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
                FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
