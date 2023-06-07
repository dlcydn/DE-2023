
import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Pattern;

public class UBERStudent20180250 implements Serializable {

	public static String parseToString(int dayN) {
		switch(dayN){
        	case 1:
        		return "SUN";
        	case 2:
        		return "MON";
        	case 3:
        		return "TUE";
        	case 4:
        		return "WED";
        	case 5:
        		return "THR";
        	case 6:
        		return "FRI";
        	case 7:
        		return "SAT";
        	default:
        		return "NULL";
		}
	}
	
	public static void main(String[] args) throws Exception {
	
		// TODO Auto-generated method stub
		if(args.length < 1) {
            System.out.println("Usage : UBER <input> <output>");
            System.exit(1);
		}

		SparkSession spark = SparkSession
            .builder()
            .appName("UBERStudent20180250")
            .getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		JavaPairRDD<String, String> words = lines.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
            	String [] sp = s.split(",");
            	String date = sp[1];
                
            	SimpleDateFormat transFormat = new SimpleDateFormat("MM/dd/yyyy");
				String dayOfWeek = "";
				try {
					Date day = transFormat.parse(date);
			
					Calendar cal = Calendar.getInstance() ;
					cal.setTime(day);
			     
					int dayN = cal.get(Calendar.DAY_OF_WEEK) ;
				
					dayOfWeek = parseToString(dayN);
				} catch (Exception e) {}
				String key = sp[0] + "," + dayOfWeek;
				String value = sp[3] + "," + sp[2];

				System.out.println("[MAPPER] " + key + " " + value);
				return new Tuple2(key, value);
            }
		});

		JavaPairRDD<String, String> counts = words.reduceByKey(new Function2<String, String, String>() {
            public String call(String val1, String val2) {
                String [] val1_sp = val1.split(",");
                String [] val2_sp = val2.split(",");
                
                int trips = Integer.valueOf(val1_sp[0]) + Integer.valueOf(val2_sp[0]);
                int vehicles = Integer.valueOf(val1_sp[1]) + Integer.valueOf(val2_sp[1]);
       		
	       	System.out.println(trips + "," + vehicles);	
                return trips + "," + vehicles;
        }
	});
		// write result
		counts.saveAsTextFile(args[1]);
    	spark.stop();
	}
}
