import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
//    private Text word = new Text();
//    private Text word1 = new Text();
//	private final static IntWritable one = new IntWritable(1);
//	public IntWritable info = new IntWritable(10);
	
	@Override
	public void configure(JobConf job) {
		System.out.println(job.getJobName());
	}
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] arr = valueString.split(",");
		String label = arr[2];
		Float x = Float.parseFloat(arr[0]);
		Float y = Float.parseFloat(arr[1]);
		
		
		
		
		

		
		//StringTokenizer itr = new StringTokenizer(test.toString());
		//StringTokenizer itr1 = new StringTokenizer(test2.toString());
		
//	      while (itr.hasMoreTokens()) {
//	        word.set(itr.nextToken());
//	        word1.set(itr1.nextToken());
//	        info1 =new FloatWritable(Float.parseFloat(word1.toString()));
//	        
//	         
//	        output.collect(word,info1);
//	        //new IntWritable(Integer.parseInt(word1.toString()))
//	        
//	      }
		//output.collect(new Text(arr[0]), new FloatWritable(Float.parseFloat(arr[1])));
	}
}
