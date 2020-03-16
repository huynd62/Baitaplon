import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount1Reducer extends MapReduceBase implements Reducer<Text,FloatWritable,Text ,FloatWritable> {

	public void reduce(Text t_key, Iterator<FloatWritable> values, OutputCollector<Text,FloatWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		
		//FloatWritable max=  new FloatWritable(Float.MIN_VALUE) ; 
				//new FloatWritable(0);
		float sum=0;
		int n=0;
		while (values.hasNext()){
			// replace type of value with the actual type of our value
			 FloatWritable value = (FloatWritable) values.next();
			//y += value.get();
//			if(max.get() < value.get()) {
//				max = new FloatWritable(value.get());			
//			}
			sum += value.get();
			n +=1;
		}
		//float tb = sum / n;
		output.collect(key,new FloatWritable(9)); 
				//max);
	}
}
