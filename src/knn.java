import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;



import java.util.Map.Entry;



class data{
	public float x;
	public float y;
	public String label;
}

@SuppressWarnings("hiding")
interface WritableComparable1<Mapvalout> extends Writable,Comparable<Mapvalout>{
	int compareTo(Mapvalout o);
}



class Mapvalout implements WritableComparable1<Mapvalout>{
	public DoubleWritable khoangcach;
	public Text label;

	public Mapvalout() {
		khoangcach = new DoubleWritable(0);
		label = new Text("Nhan");
	}
	public Mapvalout(double a,String b){
		khoangcach =new DoubleWritable(a);
		label = new Text(b);
	};
	
	public void write(DataOutput out) throws IOException {
		khoangcach.write(out);
		label.write(out);
      }
	
	 public void readFields(DataInput in) throws IOException {
		 	khoangcach.readFields(in);
		 	label.readFields(in);
	      }


	@Override
	public int compareTo(Mapvalout o) {
		if(this.khoangcach.get() < o.khoangcach.get()) {
			return -1;
		}
		if(this.khoangcach.get() >o.khoangcach.get()) {
			return 1;
		}
		else {
			return 0;
		}
	}
	@Override
    public int hashCode() {
        return Objects.hash(khoangcach,label);
    }
}



public class knn {
	public static List<data> Data = new ArrayList<data>();
	public static int k =3;
	public static class Map1 extends Mapper<LongWritable,Text,Text,Mapvalout>{
	
		@Override
		public void map(LongWritable key, Text value,Context output) throws IOException, InterruptedException {
			
			String valueString = value.toString();
			
		
			
			String[] temp = valueString.split(",");
			float _x = Float.parseFloat(temp[0]);
			float _y = Float.parseFloat(temp[1]);
			
			double _khoangcach;
		
			for (data t:Data) {
				_khoangcach = Math.sqrt((t.x -_x)*(t.x-_x)+(t.y-_y)*(t.y-_y));
				Mapvalout valout = new Mapvalout(_khoangcach,t.label.toString());

				output.write(new Text(temp[0]+" "+temp[1]), valout);
			}
		}

	}
	public static class Reduce1 extends Reducer<Text,Mapvalout,Text,Text>{

		public Mapvalout temp1 = new Mapvalout(0,"uji");
		@Override
		public void reduce(Text key, Iterable<Mapvalout> values,
				Context output) throws IOException, InterruptedException{
			System.out.println("*************************************************************************");
			System.out.println("Xin chao Reducer");
			System.out.println(key);
			List<Mapvalout> cacKhoangcach = new ArrayList<Mapvalout>();

			System.out.println("--------------------------------");
			for(Mapvalout khoangcach: values) {
				
				Mapvalout temp2 = new Mapvalout(khoangcach.khoangcach.get(),khoangcach.label.toString()); 
				cacKhoangcach.add(temp2);
			}

			
			
			Collections.sort(cacKhoangcach);

			Map<String,Integer> tudien = new HashMap<String,Integer>();
			for(int i=0;i<k;i++) {
				if(tudien.containsKey(cacKhoangcach.get(i).label.toString())){
					int temp = tudien.get(cacKhoangcach.get(i).label.toString());
					tudien.put(cacKhoangcach.get(i).label.toString(),temp+1);
				}
				else {
					 tudien.put(cacKhoangcach.get(i).label.toString(),1);
				}
			}
			int klanggieng = Collections.max(tudien.values());
			List<String> outputLabel = new ArrayList<String>();
			for(Entry<String, Integer> entry:tudien.entrySet()) {
				if(entry.getValue() == klanggieng) {
					outputLabel.add(entry.getKey());
				}
			}

			if(outputLabel.size() ==1) {
				
				output.write(key,new Text(outputLabel.get(0)));
			}
			else {
				
				if(outputLabel.size() == 0) {
					System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++");
					System.out.println("+                                             +");
					System.out.println("+                                             +");
					System.out.println("+    Co loi xay ra, khong co lang gieng gan   +");
					System.out.println("+                                             +");
					System.out.println("+                                             +");
					System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++");
				}
				else {
					output.write(key,cacKhoangcach.get(0).label);
				}
			}
			
		}

	}


	
	public static void run() throws Exception {

		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf,"testknn");
		job1.setJobName("knn");
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Mapvalout.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);

		
		FileInputFormat.setInputPaths(job1, new Path("file:///C:\\testhadoop\\input\\knninput.txt"));
		FileOutputFormat.setOutputPath(job1, new Path("C:\\testhadoop\\outputknn"));
		
		
		
		
		String datafile = "C:\\testhadoop\\input\\knndataset.txt";
		String line;
		try {
			BufferedReader dataReader = new BufferedReader(new FileReader(new File(datafile)));
			while((line = dataReader.readLine()) != null) {
				String[] temp = line.split(",");
				data t = new data();
				t.x =Float.parseFloat(temp[0]);
				t.y = Float.parseFloat(temp[1]);
				t.label = temp[2];
				Data.add(t);
				
			}
			dataReader.close();
			for(data t : Data) {
				System.out.println(t.x + " "+ t.y +" "+ t.label);
				
			}
			
		}
		catch(Exception e) {
			System.out.println("co loi khi doc file");
			System.out.println(e);
		}
		try {
			if (!job1.waitForCompletion(true))
				System.out.println("da xong");
				return;
		}
		catch(Exception e ){
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws Exception {
		run();
	}
}
