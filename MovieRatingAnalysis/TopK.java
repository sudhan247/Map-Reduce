package please;
import java.io.IOException;
import java.util.TreeMap;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
  
public class TopK {  
    public static final int K = 30;  	
    public static class KMap extends Mapper<LongWritable,Text,IntWritable,Text> {
    	
        TreeMap<Integer, String> map = new TreeMap<Integer, String>();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(line.trim().length() > 0 && line.indexOf("#") != -1) {
                String[] arr = line.split("#", 2);
                String name = arr[0];  
                Integer num = Integer.parseInt(arr[1]);  
                map.put(num, name);  
                if(map.size() > K) {
                    map.remove(map.firstKey());  
                }  
            }  
        }  
        @Override
        protected void cleanup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)  
                throws IOException, InterruptedException {  
            for(Integer num : map.keySet()) {  
                context.write(new IntWritable(num), new Text(map.get(num)));  
            }  
        }  
    }  
      
    public static class KReduce extends Reducer<IntWritable, Text, Text, IntWritable> {  
        TreeMap<Integer, String> map = new TreeMap<Integer, String>();  
        @Override  
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {    
            map.put(key.get(), values.iterator().next().toString());  
            if(map.size() > K) {
                map.remove(map.firstKey());  
            }  
        }  
        @Override  
        protected void cleanup(Reducer<IntWritable, Text, Text, IntWritable>.Context context)  
                throws IOException, InterruptedException {  
            for(Integer num : map.keySet()) {  
//                context.write(new IntWritable(num), new Text(map.get(num)));  
            	context.write(new Text(map.get(num)), new IntWritable(num));
            }  
        }  
    }  
  
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();
        Job job = new Job(conf, "my own word count"); 
        job.setJarByClass(TopK.class);  
        job.setMapperClass(KMap.class);  
        
        job.setReducerClass(KReduce.class);  
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);  
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println(job.waitForCompletion(true));
    }
}
