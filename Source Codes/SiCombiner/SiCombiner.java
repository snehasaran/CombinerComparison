        
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
        
public class SiCombiner {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
        	String tmp = word.toString();
        	if((tmp.startsWith("m")) || (tmp.startsWith("n")) || 
        			(tmp.startsWith("o")) || (tmp.startsWith("p")) || 
        			(tmp.startsWith("q")) ||(tmp.startsWith("M")) || 
        			(tmp.startsWith("N")) || (tmp.startsWith("O")) || 
        			(tmp.startsWith("P")) || (tmp.startsWith("Q"))) {
        		context.write(word, one);
        	}
        }
    }
 }
 
//This is the customer partitioner
// The partitioning phase takes place after the map phase and before 
// the reduce phase. The number of partitions is equal to the number 
// of reducers. The data gets partitioned across the reducers according
// to the partitioning function . The difference between a partitioner
// and a combiner is that the partitioner divides the data according to
// the number of reducers so that all the data in a single partition gets 
// executed by a single reducer. 
// However, the combiner functions similar to the reducer and processes 
// the data in each partition. The combiner is an optimization to the reducer.

// Partition 0 : words starting with 'm' or 'M'
// Partition 1 : words starting with 'n' or 'N'
// Partition 2 : words starting with 'o' or 'O'
// Partition 3 : words starting with 'p' or 'P'
// Partition 4 : words starting with 'q' or 'Q'

// Input to the getPartition function is:
// The key and value are the intermediate 
// key and value produced by the map function.
// The numReduceTasks is the number of reducers used in the MapReduce 
// program and it is specified in the driver program.

// To prevent divide by zero exception, I am returning the
// (partitioner number) mod numReduceTasks. 

 
 public static class Partition extends Partitioner<Text, IntWritable> {

		@Override
		// arg0 -- key
		// arg1 -- value
		// arg2 -- no of reducers
		// In this example there are 5 partitioners.
		
		public int getPartition(Text arg0, IntWritable arg1, int numReduceTasks) {
			
			String word = arg0.toString(); // key is the word starting with M,n ....
			
			if( (word.startsWith("m")) || (word.startsWith("M"))) {
				return 0;
			}
			
			if ((word.startsWith("n")) || (word.startsWith("N"))) {
				return 1 % numReduceTasks;
			}
			
			if ((word.startsWith("o")) || (word.startsWith("O"))) {
				return 2 % numReduceTasks;
			}
			
			if ((word.startsWith("p")) || (word.startsWith("P"))) {
				return 3 % numReduceTasks;
			}
			
			if ((word.startsWith("q")) || (word.startsWith("Q"))) {
				return 4 % numReduceTasks;
			}
			
			return 0;
		}
		   
	   }
 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val : values){
        	sum += val.get(); 
        }
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = Job.getInstance(conf,"SiCombiner");
    job.setJarByClass(SiCombiner.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setNumReduceTasks(5);
    
    job.setMapperClass(Map.class);
    
    
    job.setCombinerClass(Reduce.class);    //Combiner enabled
    job.setPartitionerClass(Partition.class);
    job.setReducerClass(Reduce.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}