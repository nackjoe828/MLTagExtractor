package tagcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper;

public class TagCounter{
	
	private final static IntWritable one = new IntWritable(1);
	private static Text word = new Text();
	
	public static class TSVLoader extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
			String line = value.toString();
			String[] fields = line.split("\t");
			if(fields.length < 4) return;
			/*
			 * Cleaning part to be implemented later
			 */
			StringTokenizer tokenizer = new StringTokenizer(fields[3]);
			while(tokenizer.hasMoreTokens()){
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class TagReducer extends Reducer<Text, IntWritable, IntWritable, Text>{
		private static final int CUTOFF = 5;
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			//omit less popular tags
			if(sum >= CUTOFF)
			context.write(new IntWritable(sum), key);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration config = new Configuration();
		
		Job job = new Job(config, "TagCounter");
		job.setJarByClass(TagCounter.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(TSVLoader.class);
		job.setReducerClass(TagReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("TrainSmall.tsv"));
		FileOutputFormat.setOutputPath(job, new Path("result/output-tagcount"));
		
		job.waitForCompletion(true);
	}
}
