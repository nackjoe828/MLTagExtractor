package topfivetags;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopFiveExtractor {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
			String line = value.toString();
			String[] fields = line.split("\t");
			if(fields.length < 4) return;
			int id = Integer.parseInt(fields[0]);
			
			//see if tags contain one of top5 tags
			String[] tags = fields[3].split(" ");
			int i = 0;
			for(;i < tags.length; i++){
				if(tags[i].equals("c#")){
					context.write(new IntWritable(id), new Text(fields[1] + "\t" + fields[2] + "\t" + "c#"));
					return;
				}
				else if(tags[i].equals("java")){
					context.write(new IntWritable(id), new Text(fields[1] + "\t" + fields[2] + "\t" + "java"));
					return;
				}
				else if(tags[i].equals("php")){
					context.write(new IntWritable(id), new Text(fields[1] + "\t" + fields[2] + "\t" + "php"));
					return;
				}
				else if(tags[i].equals("javascript")){
					context.write(new IntWritable(id), new Text(fields[1] + "\t" + fields[2] + "\t" + "javascript"));
					return;
				}
				else if(tags[i].equals("android")){
					context.write(new IntWritable(id), new Text(fields[1] + "\t" + fields[2] + "\t" + "android"));
					return;
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
		public void reduce(IntWritable key, Text value, Context context)
			throws IOException, InterruptedException{
			context.write(key, value);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "TopFiveExtractor");
		job.setJarByClass(TopFiveExtractor.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("TrainSmall.tsv"));
		FileOutputFormat.setOutputPath(job, new Path("result/output-topfivetags"));
		
		job.waitForCompletion(true);
	}
}
