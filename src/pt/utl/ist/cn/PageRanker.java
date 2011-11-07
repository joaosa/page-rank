package pt.utl.ist.cn;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import pt.utl.ist.cn.PageRank.Map;
import pt.utl.ist.cn.PageRank.Reduce;

public class PageRanker {

	public static void run() throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();

		Job job = new Job(conf, "PageRank");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Note that these are the default.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
//		job.setReducerClass(Reduce.class);

		FileInputFormat.addInputPath(job, new Path("in"));
		FileOutputFormat.setOutputPath(job, new Path("out"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println(new PageWritable(value).toString());
		}
	}
	
	
	
	
	
}
