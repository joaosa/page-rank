package pt.utl.ist.cn;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
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
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text urls = new Text();

		// private final static IntWritable initPR = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				urls.set(tokenizer.nextToken());
				context.write(new Text(key.toString()), urls);
			}
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Text values, Context context)
				throws IOException, InterruptedException {
			context.write(key, values);
		}
	}

	public static void main(String[] args) throws Exception {
//		Configuration conf = new Configuration();
//		String[] otherArgs = new GenericOptionsParser(conf, args)
//				.getRemainingArgs();
//
//		Job job = new Job(conf, "PageRank");
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//
//		// Note that these are the default.
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
//
//		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
//		job.setReducerClass(Reduce.class);
//
//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		/*Apaga pasta out*/
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		Path path = new Path("out");
		boolean isDeleted = hdfs.delete(path, true);
		
		/*Corre PageRanker - Step2*/
		PageRanker.run();
	}
	
	public static void moveToTrash(Configuration conf,Path path) throws IOException
	{
	     Trash t=new Trash(conf);
	     boolean isMoved=t.moveToTrash(path);
	     if(!isMoved)
	     {
	    	 System.out.println("Trash is not enabled or file is already in the trash.");
	     }
	}
	
}
