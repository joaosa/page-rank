package pt.utl.ist.cn;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.Reducer;
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
import pt.utl.ist.cn.structs.LinkOrRankWritable;
import pt.utl.ist.cn.structs.PageWritable;

public class PageRanker {

	public static int run(String input, String output) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();


		Job job = new Job(conf, "PageRank");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Note that these are the default.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputValueClass(LinkOrRankWritable.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		//PageRank.moveToTrash(conf, new Path("out"));

		
		
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return (job.waitForCompletion(false) ? 0 : 1);
		
		
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, LinkOrRankWritable>{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			PageWritable page = new PageWritable(value);
			for(String ref: page.getReferences()){
				LinkOrRankWritable lor = new LinkOrRankWritable(page.getRank(), page.getReferences().size());
				context.write(new Text(ref),lor);
			}
			LinkOrRankWritable lor = new LinkOrRankWritable(page.getReferences());
			context.write(new Text(page.getURL()),lor);
		}
	}
	
	public static class Reduce extends Reducer<Text, LinkOrRankWritable, Text, Text> {
		
		private static double DAMPING = 0.15;

		public void reduce(Text key, Iterable<LinkOrRankWritable> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> references = null;
			double linkSum = 0;
			String keyString =key.toString();
			for(LinkOrRankWritable val: values){
				if(val.isList()){
					references = val.getReferences();
				}else{
					linkSum += val.getRank()/val.getDegree();
				}
			}
			if(references==null) return;
			
			double pageRankSum = DAMPING*linkSum + (1-DAMPING);
			String res = new Double(pageRankSum).toString() + " ";
			for(String ref: references){
				res+=ref+" ";
			}
			context.write(new Text(keyString), new Text(res));
		}
	}
	
	
	
	
	
}
