package pt.utl.ist.cn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import pt.utl.ist.cn.PageRank.Map;
import pt.utl.ist.cn.PageRank.Reduce;

public class PageOrderer {

	public void run(){
//		Configuration conf = new Configuration();
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
//		FileInputFormat.addInputPath(job, new Path("in"));
//		FileOutputFormat.setOutputPath(job, new Path("out"));
//
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
