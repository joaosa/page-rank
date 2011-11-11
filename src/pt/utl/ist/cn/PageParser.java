package pt.utl.ist.cn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import pt.utl.ist.cn.WordCount.Map;
import pt.utl.ist.cn.WordCount.Reduce;

public class PageParser {

	//Load Page as a String
	static String loadPage(String name) throws Exception {
		StringBuffer output = new StringBuffer();
		FileReader file = new FileReader(name);
		BufferedReader buff = new BufferedReader(file);
		boolean eof = false;
		
		while (!eof) {
			String line = buff.readLine();
			if (line == null)
				eof = true;
			else
				output.append(line + "\n");
		}
		buff.close();
		return output.toString();
	}



	public static void main(String[] args) throws Exception {
		
		// Get Directory and Children Directories
		File dir = new File(args[0]);
		String[] children = dir.list();

		try{
			// Create file 
			FileWriter fstream = new FileWriter(args[1]+"/PageRank.txt");
			BufferedWriter out = new BufferedWriter(fstream);

			if (children == null) {
				// Either dir does not exist or is not a directory
			} else {
				for (int i=0; i<children.length; i++) {
					
					// Get filename of file or directory
					String filename = children[i];
					// Load page as String
					String page = loadPage(dir+"/"+filename);
					// Compile Patterns and Matcher
					Pattern pattern = Pattern.compile("<a.+href=\"(.+?)\"");
					Matcher matcher = pattern.matcher(page);
					// Write to file
					out.write(filename + " " + "1.0 ");
					while (matcher.find()) {
						out.write(matcher.group(1) + " ");
					}
					// Close file
					out.write("\n");
				}
			}	  

			//Close the output stream
			out.close();
			
		}catch (Exception e){//Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
	}
}

