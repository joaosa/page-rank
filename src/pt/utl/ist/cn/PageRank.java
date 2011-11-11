package pt.utl.ist.cn;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PageRank {

	public static void main(String[] args) throws Exception {
		/*Apaga pasta PageRank00000*/
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		Path path=new Path("PageRank00000");
		hdfs.delete(path, true);
		
		String linksFile = new String("page_rank.txt");
		
		linksFile=PageParser.parseDirectory(args[0]);
		
		hdfs.copyFromLocalFile(false, true, new Path(linksFile), new Path("/in/"+linksFile));
		
		NumberFormat nf = new DecimalFormat("00000");
		PageRanker.run("/in","PageRank00000");
		/*Corre PageRanker - Step2*/
		int i;
		for(i=0;i<5;i++){
			if(i!=0) {
				path = new Path("PageRank"+nf.format(i-1));
				hdfs.delete(path, true);
			}
			path = new Path("PageRank"+nf.format(i+1));
			hdfs.delete(path, true);
			
			PageRanker.run("PageRank"+nf.format(i),"PageRank"+nf.format(i+1));
		}
		PageRanker.run("PageRank"+nf.format(i),"/out");
		
		new PageOrderer().order("/out", args[1]);
		
		path=new Path("/out");
		hdfs.delete(path, true);
	}
	
	
	
}
