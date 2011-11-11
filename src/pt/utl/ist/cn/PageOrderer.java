package pt.utl.ist.cn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PageOrderer {

	public void order(String hadoopFolder,String outFile) throws IOException{
		String rank = readFromHadoopFolder(hadoopFolder);
		HashMap<String,Double> map = new HashMap<String,Double>();
		RankComparator bvc =  new RankComparator(map);
		TreeMap<String,Double> sorted_map = new TreeMap<String,Double>(bvc);
		for(String line: rank.split("\n")){
			String[] tokens = line.split("\\s+");
			map.put(tokens[0], Double.parseDouble(tokens[1]));
		}
		sorted_map.putAll(map);
		FileWriter fstream = new FileWriter(outFile);
		BufferedWriter out = new BufferedWriter(fstream);

		for (String key : sorted_map.keySet()) {
			out.write( key+"\n");
		}
		out.close();
		fstream.close();
	}

	private static String readFromHadoopFolder(String hadoopFolder) throws IOException{
		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] status = fs.listStatus(new Path("/out"));
		if(status.length>0){
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[0].getPath())));
			String res = new String(), line;
			line=br.readLine();
			while (line != null){
				res += line + "\n";
				line=br.readLine();
			}
			return res;
		}else{
			return "";
		}
	} 


	private class RankComparator implements Comparator<String> {

		HashMap<String,Double> base;
		public RankComparator(HashMap<String,Double> base) {
			this.base = base;
		}
		public int compare(String a, String b) {
			if(base.get(a) < base.get(b)) {
				return 1;
			} else if(base.get(a) == base.get(b)) {
				return 0;
			} else {
				return -1;
			}
		}
	}

}
