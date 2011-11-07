package pt.utl.ist.cn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PageWritable implements WritableComparable<PageWritable> {
	
	private String url;
	private double rank;
	private ArrayList<String> references;
	private ArrayList<String> referencedBy;

	
	
	public PageWritable(Text arg0){
		String args = arg0.toString();
		String[] tokens = args.split(" ");
		url = tokens[0];
		rank = Double.parseDouble(tokens[1]);
		boolean inReferences = true;
		references = new ArrayList<String>();
		referencedBy = new ArrayList<String>();
		for(int i = 2; i<tokens.length ; i++){
			if(tokens[i].equals(":")){
				inReferences = false;
				continue;
			}
			if(inReferences){
				references.add(tokens[i]);
			}else{
				referencedBy.add(tokens[i]);
			}
		}
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		String args = arg0.readLine();
		String[] tokens = args.split(" ");
		url = tokens[0];
		rank = Double.parseDouble(tokens[1]);
		boolean inReferences = true;
		references = new ArrayList<String>();
		referencedBy = new ArrayList<String>();
		for(int i = 2; i<tokens.length ; i++){
			if(tokens[i].equals(":")){
				inReferences = false;
				continue;
			}
			if(inReferences){
				references.add(tokens[i]);
			}else{
				referencedBy.add(tokens[i]);
			}
		}
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeChars(url+ " ");
		arg0.writeChars(new Double(rank).toString()+" ");
		for(String ref: references){
			arg0.writeChars(ref);
			arg0.writeChars(" ");
		}
		arg0.writeChars(":");
		for(String ref: referencedBy){
			arg0.writeChars(ref);
			arg0.writeChars(" ");
		}
	}
	
	public String getURL(){
		return url;
	}

	@Override
	public int compareTo(PageWritable arg0) {
		return url.compareTo(arg0.url);
	}
	
	public String toString(){
		String res = "URL: "+url+"\nRank: "+rank+"\nReferences: ";
		for(String ref: references){
			res+=ref+"; ";
		}
		res+="\nReferenced By: ";
		for(String ref: referencedBy){
			res+=ref+"; ";
		}
		return res;
	}

}
