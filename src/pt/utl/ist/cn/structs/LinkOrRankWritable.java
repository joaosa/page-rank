package pt.utl.ist.cn.structs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;


/* Pode ser a descrição com o rank e o número de links ou um array de links**/
public class LinkOrRankWritable implements WritableComparable<LinkOrRankWritable> {
	
	private boolean isList;
	double rank;
	int degree;
	ArrayList<String> references;
	
	public LinkOrRankWritable(){
		super();
	}
	
	public boolean isList(){
		return isList;
	}
	
	public double getRank(){
		return rank;
	}
	public int getDegree(){
		return degree;
	}
	public ArrayList<String> getReferences(){
		return references;
	}
	public LinkOrRankWritable(double rank,int degree){
		isList=false;
		this.rank=rank;
		this.degree=degree;
	}
	
	public LinkOrRankWritable(ArrayList<String> references){
		isList=true;
		this.references=references;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.isList=arg0.readBoolean();
		if(isList){
			String line = arg0.readLine();
			String[] tokens = line.split(" ");
			references = new ArrayList<String>();
			for(int i=0;i<tokens.length-1;i++){
				references.add(tokens[i]);
			}
		}else{
			rank = arg0.readDouble();
			degree = arg0.readInt();
		}
	}

	
	
	
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeBoolean(isList);
		if(isList){
			for(String ref: references){
				arg0.writeBytes(ref+" ");
			}
		}else{
			arg0.writeDouble(rank);
			arg0.writeInt(degree);
		}
	}

	@Override
	public int compareTo(LinkOrRankWritable o) {
		if(isList){
			if(o.isList) return references.size() - o.references.size();
			else return 1;
		}else{
			if(o.isList) return -1;
			else return (int) (rank - o.rank);
		}
	}
	
	@Override
	public String toString() {
		String res= "Boolean: "+Boolean.toString(isList)+"\n";
		if(isList){
			res += "Refs: ";
			for(String ref: references){
				res+=ref+" ";
			}
			res+="\n";
		}else{
			res+= "Rank: "+Double.toString(rank)+"\n";
			res+= "Degree: "+Integer.toString(degree)+"\n";
		}
		return res;
	}
	
}
