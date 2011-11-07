package pt.utl.ist.cn.structs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;


/* Pode ser a descrição com o rank e o número de links ou um array de links**/
public class LinkOrRankWritable implements WritableComparable<LinkOrRankWritable>  {
	
	boolean isList;
	double rank;
	int degree;
	ArrayList<String> references;
	
	
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
			String[] tokens = arg0.readLine().split(" ");
			references = new ArrayList<String>();
			for(int i=0;i<tokens.length;i++){
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
				arg0.writeChars(ref+" ");
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

}
