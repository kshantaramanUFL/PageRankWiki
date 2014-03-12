package PageRank;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiRankOrderMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{

	@Override
	public void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String[] pageAndRank = getPageAndRank(key,value);
		Double parse = Double.parseDouble(pageAndRank[1]);
		Text page = new Text(pageAndRank[0]);
		DoubleWritable rank = new DoubleWritable(parse);
		int n=context.getConfiguration().getInt("n", 0);
		if(parse>=(5.0D/n))
			context.write(rank, page);
	}

	private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
		String[] pageAndRank = new String[2];
		int pageIndex = value.find("\t");
		int rankIndex = value.find("\t",pageIndex+1);
		int end = (rankIndex == -1) ? (value.getLength()-(pageIndex+1)) : (rankIndex - (pageIndex + 1)) ;
		
		pageAndRank[0] = Text.decode(value.getBytes(),0,pageIndex);
		pageAndRank[1] = Text.decode(value.getBytes(),pageIndex+1,end);
		
		return pageAndRank;
	}

}
