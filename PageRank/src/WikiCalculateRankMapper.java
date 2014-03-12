package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiCalculateRankMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException {
		int pageIndex = value.find("\t");
		int rankIndex = value.find("\t",pageIndex+1);
		
		String page = Text.decode(value.getBytes(),0,pageIndex);
		String pageAndRank = Text.decode(value.getBytes(),0,rankIndex+1);
		
		context.write(new Text(page), new Text("!"));
		
		if(rankIndex == -1) return;
		
		String outLinks = Text.decode(value.getBytes(),rankIndex+1,value.getLength()-(rankIndex+1));
		String[] otherPages = outLinks.split(" ");
		int numLinks = otherPages.length;
		
		for(String str : otherPages) {
			Text all = new Text(pageAndRank + numLinks);
			context.write(new Text(str), all);
		}
		
		context.write(new Text(page), new Text("|"+outLinks));
	}

}
