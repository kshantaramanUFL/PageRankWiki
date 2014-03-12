package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WikiGraphMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException {
		int pageTab = value.find("\t");
		
		String page = Text.decode(value.getBytes(),0,pageTab);
		String outlinks = Text.decode(value.getBytes(),pageTab+1,value.getLength()-(pageTab+1));
		
		context.write(new Text(page), new Text(outlinks));
	}
	
}
