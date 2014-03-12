package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WikiPageCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text("N=");
	
	public void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		//output.collect(word, one);
		context.write(word, one);
	}

	
	
}
