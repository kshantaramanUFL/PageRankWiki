package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WikiPageCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	public void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> iterator=values.iterator();
		while(iterator.hasNext()) {
			sum += iterator.next().get();
		}
		context.write(key, new IntWritable(sum));
	}

}
