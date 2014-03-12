package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WikiRankOrderReducer extends Reducer<DoubleWritable, Text, Text, Text>{

	public void reduce(DoubleWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		Iterator<Text> iterator=values.iterator();
		context.write(new Text(iterator.next().toString()), new Text(key.toString()));
	}

}
