package PageRank;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WikiGraphReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		int n=context.getConfiguration().getInt("n",0);
		//System.out.println(line);
		double initRank = 1.0D/n;
		Iterator<Text> iterator=values.iterator();
		String rankOutlinks = initRank+"\t"+iterator.next().toString();
		context.write(key, new Text(rankOutlinks));
	}

}
