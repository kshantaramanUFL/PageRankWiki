package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WikiCalculateRankReducer extends Reducer<Text, Text, Text, Text> {

	private static final float dampingFactor = 0.85F;
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		boolean exists = false;
		String[] parts;
		float sumOfOtherPageRanks=0;
		String outLinks = "";
		String pageAndRank;
		
		Iterator<Text> iterator=values.iterator();
		
		while(iterator.hasNext()) {
			pageAndRank = iterator.next().toString();
			if(pageAndRank.equals("!")) {
				exists=true;
				continue;
			}
			
			if(pageAndRank.startsWith("|")) {
				outLinks = "\t"+pageAndRank.substring(1);
				continue;
			}
			
			parts = pageAndRank.split("\\t");
			float rank = Float.valueOf(parts[1]);
			int numOutlinks = Integer.valueOf(parts[2]);
			sumOfOtherPageRanks += (rank/numOutlinks);
		}

		if(!exists) return;
		int n=context.getConfiguration().getInt("n", 0);
		float newRank = dampingFactor * sumOfOtherPageRanks + ((1-dampingFactor)/n);
		context.write(key, new Text(newRank+outLinks));
	}
}