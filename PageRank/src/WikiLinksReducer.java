package PageRank;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WikiLinksReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		String outlinkGraph = "";
		Iterator<Text> iterator=values.iterator();
		boolean initial=true;
		while(iterator.hasNext()){
			if(!initial) outlinkGraph+=" ";

			outlinkGraph+=iterator.next().toString();
			initial=false;
		}
		context.write(key, new Text(outlinkGraph));
	}
}