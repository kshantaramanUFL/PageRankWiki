package PageRank;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WikiLinksMapper extends Mapper<LongWritable, Text, Text, Text>{

	private static final Pattern LinksPattern=Pattern.compile("\\[.+?\\]");

	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException
	{
		String[] titleText = parseTitleandText(value);
		String pageName = titleText[0];
		if(pageName.contains(":"))
			return;
		Text page=new Text(pageName.replace(' ', '_'));

		Matcher patternMatcher=LinksPattern.matcher(titleText[1]);

		while(patternMatcher.find())
		{
			String otherPage = patternMatcher.group();
			otherPage = extractWikiLink(otherPage);
			if(otherPage==null || otherPage.isEmpty())
				continue;
			//output.collect(page, new Text(otherPage));
			context.write(page, new Text(otherPage));
		}
	}

	/*private boolean invalidPage(String pageName) {
		return pageName.contains(":");
	}

*/	
	private String extractWikiLink(String otherPage) {

		if(notWikiLink(otherPage)) return null;

		int start = otherPage.startsWith("[[") ? 2 : 1;
		int pipeIndex=otherPage.indexOf("|");
		int end= (pipeIndex>0) ? pipeIndex : otherPage.indexOf("]");
		/*if(pipeIndex>0)
			end=pipeIndex;
		*/int part = otherPage.indexOf("#");
		end=(part>0) ? part : end ;
		/*if(part>0)
			end=part;

*/		otherPage = otherPage.substring(start,end);
		otherPage = otherPage.replaceAll("\\s", "_");
		otherPage = otherPage.replaceAll(",", "");
		//otherPage=modify(otherPage);
		otherPage = otherPage.replace("&amp;", "&");

		return otherPage;
	}

/*	private String modify(String otherPage) {
		if(otherPage.contains("&amp;"))
			return otherPage.replace("&amp;", "&");
		return otherPage;
	}
*/
	private boolean notWikiLink(String otherPage) {

		int start = otherPage.startsWith("[[") ? 2 : 1 ;
		/*int start=1;
		if(otherPage.startsWith("[[")){
			start=2;
		}
*/
		if(otherPage.length() < start+2 || otherPage.length() > 100) return true;

		char startingChar = otherPage.charAt(start);

		if(startingChar == '.') return true;
		if(startingChar == '-') return true;
		if(startingChar == '&') return true;
		if(startingChar == '#') return true;
		if(startingChar == ',') return true;
		if(startingChar == '{') return true;
		if(startingChar == '\'') return true;

		if(otherPage.contains("&")) return true;
		if(otherPage.contains(",")) return true;
		if(otherPage.contains(":")) return true;

		return false;
	}

	private String[] parseTitleandText(Text value) throws CharacterCodingException{
		String[] titleText = new String[2];
		int start=value.find("<tit");
		int end=value.find("</tit", start);
		start+=7;

		titleText[0] = Text.decode(value.getBytes(),start,end-start);

		start=value.find("<tex");
		start=value.find(">",start);
		end = value.find("</tex",start);
		start+=1;

		if(start == -1 || end == -1)
		{
			return new String[]{"",""};
		}

		titleText[1]=Text.decode(value.getBytes(), start, end-start);

		return titleText;
	}
}

