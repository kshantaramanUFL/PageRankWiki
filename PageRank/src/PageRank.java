package PageRank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
public class PageRank {

	private static NumberFormat numFormat = new DecimalFormat("0");
	public static void main(String[] args) throws Exception
	{
		PageRank pageRank= new PageRank();

		String bucketName=(args[0].contains("s3n"))? args[0] : ("s3n://"+args[0]);
		pageRank.parseXml("s3://spring-2014-ds/data/enwiki-latest-pages-articles.xml",bucketName+"/tmp/xmlp/");
		pageRank.nCalculate(bucketName+"/tmp/xmlp/",bucketName+"/tmp/nc/");
		System.out.println("Job nCalculate completed");


		Path pt=new Path(bucketName+"/tmp/nc/part-r-00000");
		FileSystem fs=null;
		fs = FileSystem.get(new URI(bucketName) ,new Configuration());
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		System.out.println("File Opened");
		String line=br.readLine();
		line=line.replaceAll("[^0-9]", "");
		System.out.println(line);
		int numOfLines=Integer.parseInt(line);
		br.close();
		pageRank.createGraph(bucketName+"/tmp/xmlp/",bucketName+"/tmp/run0/",numOfLines);
		System.out.println("CreateGraphCompleted");
		for(int run = 0; run < 8; run++) {
			pageRank.calculateRank(bucketName+"/tmp/run"+numFormat.format(run),bucketName+"/tmp/run"+numFormat.format(run+1),numOfLines);
		}

		pageRank.rankOrder(bucketName+"/tmp/run1",bucketName+"/tmp/results1",1,numOfLines);
		pageRank.rankOrder(bucketName+"/tmp/run8",bucketName+"/results",2,numOfLines);


		fs.rename(new Path(bucketName+"/tmp/xmlp/part-r-00000"),new Path(bucketName+"/results/PageRank.outlink.out"));
		fs.rename(new Path(bucketName+"/tmp/nc/part-r-00000"),new Path(bucketName+"/results/PageRank.n.out"));
		fs.rename(new Path(bucketName+"/tmp/results1/part-r-00000"),new Path(bucketName+"/results/PageRank.iter1.out"));
		fs.rename(new Path(bucketName+"/results/part-r-00000"),new Path(bucketName+"/results/PageRank.iter8.out"));
	}
	private void rankOrder(String inputPath, String outputPath, int iter, int numOfLines) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf= new Configuration();

		conf.setInt("n", numOfLines);
		Job job=new Job(conf,"RankOrder");
		job.setJarByClass(PageRank.class);

		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		TextOutputFormat.setOutputPath(job,new Path(outputPath));

		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(WikiRankOrderMapper.class);
		job.setReducerClass(WikiRankOrderReducer.class);
		job.setNumReduceTasks(1);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job.waitForCompletion(true);
	}
	private void createGraph(String inputPath, String outputPath, int numOfLines) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Inside Create Graph");
		Configuration conf = new Configuration();

		conf.setInt("n", numOfLines);
		Job job=new Job(conf);

		job.setJarByClass(PageRank.class);

		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		TextOutputFormat.setOutputPath(job,new Path(outputPath));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(WikiGraphMapper.class);
		job.setReducerClass(WikiGraphReducer.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(false);
	}

	private void calculateRank(String inputPath, String outputPath, int numOfLines) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.setInt("n", numOfLines);

		Job job=new Job(conf,"RankCalculate");

		job.setJarByClass(PageRank.class);

		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		TextOutputFormat.setOutputPath(job,new Path(outputPath));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(WikiCalculateRankMapper.class);
		job.setReducerClass(WikiCalculateRankReducer.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
	}
	private void nCalculate(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job=new Job(conf,"NCalculate"); 

		job.setJarByClass(PageRank.class);
		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		TextOutputFormat.setOutputPath(job,new Path(outputPath));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(WikiPageCountMapper.class);
		job.setCombinerClass(WikiPageCountReducer.class);
		job.setReducerClass(WikiPageCountReducer.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
	}
	private void parseXml(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException{

		Configuration conf= new Configuration();

		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		Job job=new Job(conf,"xml parse");
		job.setJarByClass(PageRank.class);
		job.setInputFormatClass(XmlInputFormat.class);


		FileInputFormat.setInputPaths(job, new Path(inputPath));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(WikiLinksMapper.class);
		job.setReducerClass(WikiLinksReducer.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
	}
}