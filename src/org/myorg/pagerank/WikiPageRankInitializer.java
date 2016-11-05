/******************************************************************************//*!
* @File          WikiPageRankInitializer.java
* 
* @Title         MapReduce application to initialize page rank score for every
* 				 wikipedia page in corpus.
* 
* @Author        Chetan Borse
* 
* @EMail         cborse@uncc.edu
* 
* @Created on    10/31/2016
* 
*//*******************************************************************************/ 


package org.myorg.pagerank;


import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/******************************************************************************
* @Class		WikiPageRankInitializer
* @Description	Class representing MapReduce implementation to initialize 
* 				page rank score for every wikipedia page in corpus.
* 				This class implements static 'PageRankInitializerMap' and 
* 				'PageRankInitializerReducer' classes. It also holds job 
* 				configuration for 'WikiPageRankInitializer'.
******************************************************************************/
public class WikiPageRankInitializer extends Configured implements Tool {

	/* Function for truncating a given page rank score to provided precision */
	public static double getTruncatedDouble(double number, int precision) {
		return BigDecimal.valueOf(number)
						 .setScale(precision, RoundingMode.HALF_UP)
						 .doubleValue();
	}
	
	/***************************************************************************
	* @StaticClass	PageRankInitializerMap
	* @Description	Map class for transforming input splits to intermediate 
	* 				key-value representation such as 
	* 				‘<Title>, <List of outgoing links’.
	***************************************************************************/
	public static class PageRankInitializerMap extends Mapper<LongWritable, Text, Text, Text> {

		/* Overridden map method */
		@Override
		public void map(LongWritable offset, Text line, Context context)
							throws IOException, InterruptedException {
			String[] wikiPageContent;
			String title;
			String outlinks = "";

			String wikiPage = line.toString();
			
			wikiPageContent = wikiPage.split("\\$T\\$A\\$B\\$");
			
			// Extract title and outgoing links in wikipedia page
			title = wikiPageContent[0];
			if (wikiPageContent.length == 2) {
				outlinks = wikiPageContent[1];
			}
			
			// Emit '<Title>, <List of outgoing links>'
			context.write(new Text(title), new Text(outlinks.trim()));
		}
		
	}

	/***************************************************************************
	* @StaticClass	PageRankInitializerReducer
	* @Description	Reduce class to initialize page rank score for every wikipedia 
	* 				page in corpus.
	*  				The output key-value representation is as below,
	* 				‘<Title>, <Initial Page Rank, List of outgoing link>’
	***************************************************************************/
	public static class PageRankInitializerReducer extends Reducer<Text, Text, Text, Text> {

		private int wikiCorpusSize;

		/* Overridden setup method */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			wikiCorpusSize    = conf.getInt("WikiCorpusSize", 1);
		}

		/* Overridden reduce method */
		@Override 
		public void reduce(Text title, Iterable<Text> links, Context context)
								throws IOException, InterruptedException {
			double initialPageRank;
			String outlinks;

			// Initial page rank,
			// i.e. <1.0> / <Total wikipedia pages in corpus>
			initialPageRank = 1.0 / wikiCorpusSize;
			
			// Emit ‘<Title>, <Initial Page Rank, List of outgoing link>’
			for (Text link : links) {
				outlinks = link.toString().trim();
				context.write(new Text(title), 
							  new Text(Double.toString(initialPageRank) + "$#$" + outlinks));
			}
		}
		
	}

	/**************************************************************************
	* @Function		run
	* @Description	Driver function for configuring MapReduce job.
	* @Input		String[]	args	String array of size 3. It consists 
	* 									input path of link graph over wikipedia 
	* 									pages, input path of wikipedia information
	* 									and output path for storing a initialized 
	* 									page ranks.
	* @Return		int					Returns integer.
	***************************************************************************/
	public int run(String[] args) throws Exception {
		// Extract wikipedia information
		WikiInfo info = new WikiInfo();
		String[] wikiInfo = info.getWikiInfo(args[1]);
		
		Configuration conf = new Configuration();
		conf.setInt("WikiCorpusSize", Integer.parseInt(wikiInfo[0]));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "$T$A$B$");
		conf.set("mapreduce.output.textoutputformat.separator", "$T$A$B$");

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName(" Wiki Page Rank Initializer ");
		job.setMapperClass(PageRankInitializerMap.class);
		job.setReducerClass(PageRankInitializerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

}

