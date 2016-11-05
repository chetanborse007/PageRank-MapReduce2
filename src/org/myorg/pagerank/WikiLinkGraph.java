/******************************************************************************//*!
* @File          WikiLinkGraph.java
* 
* @Title         MapReduce application for generating a link graph between wikipedia 
* 				 pages and its outgoing links.
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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
* @Class		WikiLinkGraph
* @Description	Class representing MapReduce implementation for generating a 
* 				link graph between wikipedia pages and its outgoing links.
* 				This class encloses static 'LinkGraphMap' and 'LinkGraphReducer' 
* 				classes. It also holds job configuration for 'WikiLinkGraph'.
******************************************************************************/
public class WikiLinkGraph extends Configured implements Tool {

	/***************************************************************************
	* @StaticClass	LinkGraphMap
	* @Description	Map class for transforming wikipedia pages to intermediate 
	* 				key-value representation such as,
	* 				‘<Title>, <Outgoing link>’.
	***************************************************************************/
	public static class LinkGraphMap extends Mapper<LongWritable, Text, Text, Text> {

		/* Patterns for matching title, text and outgoing links */
		private static final Pattern TITLE = Pattern.compile(Pattern.quote("<title>") +
															 "(.*?)" +
															 Pattern.quote("</title>"));
		private static final Pattern TEXT = Pattern.compile(Pattern.quote("<text") +
															".*" +
															Pattern.quote(">") +
															"(.*?)" +
															Pattern.quote("</text>"));
		private static final Pattern OUTLINK = Pattern.compile("\\[\\[(.*?)\\]\\]");
		
		/* Overridden map method */
		@Override
		public void map(LongWritable offset, Text line, Context context)
							throws IOException, InterruptedException {
			String title;
			String text;
			String outlink;

			// Parse wikipedia page
			String wikiPage = line.toString();

			// Extract a valid title from wikipedia page
			title = extractTitle(wikiPage);
			if (title == null) {
				return;
			}

			// Extract a valid text body from wikipedia page
//			text = extractText(wikiPage);
//			if (text == null) {
//				return;
//			}

			// Extract outgoing links
			Matcher outlinkMatcher = OUTLINK.matcher(wikiPage);
			while (outlinkMatcher.find()) {
				outlink = outlinkMatcher.group(1);
				
				outlink = outlink.trim();
				if (outlink.isEmpty()) {
					continue;
				}
				
				// Remove '[[' or ']]' characters from outgoing links
				outlink = outlink.replace("[[", "").replace("]]", "");
				
				// Emit '<Title>, <Outgoing link>'
				context.write(new Text(title), new Text(outlink));
				
				// Emit '<Outgoing link>, <"">'.
				// This step should be performed in order to handle sink nodes, 
				// which do not have outgoing links at all; in fact for handling
				// links, which are not crawled at all.
				context.write(new Text(outlink), new Text(""));
			}
			
			// Emit '<Title>, <"">'.
			// This step should be performed in order to handle sink nodes; 
			// which are crawled, but do not have any outgoing link.
			context.write(new Text(title), new Text(""));
		}
		
		/* Function for extracting a valid title from wikipedia page */
		public String extractTitle(String wikiPage) {
			String title = null;
			
			Matcher titleMatcher = TITLE.matcher(wikiPage);
			
			if (titleMatcher.find()) {
				title = titleMatcher.group(1);
				
				title = title.trim();
				if (title.isEmpty()) {
					title = null;
				} 
			}
			
			return title;
		}
		
		/* Function for extracting a valid text body from wikipedia page */
		public String extractText(String wikiPage) {
			String text = null;
			
			Matcher textMatcher = TEXT.matcher(wikiPage);
			
			if (textMatcher.find()) {
				text = textMatcher.group(1);
				
				text = text.trim();
				if (text.isEmpty()) {
					text = null;
				} 
			}
			
			return text;
		}

	}

	/***************************************************************************
	* @StaticClass	LinkGraphReducer
	* @Description	Reduce class for generating a link graph between wikipedia pages
	*  				and its outgoing links.
	*  				The output key-value representation is as below,
	* 				‘<Title>, <List of outgoing link>’
	***************************************************************************/
	public static class LinkGraphReducer extends Reducer<Text, Text, Text, Text> {

		/* Overridden reduce method */
		@Override 
		public void reduce(Text title, Iterable<Text> links, Context context)
								throws IOException, InterruptedException {
			List<String> outlinks = new ArrayList<String>();

			// Iterate through a list of tokens and remove empty tokens.
			for (Text link : links) {
				if (link.toString().isEmpty()) {
					continue;
				}
				
				outlinks.add(link.toString());
			}

			context.write(title, new Text(String.join("$#$", outlinks)));
		}
		
	}

	/**************************************************************************
	* @Function		run
	* @Description	Driver function for configuring MapReduce job.
	* @Input		String[]	args	String array of size 2. It consists 
	* 									input path of wikipedia pages to be 
	* 									ranked and output path for storing a 
	* 									link graph.
	* @Return		int					Returns integer.
	***************************************************************************/
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "$T$A$B$");
		conf.set("mapreduce.output.textoutputformat.separator", "$T$A$B$");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName(" Wiki Link Graph Generator ");
		job.setMapperClass(LinkGraphMap.class);
		job.setReducerClass(LinkGraphReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

}

