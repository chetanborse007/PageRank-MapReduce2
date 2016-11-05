/******************************************************************************//*!
* @File          WikiPageRankSorter.java
* 
* @Title         MapReduce application for retrieving top K wikipedia pages based
* 				 on their page ranks.
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
* @Class		WikiPageRankSorter
* @Description	Class representing MapReduce implementation for retrieving 
* 				top K wikipedia pages based on their page ranks.
* 				This class implements static 'PageRankSorterMap' and 
* 				'PageRankSorterReducer' classes. It also holds job configuration
* 				for 'WikiPageRankSorter'.
******************************************************************************/
public class WikiPageRankSorter extends Configured implements Tool {

	/**************************************************************************
	* @StaticClass	ValueComparator
	* @Description	Class for comparing page ranks.
	***************************************************************************/
	public static class ValueComparator implements Comparator<Text> {
		Map<Text, Text> base;
		
		public ValueComparator(Map<Text, Text> base) {
			this.base = base;
		}
		
		public int compare(Text a, Text b) {
			if (Double.parseDouble(base.get(a).toString()) >= 
					Double.parseDouble(base.get(b).toString())) {
				return -1;
			} else {
				return 1;
			}
		}
	}
	
	/***************************************************************************
	* @StaticClass	PageRankSorterMap
	* @Description	Map class for transforming input splits to intermediate 
	* 				key-value representation such as 
	* 				‘<Title>, <Page Rank>’.
	***************************************************************************/
	public static class PageRankSorterMap extends Mapper<LongWritable, Text, Text, Text> {

		private int wikiResultSize;
		private static final HashMap<Text, Text> pageRankMap = new HashMap<Text, Text>();

		/* Overridden setup method */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			wikiResultSize    = conf.getInt("WikiResultSize", 100);
		}
		
		/* Overridden map method */
		@Override
		public void map(LongWritable offset, Text line, Context context)
							throws IOException, InterruptedException {
			String title;
			String wikiPageInfo;
			String[] info;
			String pageRank;
			String outlinks;

			String wikiPage = line.toString();
			
			// Extract title of wikipedia page
			title = wikiPage.split("\\$T\\$A\\$B\\$")[0];
			wikiPageInfo = wikiPage.split("\\$T\\$A\\$B\\$")[1];
			
			// Extract page rank of wikipedia page
			info = wikiPageInfo.split("\\$\\#\\$", 2);
			pageRank = info[0];
			outlinks = info[1];
			
			// Store ‘<Title>, <Page Rank>’ into HashMap
			pageRankMap.put(new Text(title), new Text(pageRank));
		}
		
		/* Overridden cleanup method for emitting only top K results */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Create an instance of customized comparator
			ValueComparator cmp = new ValueComparator(pageRankMap);

			// Sort HashMap in decreasing order of page ranks
			TreeMap<Text, Text> sortedPageRankMap = new TreeMap<Text, Text>(cmp);
			sortedPageRankMap.putAll(pageRankMap);
			
			// Emit only top k pairs
			int count = 0;
			for (Text title : sortedPageRankMap.keySet()) {
				if (count == wikiResultSize) {
					break;
				}
				count++;
				
				context.write(title, pageRankMap.get(title));
			}
		}
		
	}

	/***************************************************************************
	* @StaticClass	PageRankSorterReducer
	* @Description	Reduce class for retrieving top K wikipedia pages based on 
	* 				their page ranks.
	***************************************************************************/
	public static class PageRankSorterReducer extends Reducer<Text, Text, Text, Text> {

		private int wikiResultSize;
		private static final HashMap<Text, Text> pageRankMap = new HashMap<Text, Text>();

		/* Overridden setup method */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			wikiResultSize    = conf.getInt("WikiResultSize", 100);
		}
		
		/* Overridden reduce method */
		@Override 
		public void reduce(Text title, Iterable<Text> pageRanks, Context context)
								throws IOException, InterruptedException {
			// Store ‘<Title>, <Page Rank>’ into HashMap
			for (Text pageRank : pageRanks) {
				pageRankMap.put(new Text(title), new Text(pageRank));
			}
		}
		
		/* Overridden cleanup method for emitting only top K results */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Create an instance of customized comparator
			ValueComparator cmp = new ValueComparator(pageRankMap);
			
			// Sort HashMap in decreasing order of page ranks
			TreeMap<Text, Text> sortedPageRankMap = new TreeMap<Text, Text>(cmp);
			sortedPageRankMap.putAll(pageRankMap);
			
			// Emit only top k pairs
			int count = 0;
			for (Text title : sortedPageRankMap.keySet()) {
				if (count == wikiResultSize) {
					break;
				}
				count++;
				
				context.write(title, pageRankMap.get(title));
			}
		}
		
	}

	/**************************************************************************
	* @Function		run
	* @Description	Driver function for configuring MapReduce job.
	* @Input		String[]	args	String array of size 3. It consists 
	* 									top K to be retrieved, input path of
	* 									page ranks to be sorted and output path
	* 									for storing top K results.
	* @Return		int					Returns integer.
	***************************************************************************/
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("WikiResultSize", Integer.parseInt(args[0]));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "$T$A$B$");
		conf.set("mapreduce.output.textoutputformat.separator", "\t");

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName(" Wiki Page Rank Sorter ");
		job.setMapperClass(PageRankSorterMap.class);
		job.setReducerClass(PageRankSorterReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

}

