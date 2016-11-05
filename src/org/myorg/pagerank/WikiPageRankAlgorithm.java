/******************************************************************************//*!
* @File          WikiPageRankAlgorithm.java
* 
* @Title         MapReduce application for computing page rank of wikipedia pages.
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
* @Class		WikiPageRankAlgorithm
* @Description	Class representing MapReduce implementation for computing 
* 				page rank of wikipedia pages.
* 				This class implements static 'PageRankAlgoMap' and 
* 				'PageRankAlgoReducer' classes. It also holds job configuration 
* 				for 'WikiPageRankAlgorithm'.
******************************************************************************/
public class WikiPageRankAlgorithm extends Configured implements Tool {

	/* Function for truncating a given page rank score to provided precision */
	public static double getTruncatedDouble(double number, int precision) {
		return BigDecimal.valueOf(number)
						 .setScale(precision, RoundingMode.HALF_UP)
						 .doubleValue();
	}

	/**************************************************************************
	* @StaticClass	PageRankAlgoMap
	* @Description	Map class for transforming input splits to intermediate 
	* 				key-value representation such as 
	* 				‘<Outgoing link>, <Page rank contribution>’ and
	* 				‘<Title>, <List of outgoing links>’.
	**************************************************************************/
	public static class PageRankAlgoMap extends Mapper<LongWritable, Text, Text, Text> {

		private boolean handleSinkNodes;
		private String wikiCorpusList;

		/* Overridden setup method */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			handleSinkNodes    = conf.getBoolean("HandleSinkNodes", false);
			if (handleSinkNodes) {
				wikiCorpusList    = conf.get("WikiCorpusList");
			}
		}

		/* Overridden map method */
		@Override
		public void map(LongWritable offset, Text line, Context context)
							throws IOException, InterruptedException {
			String title;
			String wikiPageInfo;
			String[] info;
			double pageRank;
			String[] outlinks = null;
			double contribution;

			String wikiPage = line.toString();
			
			// Extract title of wikipedia page
			title = wikiPage.split("\\$T\\$A\\$B\\$")[0];
			wikiPageInfo = wikiPage.split("\\$T\\$A\\$B\\$")[1];
			
			// Extract current page rank and outgoing links in wikipedia page
			info = wikiPageInfo.split("\\$\\#\\$", 2);
			pageRank = Double.parseDouble(info[0]);
			if (info.length > 1 && !info[1].isEmpty()) {
				outlinks = info[1].split("\\$\\#\\$");
			} else if (handleSinkNodes) {
				outlinks = wikiCorpusList.trim().split("\\$\\#\\$");
			}
			
			// Distribute current page rank equally to every outgoing link
			if ((info.length > 1 && !info[1].isEmpty()) || (handleSinkNodes)) {
				contribution = pageRank / outlinks.length;
			
				for (String outlink : outlinks) {
					context.write(new Text(outlink), 
								  new Text(Double.toString(contribution)));
				}
			}
			
			// Also, emit ‘<Title>, <List of outgoing links>’
			if (info.length > 1 && !info[1].isEmpty()) {
				context.write(new Text(title), new Text("##$##" + info[1]));
			} else {
				context.write(new Text(title), new Text("##$##"));
			}
		}
		
	}

	/**************************************************************************
	* @StaticClass	PageRankAlgoReducer
	* @Description	Reduce class for computing page rank of wikipedia pages.
	*  				The output key-value representation is as below,
	* 				‘<Title>, <New Page Rank, List of outgoing link>’
	**************************************************************************/
	public static class PageRankAlgoReducer extends Reducer<Text, Text, Text, Text> {

		private double dampingFactor;
		private int wikiCorpusSize;

		/* Overridden setup method */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dampingFactor    = conf.getDouble("DampingFactor", 1.0);
			wikiCorpusSize    = conf.getInt("WikiCorpusSize", 1);
		}

		/* Overridden reduce method */
		@Override 
		public void reduce(Text title, Iterable<Text> tokens, Context context)
								throws IOException, InterruptedException {
			String info;
			double contributions = 0.0;
			String outlinks = "";
			double pageRank;

			// Iterate through a list of tokens
			for (Text token : tokens) {
				info = token.toString().trim();
				
				// If given token is page rank contribution, then add it
				// to sum of contributions.
				// If not, store it as outlinks.
				try {
					contributions += Double.parseDouble(info);
				} catch(NumberFormatException e) {
					outlinks = info.split("\\#\\#\\$\\#\\#", 2)[1];
				}
			}
			
			// Compute new page rank
			pageRank = (1.0 - dampingFactor) + (dampingFactor * contributions);
			
			// Emit ‘<Title>, <New Page Rank, List of outgoing link>’
			context.write(new Text(title), new Text(Double.toString(pageRank) + "$#$" + outlinks));
		}
		
	}

	/**************************************************************************
	* @Function		run
	* @Description	Driver function for configuring MapReduce job.
	* @Input		String[]	args	String array of size 5. It consists 
	* 									flag for handling sink nodes, damping
	* 									factor, input path of wikipedia information,
	* 									input path of page ranks from previous
	* 									iteration and output path for storing 
	* 									new page ranks.
	* @Return		int					Returns integer.
	***************************************************************************/
	public int run(String[] args) throws Exception {
		// Extract wikipedia information
		WikiInfo info = new WikiInfo();
		String[] wikiInfo = info.getWikiInfo(args[2]);

		Configuration conf = new Configuration();
		conf.setBoolean("HandleSinkNodes", Boolean.parseBoolean(args[0]));
		conf.setDouble("DampingFactor", Double.parseDouble(args[1]));
		conf.setInt("WikiCorpusSize", Integer.parseInt(wikiInfo[0]));
		conf.set("WikiCorpusList", wikiInfo[1]);
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "$T$A$B$");
		conf.set("mapreduce.output.textoutputformat.separator", "$T$A$B$");

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName(" Wiki Page Rank Algorithm ");
		job.setMapperClass(PageRankAlgoMap.class);
		job.setReducerClass(PageRankAlgoReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPaths(job, args[3]);
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

}

