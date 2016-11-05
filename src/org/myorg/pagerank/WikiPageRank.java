/******************************************************************************//*!
* @File          WikiPageRank.java
* 
* @Title         MapReduce application for computing page rank over wikipedia pages
* 				 and for retrieving top K results accordingly.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;


/******************************************************************************
* @Class		WikiPageRank
* @Description	Class representing MapReduce implementation for computing page 
* 				rank over wikipedia pages and for retrieving top K results 
* 				accordingly.
* 				This class is an entry point for 'WikiPageRank' application.
******************************************************************************/
public class WikiPageRank {

	/* Remove contents from provided path */
	public void clean(Path path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(path);
	}
	
	/* Entry point */
	public static void main(String[] args) throws Exception {
		WikiPageRank wikiPageRank = new WikiPageRank();
		
		int i;
		int iterations;
		String dampingFactor;
		String topK;
		String input;
		String wikiLinkGraph;
		String wikiInfo;
		String wikiPageRankInitializer;
		String wikiPageRankAlgorithm;
		String iterationIn;
		String iterationOut;
		String sorterIn;
		String output;
		String handleSinkNodes;
		int status;
		
		// Read arguments from user
		iterations = Integer.parseInt(args[0]);
		dampingFactor = args[1];
		topK = args[2];
		input  = args[3];
		wikiLinkGraph = "output/MapReduce2/PageRank/WikiLinkGraph";
		wikiInfo = "output/MapReduce2/PageRank/WikiInfo";
		wikiPageRankInitializer = "output/MapReduce2/PageRank/WikiPageRankInitializer";
		wikiPageRankAlgorithm = "output/MapReduce2/PageRank/WikiPageRankAlgorithm";
		output = args[4];
		handleSinkNodes = args[5];
		
		// Generate a link graph between wikipedia pages and its outgoing links
		status = ToolRunner.run(new WikiLinkGraph(), new String[] {input, 
																   wikiLinkGraph});

		// Count the number of wikipedia pages in corpus
		status = ToolRunner.run(new WikiInfo(), new String[] {wikiLinkGraph, 
															  wikiInfo});

		// Initialize page rank score for every wikipedia page in corpus
		status = ToolRunner.run(new WikiPageRankInitializer(), new String[] {wikiLinkGraph,
																			 wikiInfo, 
																			 wikiPageRankInitializer});

		// Compute page rank of wikipedia pages using specified number of iterations
		for (i=1; i<=iterations; i++) {
			if (i == 1) {
				iterationIn = wikiPageRankInitializer;
			} else {
				iterationIn = wikiPageRankAlgorithm + "/Iteration_" + Integer.toString(i-1);
			}
			iterationOut = wikiPageRankAlgorithm + "/Iteration_" + Integer.toString(i);
			
			status = ToolRunner.run(new WikiPageRankAlgorithm(), new String[] {handleSinkNodes, 
																			   dampingFactor, 
																			   wikiInfo, 
																			   iterationIn, 
																			   iterationOut});
			
			wikiPageRank.clean(new Path(iterationIn));
		}
		
		// Retrieve top K wikipedia pages based on their page ranks
		sorterIn = wikiPageRankAlgorithm + "/Iteration_" + Integer.toString(i-1);
		status = ToolRunner.run(new WikiPageRankSorter(), new String[] {topK, 
																		sorterIn, 
																		output});
	}

}

