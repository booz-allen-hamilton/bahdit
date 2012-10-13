/**
 * Copyright 2012 Booz Allen Hamilton. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Booz Allen Hamilton licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bah.applefox.main;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.util.ToolRunner;

import com.bah.applefox.main.plugins.fulltextindex.FTAccumuloSampler;
import com.bah.applefox.main.plugins.imageindex.ImageAccumuloSampler;
import com.bah.applefox.main.plugins.imageindex.ImageLoader;
import com.bah.applefox.main.plugins.pageranking.PageRank;
import com.bah.applefox.main.plugins.utilities.AccumuloUtils;
import com.bah.applefox.main.plugins.utilities.IngesterModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * This class is used for the command line user interface. It contains all of
 * the method calls and variables necessary to do indexing and sampling of
 * images and text from web pages, web crawling, and page ranking. This class
 * can be used from the command line by passing in two arguments, the location
 * of the configuration file and the name of the command to run.
 * 
 */
public class Ingest {

	// Strings to take in from the configuration file/command line
	private static String INSTANCE_NAME, PASSWORD, USERNAME, ZK_SERVERS,
			USER_AGENT, URL_TABLE, FT_DATA_TABLE, GENERAL_STOP, FT_SAMPLE, RUN,
			SEED, FT_CHECKED_TABLE, SPLIT_SIZE, WORK_DIR, PR_TABLE_PREFIX,
			PR_URL_MAP_TABLE_PREFIX, PR_OUT_LINKS_COUNT_TABLE, PR_FILE,
			IMG_HASH_TABLE, IMG_CHECKED_TABLE, IMG_TAG_TABLE,
			IMG_HASH_SAMPLE_TABLE, IMG_TAG_SAMPLE_TABLE, FT_DIVS_FILE,
			FT_SPLIT_SIZE, IMG_SPLIT_SIZE, URL_SPLIT_SIZE, PR_SPLIT_SIZE;

	// Integers to take in from the configuration file
	private static int MAX_NGRAMS, NUM_ITERATIONS, NUM_NODES, PR_ITERATIONS;
	private static double PR_DAMPENING_FACTOR;

	// The error log
	private static Log log = LogFactory.getLog(Ingest.class);

	private static Injector injector;

	public static void main(String[] args) throws Exception {

		if(args.length == 1 && args[0].equals("--help")){
			System.out.println("Not enough arguments");
			System.out
					.println("Arguments should be in the format <properties file> <command>");
			System.out.println("Valid commands:");
			System.out.println("\tpr: Calculates Page Rank");
			System.out.println("\timageload: Loads Images from URLs");
			System.out.println("\tload: Loads Full Text Data");
			System.out.println("\tingest: Ingests URLs from given seed");
			System.out
					.println("\tftsample: Creates a Full Text Index Sample HashMap");
			System.out
					.println("\timagesample: Creates an Image Hash and Image Tag Sample HashMap");
		}
		if (args.length > 2) {
			System.out.println("2 Arguments expected, " + args.length
					+ " given.");
		}

		if (args.length < 2) {
			System.out.println("Not enough arguments");
			System.out
					.println("Arguments should be in the format <properties file> <command>");
			System.out.println("Valid commands:");
			System.out.println("\tpr: Calculates Page Rank");
			System.out.println("\timageload: Loads Images from URLs");
			System.out.println("\tload: Loads Full Text Data");
			System.out.println("\tingest: Ingests URLs from given seed");
			System.out
					.println("\tftsample: Creates a Full Text Index Sample HashMap");
			System.out
					.println("\timagesample: Creates an Image Hash and Image Tag Sample HashMap");
		}
		injector = Guice.createInjector(new IngesterModule());

		// The properties object to read from the configuration file
		Properties properties = new Properties();

		try {
			// Load configuration file from the command line
			properties.load(new FileInputStream(args[0]));
		} catch (Exception e) {
			log.error("ABORT: File not found or could not read from file ->"
					+ e.getMessage());
			log.error("Enter the location of the configuration file");
			System.exit(1);
		}

		// Initialize variables from configuration file

		// Accumulo Variables
		INSTANCE_NAME = properties.getProperty("INSTANCE_NAME");
		ZK_SERVERS = properties.getProperty("ZK_SERVERS");
		USERNAME = properties.getProperty("USERNAME");
		PASSWORD = properties.getProperty("PASSWORD");
		SPLIT_SIZE = properties.getProperty("SPLIT_SIZE");
		NUM_ITERATIONS = Integer.parseInt(properties
				.getProperty("NUM_ITERATIONS"));
		NUM_NODES = Integer.parseInt(properties.getProperty("NUM_NODES"));

		// General Search Variables
		MAX_NGRAMS = Integer.parseInt(properties.getProperty("MAX_NGRAMS"));
		GENERAL_STOP = properties.getProperty("GENERAL_STOP");

		// Full Text Variables
		FT_DATA_TABLE = properties.getProperty("FT_DATA_TABLE");
		FT_SAMPLE = properties.getProperty("FT_SAMPLE");
		FT_CHECKED_TABLE = properties.getProperty("FT_CHECKED_TABLE");
		FT_DIVS_FILE = properties.getProperty("FT_DIVS_FILE");
		FT_SPLIT_SIZE = properties.getProperty("FT_SPLIT_SIZE");

		// Web Crawler Variables
		URL_TABLE = properties.getProperty("URL_TABLE");
		SEED = properties.getProperty("SEED");
		USER_AGENT = properties.getProperty("USER_AGENT");
		URL_SPLIT_SIZE = properties.getProperty("URL_SPLIT_SIZE");

		// Page Rank Variables
		PR_TABLE_PREFIX = properties.getProperty("PR_TABLE_PREFIX");
		PR_URL_MAP_TABLE_PREFIX = properties
				.getProperty("PR_URL_MAP_TABLE_PREFIX");
		PR_OUT_LINKS_COUNT_TABLE = properties
				.getProperty("PR_OUT_LINKS_COUNT_TABLE");
		PR_FILE = properties.getProperty("PR_FILE");
		PR_DAMPENING_FACTOR = Double.parseDouble(properties
				.getProperty("PR_DAMPENING_FACTOR"));
		PR_ITERATIONS = Integer.parseInt(properties
				.getProperty("PR_ITERATIONS"));
		PR_SPLIT_SIZE = properties.getProperty("PR_SPLIT_SIZE");

		// Image Variables
		IMG_HASH_TABLE = properties.getProperty("IMG_HASH_TABLE");
		IMG_CHECKED_TABLE = properties.getProperty("IMG_CHECKED_TABLE");
		IMG_TAG_TABLE = properties.getProperty("IMG_TAG_TABLE");
		IMG_HASH_SAMPLE_TABLE = properties.getProperty("IMG_HASH_SAMPLE_TABLE");
		IMG_TAG_SAMPLE_TABLE = properties.getProperty("IMG_TAG_SAMPLE_TABLE");
		IMG_SPLIT_SIZE = properties.getProperty("IMG_SPLIT_SIZE");

		// Future Use:
		// Work Directory in HDFS
		WORK_DIR = properties.getProperty("WORK_DIR");

		// Initialize variable from command line
		RUN = args[1].toLowerCase();

		// Set the instance information for AccumuloUtils
		AccumuloUtils.setInstanceName(INSTANCE_NAME);
		AccumuloUtils.setInstancePassword(PASSWORD);
		AccumuloUtils.setUser(USERNAME);
		AccumuloUtils.setZooserver(ZK_SERVERS);
		AccumuloUtils.setSplitSize(SPLIT_SIZE);

		String[] temp = new String[25];

		// Accumulo Variables
		temp[0] = INSTANCE_NAME;
		temp[1] = ZK_SERVERS;
		temp[2] = USERNAME;
		temp[3] = PASSWORD;

		// Number of Map Tasks
		temp[4] = Integer.toString((int) Math.ceil(1.75 * NUM_NODES * 2));

		// Web Crawler Variables
		temp[5] = URL_TABLE;
		temp[6] = USER_AGENT;

		// Future Use
		temp[7] = WORK_DIR;

		// General Search
		temp[8] = GENERAL_STOP;
		temp[9] = Integer.toString(MAX_NGRAMS);

		// Full Text Variables
		temp[10] = FT_DATA_TABLE;
		temp[11] = FT_CHECKED_TABLE;

		// Page Rank Variables
		temp[12] = PR_URL_MAP_TABLE_PREFIX;
		temp[13] = PR_TABLE_PREFIX;
		temp[14] = Double.toString(PR_DAMPENING_FACTOR);
		temp[15] = PR_OUT_LINKS_COUNT_TABLE;
		temp[16] = PR_FILE;

		// Image Variables
		temp[17] = IMG_HASH_TABLE;
		temp[18] = IMG_CHECKED_TABLE;
		temp[19] = IMG_TAG_TABLE;

		temp[20] = FT_DIVS_FILE;

		// Table Split Sizes
		temp[21] = FT_SPLIT_SIZE;
		temp[22] = IMG_SPLIT_SIZE;
		temp[23] = URL_SPLIT_SIZE;
		temp[24] = PR_SPLIT_SIZE;

		if (RUN.equals("pr")) {
			// Run PR_ITERATIONS number of iterations for page ranking
			PageRank.createPageRank(temp, PR_ITERATIONS, URL_SPLIT_SIZE);
		} else if (RUN.equals("imageload")) {
			// Load image index
			AccumuloUtils.setSplitSize(URL_SPLIT_SIZE);
			ToolRunner.run(new ImageLoader(), temp);
		} else if (RUN.equals("ingest")) {
			// Ingest
			System.out.println("Ingesting");
			// Set table split size
			AccumuloUtils.setSplitSize(URL_SPLIT_SIZE);
			// Write the seed value to the table
			BatchWriter w;
			Value v = new Value();
			v.set("0".getBytes());
			Mutation m = new Mutation(SEED);
			m.put("0", "0", v);
			w = AccumuloUtils.connectBatchWrite(URL_TABLE);
			w.addMutation(m);

			for (int i = 0; i < NUM_ITERATIONS; i++) {
				// Run the ToolRunner for NUM_ITERATIONS iterations
				ToolRunner.run(CachedConfiguration.getInstance(),
						injector.getInstance(Ingester.class), temp);
			}
		} else if (RUN.equals("load")) {
			// Parse the URLs and add to the data table
			AccumuloUtils.setSplitSize(URL_SPLIT_SIZE);
			BatchWriter w = AccumuloUtils.connectBatchWrite(FT_CHECKED_TABLE);
			w.close();

			AccumuloUtils.setSplitSize(FT_SPLIT_SIZE);
			w = AccumuloUtils.connectBatchWrite(FT_DATA_TABLE);
			w.close();
			ToolRunner.run(CachedConfiguration.getInstance(),
					injector.getInstance(Loader.class), temp);
		}
		else if (RUN.equals("ftsample")) {
			// Create a sample table for full text index
			FTAccumuloSampler ftSampler = new FTAccumuloSampler(FT_SAMPLE,
					FT_DATA_TABLE, FT_CHECKED_TABLE);
			ftSampler.createSample();

		} else if (RUN.equals("imagesample")) {
			// Create a sample table for images
			ImageAccumuloSampler imgHashSampler = new ImageAccumuloSampler(
					IMG_HASH_SAMPLE_TABLE, IMG_HASH_TABLE, IMG_CHECKED_TABLE);
			imgHashSampler.createSample();

			ImageAccumuloSampler imgTagSampler = new ImageAccumuloSampler(
					IMG_TAG_SAMPLE_TABLE, IMG_TAG_TABLE, IMG_CHECKED_TABLE);
			imgTagSampler.createSample();
		} else {
			System.out.println("Invalid argument " + RUN + ".");
			System.out.println("Valid Arguments:");
			System.out.println("\tpr: Calculates Page Rank");
			System.out.println("\timageload: Loads Images from URLs");
			System.out.println("\tload: Loads Full Text Data");
			System.out.println("\tingest: Ingests URLs from given seed");
			System.out
					.println("\tftsample: Creates a Full Text Index Sample HashMap");
			System.out
					.println("\timagesample: Creates an Image Hash and Image Tag Sample HashMap");
		}

	}

}
