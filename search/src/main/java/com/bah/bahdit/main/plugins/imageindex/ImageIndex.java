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
package com.bah.bahdit.main.plugins.imageindex;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.servlet.ServletContext;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.search.spell.SpellChecker;

import com.bah.bahdit.main.plugins.fulltextindex.data.SearchResults;
import com.bah.bahdit.main.plugins.fulltextindex.utils.Utils;
import com.bah.bahdit.main.plugins.imageindex.utils.ImageHasher;
import com.bah.bahdit.main.plugins.imageindex.utils.SimiliarImageRanker;
import com.bah.bahdit.main.plugins.index.Index;
import com.bah.bahdit.main.search.Search;
import com.bah.bahdit.main.search.utils.LevenshteinDistance;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;


/**
 * Search by using an image index table.  The query can either be a url, file, 
 * or just text. A url or file will be hashed and searched in the image hash table.
 * Text will be looked up in the tag table. 
 * 
 * In the event that a threshold specified in the properties file is in met, fuzzy query
 * will take over and try and find relevant tags and/or hashes.
 */
public class ImageIndex implements Index {

	private static Log log = LogFactory.getLog(ImageIndex.class);

	// Constants to pull stuff out of the properties file
	public static final String IMG_HASH_TABLE = "IMG_HASH_TABLE";
	public static final String IMG_TAG_TABLE = "IMG_TAG_TABLE";
	public static final String IMG_CHECKED_TABLE = "IMG_CHECKED_TABLE";
	public static final String IMG_TAG_SAMPLE_TABLE = "IMG_HASH_SAMPLE_TABLE";
	public static final String IMG_TAG_THRESHOLD = "IMG_TAG_THRESHOLD";
	public static final String IMG_TAG_SUGGESTION_NUMBER = "IMG_TAG_SUGGESTION_NUMBER";
	public static final String IMG_HASH_DISTANCE = "IMG_HASH_DISTANCE";

	public static final String ENCODING = "ISO-8859-1";

	// General fields for the Image Index
	private Connector conn; 
	private Properties properties;
	private SpellChecker tagSpellChecker;
	private HashMap<String, Integer> tagSampleTable;
	private int imgHashingDistance;
	private int doSimilar;

	/**
	 * Get the properties from the configuration file and store for search
	 *
	 * @param conn - Connector
	 * @param properties - properties from config file
	 * @param context - passed from servlet
	 */
	@Override
	public void configure(Connector conn, Properties properties, ServletContext context) {
		this.properties = properties;
		this.conn = conn;
		loadResources(context);		
		imgHashingDistance = Integer.parseInt(properties.getProperty(IMG_HASH_DISTANCE));

	}

	/**
	 * Search by using an image index table.  The query can either be a url, file, 
	 * or just text. A url or file will be hashed and searched in the image hash table.
	 * Text will be looked up in the tag table. 
	 * 
	 * In the event that a threshold specified in the properties file is in met, fuzzy query
	 * will take over and try and find relevant tags and/or hashes.
	 * 
	 * @param strQuery - query specified by the servlet
	 * @param page - the page requested by the servlet (not implemented)
	 * @param resultsPerPage - the number of results requested (not implemented)
	 * 
	 * @return a SearchResults object, containing :
	 * - a ArrayList of strings for each image -> loc + "[ ]" + URL
	 * - The total number of results found (not necessarily returned)
	 * - Total time needed to find the results
	 */
	@Override
	public SearchResults search(Object query, int page, int resultsPerPage) {

		String mainQuery = "";
		
		this.doSimilar = page;

		long start = System.nanoTime();

		HashSet<String> hashRanges;

		boolean isURL = (query instanceof String) && ((String)query).contains("http://");

		// skip straight to hashing
		if (query instanceof File || isURL) {
			hashRanges = new HashSet<String>();
			try {
				mainQuery = ImageHasher.hash(query);
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			hashRanges.add(mainQuery);
		}

		// look through tags before hashing
		else {

			mainQuery = (String)query;

			// create scanner to get image hashes based on tags and IDs
			BatchScanner tagScanner = createImageTagScanner();

			// get a set of the ranges to look through to find the hashes we want
			hashRanges = getTag(tagScanner, mainQuery);
		}

		if(hashRanges == null)
			return new SearchResults(null, "", 0);

		// create scanner to get image info based on hashes
		BatchScanner hashScanner = createHashScanner();

		// get the image information and return
		HashSet<String> results = getHash(hashScanner, hashRanges);

		// create searchResults based on returned images
		ArrayList<String> r = new ArrayList<String>(results);
		int numResults = r.size();

		String correction = "";
		long elapsedTime = System.nanoTime() - start;

		SearchResults finalResults = new SearchResults(r, correction, numResults, elapsedTime);

		return finalResults;
	}

	/**
	 * Creates a batch scanner on the hash table
	 */
	private BatchScanner createHashScanner() {
		// initialize the main scanner that looks through the main table
		BatchScanner hashScanner = null;


		// set which table to scan through
		String imageTable = properties.getProperty(IMG_HASH_TABLE);

		// makes sure table has been created
		if(!conn.tableOperations().exists(imageTable)) {
			log.error("FATAL: The tables do not exist. Please run ingest."); 
			System.exit(1);
		}

		// create the base scanner
		try {
			hashScanner = conn.createBatchScanner(imageTable, new Authorizations(), 10);
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
		}

		return hashScanner;
	}

	/**
	 * Creates a batch scanner on the image tag table
	 */
	private BatchScanner createImageTagScanner() {

		// initialize the main scanner that looks through the main table
		BatchScanner tagScanner = null;

		// set which table to scan through
		String imageTable = properties.getProperty(IMG_TAG_TABLE);

		// makes sure table has been created
		if(!conn.tableOperations().exists(imageTable)) {
			log.error("FATAL: The tables do not exist. Please run ingest."); 
			System.exit(1);
		}

		// create the base scanner
		try {
			tagScanner = conn.createBatchScanner(imageTable, new Authorizations(), 10);
		} catch (TableNotFoundException e) {
			log.error(e.getMessage());
		}

		return tagScanner;
	}

	/**
	 * Query the hash table for the specified ranges
	 * If the not enough pictures are found then fuzzy query kicks in
	 * 
	 * @param scanner - scanner to the hash table
	 * @param hashRanges - ranges to search for
	 * @return - a set of strings -> loc + "[ ]" + URL
	 */
	public HashSet<String> getHash(BatchScanner scanner, HashSet<String> hashRanges) {

		HashSet<String> results = new HashSet<String>();

		if(doSimilar == 1){

			String hashRangesString = null;
			try {
				hashRangesString = new String(Utils.serialize(hashRanges), ENCODING);
			} catch (UnsupportedEncodingException e) {
				log.error(e.getMessage());
			} catch (IOException e) {
				log.error(e.getMessage());
			}

			Map<String,String> iteratorProperties = new HashMap<String, String>();
			iteratorProperties.put(Search.QUERY, hashRangesString);
			iteratorProperties.put(IMG_HASH_DISTANCE, String.valueOf(imgHashingDistance));
			IteratorSetting cfg = new IteratorSetting(10, SimiliarImageRanker.class, iteratorProperties);
			scanner.addScanIterator(cfg);
			
			
			HashSet<Range> ranges = new HashSet<Range>();
			ranges.add(new Range());
			scanner.setRanges(ranges);
			
		} else {
			HashSet<Range> ranges = new HashSet<Range>();

			for(String s : hashRanges)
				ranges.add(new Range(s));

			scanner.setRanges(ranges);
		}

		// scan for hashes with the specified ranges
		for(Entry<Key, Value> entry : scanner) {
			Key key = entry.getKey();
			String loc = key.getColumnFamily().toString();
			String URL = key.getColumnQualifier().toString();
			String info = loc + "[ ]" + URL;
			results.add(info);
		}

		return results;
	}

	/**
	 * Gets tags from the query and returns the hashes
	 * associated with those tags.
	 * 
	 * @param tagScanner - batch scanner to the tag table
	 * @param query - the query specified by the user
	 * @return - hashes found that satisfy the query
	 */
	private HashSet<String> getTag(BatchScanner tagScanner, String query) {


		List<Range> ranges = new ArrayList<Range>();
		for (String s : query.split(" ")){
			ranges.add(new Range(s));
		}
		tagScanner.setRanges(ranges);


		HashSet<String> hashRanges = new HashSet<String>();
		for(Entry<Key,Value> e : tagScanner){
			hashRanges.add(new String(e.getValue().get()));
		}

		if(hashRanges.size() == 0){
			String[] suggestions = { query };
			try{
				for(String q : query.split("\\s")){
					suggestions = (String[]) ArrayUtils.addAll(suggestions, tagSpellChecker.suggestSimilar(q, 1));  
				}
			} catch (IOException e){
				log.warn(e.getMessage());
			}
			for(Entry<Key,Value> e : tagScanner){
				hashRanges.add(new String(e.getValue().get()));
			}
			if(hashRanges.size() == 0)
				return null;
		}

		return hashRanges;
	}

	/**
	 * Get an arraylist of visualization information for the front end
	 * 
	 * @param searchResults - the returned page of search results
	 * @return an ArrayList containing the visualization information separately
	 */
	@Override
	public ArrayList<?> getVisualizations(SearchResults searchResults) {
		return null;
	}

	/**
	 * Loads the necessary files from the properties file
	 * called from FullTextIndex.configure
	 * 
	 * @param context - passed from the servlet
	 */
	@SuppressWarnings("unchecked")
	private void loadResources(ServletContext context) {
		try {
			// get the sample table from resources
			InputStream sample = context.getResourceAsStream(properties.getProperty(IMG_TAG_SAMPLE_TABLE));
			InputStream samplebuffer = new BufferedInputStream( sample );
			ObjectInput objectsample;
			objectsample = new ObjectInputStream ( samplebuffer );
			tagSampleTable = (HashMap<String, Integer>)objectsample.readObject();

			tagSpellChecker = LevenshteinDistance.createSpellChecker(context, tagSampleTable);

		} catch (IOException e) {
			log.warn(e.getMessage());
		} catch (ClassNotFoundException e) {
			log.warn(e.getMessage());
		}
	}

}
