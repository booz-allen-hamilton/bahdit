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
package com.bah.bahdit.main.plugins.fulltextindex;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.servlet.ServletContext;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.lucene.search.spell.SpellChecker;

import com.bah.bahdit.main.plugins.fulltextindex.data.EdgeLinks;
import com.bah.bahdit.main.plugins.fulltextindex.data.FixedSizePQ;
import com.bah.bahdit.main.plugins.fulltextindex.data.SearchResults;
import com.bah.bahdit.main.plugins.fulltextindex.data.Term;
import com.bah.bahdit.main.plugins.fulltextindex.data.TermComparator;
import com.bah.bahdit.main.plugins.fulltextindex.iterators.DocumentRanker;
import com.bah.bahdit.main.plugins.fulltextindex.iterators.RankCalculator;
import com.bah.bahdit.main.plugins.fulltextindex.utils.Utils;
import com.bah.bahdit.main.plugins.index.Index;
import com.bah.bahdit.main.search.Search;
import com.bah.bahdit.main.search.utils.LevenshteinDistance;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * A plugin for Index that configures, loads, and searches for a string query.  
 * This plugin is used for a corpus of documents whose text is  parsed into a 
 * document-partitioned index Accumulo table.  This class is called by 
 * Search.class.
 */
public class FullTextIndex implements Index {

  // Names of Properties in the .properties file
  public static final String FT_TABLE_NAME = "FT_DATA_TABLE";
  public static final String FT_SAMPLE = "FT_SAMPLE";
  public static final String PR_FILE = "PR_FILE";

  // Encoding type to serialize hashmap into string
  public static final String ENCODING = "ISO-8859-1";

  // Priorities for different iterators
  private static final int RANK_CALCULATOR_PRIORITY = 10;
  private static final int DOCUMENT_RANKER_PRIORITY = 11;
  private static final String NUM_RESULTS = "[NUM_RESULTS]";

  // regex for removing all characters from a string except numbers
  private static final String KEEP_NUMBERS = "[^0-9]";

  private static Log log = LogFactory.getLog(FullTextIndex.class);

  private static HashMap<String, SoftReference<SearchResults>> searchResultsCache;
  private SpellChecker spellChecker = null;
  private HashSet<String> stopWords = null;
  private HashMap<String, Integer> sampleTable = null;
  private HashMap<String, Double> pagerankTable = null;
  private Properties properties;
  private Connector conn;  

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
    searchResultsCache = new HashMap<String, SoftReference<SearchResults>>();
  }

  /**
   * Search by using a full text index table.  The entire text body of the 
   * website is analyzed against the query and used to provide a ranking for 
   * all the websites against the query.  Only returns the number of results 
   * requested, and only for a certain page.  Assumes that all pages have the 
   * same number of results. 
   * (i.e. page 4, 10 results per page => results ranked 31-40)
   * 
   * @param strQuery - string query to be looked up in the table
   * @param page - the page requested by the servlet
   * @param resultsPerPage - the number of results requested
   * 
   * @return a SearchResults object, containing :
   * - ArrayList of URL info (rank, title, url)
   * - One possible correction for the query (if there are no results)
   * - The total number of results found (not necessarily returned)
   * - Total time needed to find the results
   */
  @Override
  public SearchResults search(Object strQuery, int page, int resultsPerPage) {

    // used as base starting time to calculate time spent searching for results
    long startTime = System.nanoTime();

    // we assume that the query is a string of terms
    String query = (String)strQuery;

    // checks if the query is in the memory cache
    // if so, get the results straight from the cache instead of accumulo
    String cacheString = query + String.valueOf(page);
    if(searchResultsCache.containsKey(cacheString)) {
      SearchResults s = searchResultsCache.get(cacheString).get();
      if (s != null) {
        // save time needed to get from cache
        s.setTime(System.nanoTime() - startTime);
        return s;
      }
      // get rid of any keys that point to null
      else searchResultsCache.remove(cacheString);
    }

    // initialize the scanner that looks through the main table
    Scanner mainScanner = null;


    // set the main table to scan through
    String mainTable = properties.getProperty(FT_TABLE_NAME);

    // makes sure table has been created
    // if not, close program and exit
    if(!conn.tableOperations().exists(mainTable)) {
      log.error("FATAL: The tables do not exist. Please run ingest."); 
      System.exit(1);
    }
    try {
      mainScanner = conn.createScanner(mainTable, new Authorizations());
    } catch (TableNotFoundException e) {
      log.error(e.getMessage());
    }

    // store the sampling table for tf-idf calculations
    String sampleTableString = null;
    try {
      sampleTableString = new String(Utils.serialize(sampleTable), ENCODING);
    } catch (UnsupportedEncodingException e) {
      log.error(e.getMessage());
    } catch (IOException e) {
      log.error(e.getMessage());
    }


    // store the pagerank table for pagerank calculations
    String pagerankTableString = null;
    try {
      pagerankTableString = new String(Utils.serialize(pagerankTable), ENCODING);
    } catch (UnsupportedEncodingException e) {
      log.error(e.getMessage());
    } catch (IOException e) {
      log.error(e.getMessage());
    }

    // put the necessary properties into the hashmap for the iterators
    Map<String,String> iteratorProperties = new HashMap<String, String>();
    iteratorProperties.put(Search.QUERY, query);
    iteratorProperties.put(Search.PAGE, String.valueOf(page));
    iteratorProperties.put(Search.NUM_RESULTS, String.valueOf(resultsPerPage));
    iteratorProperties.put(Search.MAX_NGRAMS, properties.getProperty(Search.MAX_NGRAMS));
    iteratorProperties.put(FT_SAMPLE, sampleTableString);
    iteratorProperties.put(Search.PAGERANK_TABLE, pagerankTableString);

    // this iterator calculates the rank of each document
    IteratorSetting cfg = new IteratorSetting(RANK_CALCULATOR_PRIORITY, 
        RankCalculator.class, iteratorProperties);
    mainScanner.addScanIterator(cfg);

    // this iterator sorts the ranks of each document 
    IteratorSetting cfg2 = new IteratorSetting(DOCUMENT_RANKER_PRIORITY, 
        DocumentRanker.class, iteratorProperties);
    mainScanner.addScanIterator(cfg2);

    // Uses a sample table to determine which rowid to search for
    // Look for the least frequent term to best limit the number of documents
    String minTerm = "";
    Integer min = null;
    for (String s : query.split(" ")) {
      // replace terms in query that are in the stop words list
      if(stopWords.contains(s)) {
        query = query.replaceAll(s,"");
        continue;
      }
      // set the term with the minimum frequency from the sample table
      if(sampleTable.containsKey(s) && (min == null || sampleTable.get(s) < min)) {
        min = sampleTable.get(s);
        minTerm = s;
      }
    }

    // initialize internal objects for search results
    String correction = "";
    ArrayList<String> urls = null;
    FixedSizePQ<Term> urlsPQ = null;
    int numResults = 0;

    // if all terms appear in sample table, look up in accumulo table
    if(!minTerm.equals("")) {

      // holds the information for each URL to be returned
      urls = new ArrayList<String>();
      urlsPQ = new FixedSizePQ<Term>(page * resultsPerPage, new TermComparator());

      // limit the search based on the minimum term
      // rows are created as : "[minTerm] [timestamp]"
      mainScanner.setRange(new Range(minTerm + " ", minTerm + "!"));

      // used to store the last column qualifier of each string
      String lastCQ = "";

      // scan through the rows of the table using the pre-added iterators
      for(Entry<Key, Value> entry : mainScanner) {
        Double rank = 0.0;
        Key key = entry.getKey();

        // get the cosim of the current entry
        try {
          rank = (Double)Utils.deserialize(entry.getValue().get());
        } catch (IOException e) {
          log.error(e.getMessage());
        } catch (ClassNotFoundException e) {
          log.error(e.getMessage());
        }

        // add the current entry to the priority queue
        urlsPQ.add(new Term(key, rank));

        // checks the last column qualifier for the number of total results
        lastCQ = key.getColumnQualifier().toString();
        if (lastCQ.contains(NUM_RESULTS)) {
          lastCQ = lastCQ.replaceAll(KEEP_NUMBERS,"");
          numResults += Integer.parseInt(lastCQ);
        }
      }
    }


    int i = resultsPerPage;
    // limit results if at the last of rankings
    if(resultsPerPage * page > numResults)
      i = numResults - (resultsPerPage * (page - 1));
    // get only the specific urls we want from the back of the priority queue
    if (urlsPQ != null) {
      while (!urlsPQ.isEmpty() && i > 0) {
        Term t = urlsPQ.pollFirst();
        urls.add(t.getValue() + "[ ]" + t.getKey().getColumnFamily().toString());
        i--;
      }
    }

    // reverse the arraylist to get the correct highest to lowest order
    if (urls != null) Collections.reverse(urls);

    // if no results, assume misspelling and look for alternatives
    if (numResults == 0)
      correction = fullTextLevDistance(query, sampleTable, spellChecker);

    // get the total amount of time needed to get all the results
    long timeElapsed = System.nanoTime() - startTime;
    SearchResults results = new SearchResults(urls, correction, numResults, timeElapsed);

    // if there are results, store in cache for future searches
    if (numResults != 0)
      searchResultsCache.put(cacheString, new SoftReference<SearchResults>(results));

    return results;
  }

  // limit the number of suggestions
  private static final int NUM_SUGGESTIONS = 15;

  /**
   * Performs levenshtein distance on each individual term in the search query.  
   * 
   * @param query - the query with no results
   * @param sampleTable - used to find the most popular correction
   * @param spellChecker- Lucene's spell checker
   * 
   * @return the best instance of a misspelled or not-found word.
   */
  public static String fullTextLevDistance(String query, 
      HashMap<String, Integer> sampleTable, SpellChecker spellChecker) {

    String bestResult = query;
    // look up every term individually
    for(String s : query.split(" ")) {

      // only account for words that don't appear in the sample table
      if (sampleTable.containsKey(s) || s.equals("")) continue;

      String[] suggestions;
      // get the best suggestions from Apache Lucene's spell check algorithm
      try {
        suggestions = spellChecker.suggestSimilar(s, NUM_SUGGESTIONS);
      } catch (IOException e) {
        return "";
      }

      // out of the given suggestions, find the most popular from the sample
      int max = 0;
      String popularStr = "";
      for(String result : suggestions) {
        Integer freq = sampleTable.get(result);
        if (freq != null && freq > max) {
          popularStr = result;
          max = freq;
        }
      }
      // replace bad terms with the new terms
      bestResult = bestResult.replaceAll(s, popularStr);
    }
    return bestResult;
  }


  /**
   * get an arraylist of visualization information for the front end
   * currently contains info for three elements :
   * - tag cloud of keywords
   * - tree of keywords
   * - pagerank graph
   * @param searchResults - the returned page of search results
   * 
   * @return an ArrayList containing the visualization information separately
   */
  @Override
  public ArrayList<Object> getVisualizations(SearchResults searchResults) {
    ArrayList<Object> result = new ArrayList<Object>();

    // first index : tag cloud
    HashMap<String, Double> tagCloud = Visualizations.getTagCloud(searchResults, sampleTable, stopWords);
    result.add(tagCloud);

    // second index : keyword tree
    HashMap<String, HashSet<String>> keywordsTree = Visualizations.getKeywordsTree(searchResults, stopWords);
    result.add(keywordsTree);

    // third index : pagerank graph
    HashMap<String, EdgeLinks> pagerankGraph = Visualizations.getRankGraph(conn, properties, searchResults, pagerankTable);
    result.add(pagerankGraph);

    return result;
  }

  /**
   * loads the necessary files from the properties file
   * called from FullTextIndex.configure
   * 
   * @param context - passed from the servlet
   */
  @SuppressWarnings("unchecked")
  private void loadResources(ServletContext context) {

    // get the sample table from resources
    try {
      InputStream sample = context.getResourceAsStream(properties.getProperty(FT_SAMPLE));
      InputStream samplebuffer = new BufferedInputStream( sample );
      ObjectInput objectsample = new ObjectInputStream ( samplebuffer );
      sampleTable = (HashMap<String, Integer>)objectsample.readObject();
    } catch (ClassNotFoundException e) {
      log.error(e.getMessage());
    } catch (IOException e) {
      log.error(e.getMessage());
    }

    // get the pagerank table from resources
    try{
      InputStream pagerank = context.getResourceAsStream(properties.getProperty(PR_FILE));
      InputStream pagerankbuffer = new BufferedInputStream( pagerank );
      ObjectInput objectpagerank = new ObjectInputStream ( pagerankbuffer );
      pagerankTable = (HashMap<String, Double>)objectpagerank.readObject();
    } catch (ClassNotFoundException e) {
      log.error(e.getMessage());
    } catch (IOException e) {
      log.error(e.getMessage());
    }

    // get the spell checker
    spellChecker = LevenshteinDistance.createSpellChecker(context, sampleTable);		

    // create stop words list from general list
    try {
      stopWords = new HashSet<String>();
      InputStream gstop = context.getResourceAsStream(properties.getProperty(Search.GENERAL_STOP));
      DataInputStream gin = new DataInputStream(gstop);
      BufferedReader gbr = new BufferedReader(new InputStreamReader(gin));
      String strLine;
      while ((strLine = gbr.readLine()) != null)
        stopWords.add(strLine);
    } catch (IOException e) {
      log.error(e.getMessage());
    }

  }
}
