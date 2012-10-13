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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import com.bah.bahdit.main.plugins.fulltextindex.data.EdgeLinks;
import com.bah.bahdit.main.plugins.fulltextindex.data.SearchResults;

/**
 * 
 * Functions to get visualizations for the FullTextIndex
 *
 */
public class Visualizations {

  private static final int TITLE_INDEX = 2;
  private static final int KEYWORD_START_INDEX = 3;
  private static final String DELIMITER = "\\[ \\]";
  private static final String PR_URL_MAP_TABLE_PREFIX = "PR_URL_MAP_TABLE_PREFIX";

  /** 
   * grabs all the keywords from the page of results and their weights from the
   * sampling table and returns in hashmap for UI to place in tag cloud
   * 
   * @param searchResults - the search results for the specific page
   * @param sampleTable - sampling table
   * @param stops - stop words list
   * 
   * @return hashmap of keywords mapped to their weights in the sample table
   */
  public static HashMap<String, Double> getTagCloud(
      SearchResults searchResults, HashMap<String, Integer> sampleTable,
      HashSet<String> stops) {

    // if no search results, return null
    if (searchResults == null || !searchResults.getCorrection().equals("") || searchResults.getNumResults() == 0)
      return null;

    ArrayList<String> results = searchResults.getResults();
    HashMap<String, Integer> keywordCloud = new HashMap<String, Integer>();
    HashMap<String, Double> keywordTagCloud = new HashMap<String, Double>();

    // get weights for the keywords and save in hashmap
    for (String s : results) {
      String[] info = s.split(DELIMITER);
      // ignore rank, URL, title and get all keywords
      for (int i = KEYWORD_START_INDEX; i < info.length; i++) {
        String keyword = info[i].toLowerCase();
        // ignore stop words
        if (stops != null && stops.contains(keyword)) break;
        // split into individual words
        String [] keys = keyword.split(" ");
        // sum the values of individual words
        int value = 0;
        for (String k : keys) {
          if (sampleTable.containsKey(k))
            value += sampleTable.get(k);
        }
        // normalize the value against the length
        value /= keys.length;
        keywordCloud.put(keyword, value);
      }
    }
    
    // search for the max value of the keyword cloud
    int max = 0;
    for (Entry<String, Integer>e : keywordCloud.entrySet()) {
      int value = e.getValue();
      if (value > max)
        max = value;
    }
    
    // normalize values against max, to get values between 0 and 1
    for (Entry<String, Integer>e : keywordCloud.entrySet()) {
      keywordTagCloud.put(e.getKey(), (double)e.getValue() / (double)max);
    }
    
    return keywordTagCloud;
  }

  /**
   * Grabs the keywords from the SearchResults object and links them to the 
   * title of the URL they belong to.  This is then used to create a tree 
   * structure in the UI.
   * 
   * @param searchResults
   * @param stops
   * 
   * @return hashmap of URL titles mapped to their keywords
   */
  public static HashMap<String, HashSet<String>> getKeywordsTree(
      SearchResults searchResults, HashSet<String> stops) {

    // if no search results, return null
    if (searchResults == null || !searchResults.getCorrection().equals("") || searchResults.getNumResults() == 0)
      return null;

    ArrayList<String> results = searchResults.getResults();
    HashMap<String, HashSet<String>> keywordsTree = new HashMap<String, HashSet<String>>();

    for (String s : results) {

      // create a hashset of keywords for each URL
      HashSet<String> keywords = new HashSet<String>();

      String[] info = s.split(DELIMITER);
      // ignore rank, URL, title and get all keywords
      for (int i = KEYWORD_START_INDEX; i < info.length; i++) {
        String keyword = info[i];
        // ignore stop words
        if (stops != null && stops.contains(keyword)) break;
        keywords.add(keyword);
      }
      keywordsTree.put(info[TITLE_INDEX], keywords);
    }
    return keywordsTree;
  }


  /**
   * 
   * Gets all the links from the search result
   * 
   * @param conn - connection to use
   * @param properties - main properties file
   * @param searchResults - current search results
   * @param pagerankTable - pagerank table from ingest
   * @return
   */
  public static HashMap<String, EdgeLinks> getRankGraph(Connector conn, Properties properties, SearchResults searchResults, HashMap<String, Double> pagerankTable) {

    HashMap<String, EdgeLinks> results = new HashMap<String, EdgeLinks>();

    // if no search results, return null
    if (searchResults == null || !searchResults.getCorrection().equals("") || searchResults.getNumResults() == 0)
      return null;

    Scanner scann = null;
    try {
      scann = conn.createScanner(properties.getProperty(PR_URL_MAP_TABLE_PREFIX) + "From", new Authorizations());
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }

    for(String s : searchResults.getResults()){
      String[] tmp = s.split(DELIMITER);
      String rank = tmp[0];
      String url = tmp[1];
      scann.setRange(new Range(url));
      HashMap<String, Double> links = new HashMap<String, Double>();
      
      for (Entry<Key, Value> entry : scann)
        links.put(entry.getKey().getColumnFamily().toString(), pagerankTable.get(entry.getKey().getColumnFamily().toString()));
    
      results.put(url, new EdgeLinks(Double.valueOf(rank), links));
    }

    return results;
  }
}
