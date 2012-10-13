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
package com.afspq.model;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.bah.bahdit.main.plugins.fulltextindex.data.EdgeLinks;
import com.bah.bahdit.main.plugins.fulltextindex.data.SearchResults;
import com.bah.bahdit.main.search.Search;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class Results {

	private String query;
	private String resultStr;
	private String correction;
	private String tagCloudWords;
	private boolean corrected = false;

	private long timeElapsed;
	private int numberOfPages = 0;
	private int numberOfResults = 0;

	public JsonArray nodesArr;
	public JsonObject keywordsTreeJson;
	public JsonObject keywordsCloudJson;

	public Results() {
		resultStr = "";
		correction = "";
		tagCloudWords = "";
	}

	/**
	 * Returns the time it took to process a query in milli seconds.
	 * @return
	 */
	public long getTimeElapsed() {
		return timeElapsed;
	}
	
	public void setTimeElapsed(long timeElapsed) {
		this.timeElapsed = timeElapsed;
	}

	/**
	 * Returns the all the results of a single page as a string.
	 * @return
	 */
	public String getResult() {
		return resultStr;
	}

	/**
	 * Returns the total number of results for a search query.
	 * @return
	 */
	public int getNumResults() {
		return numberOfResults;
	}

	/**
	 * Returns the number of pages that the search results will need.
	 * By default, the search result shows 8 results.  So, if there were a total
	 * of 16 results for a search query, this method would return 2.
	 * @return
	 */
	public int getNumPages() {
		return numberOfPages;
	}

	/**
	 * Returns the search term(s) used for the query.  If the user searched for
	 * "apple fox," then it this method would return that string.
	 * @return
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * Returns the string of words used by the key word cloud visualization.
	 * @return
	 */
	public String getTagCloudWords() {
		return tagCloudWords;
	}

	/**
	 * Creates the string of words for the cloud visualization. 
	 * Also creates a Jsona object of the keywords with the following
	 * structure:
	 * {
     * "words": [
     *   {
     *       "word": "keyword",
     *       "color": "blue"
     *   }
	 * }
	 * 
	 * This Json object can be used in other types of visualizations.
	 * @param search
	 * @param searchResults
	 */
	@SuppressWarnings("unchecked")
	private void createKeywordsCloud(Search search, SearchResults searchResults) {

		/*
		 * getVisualizations returns an ArrayList containing data for different
		 * visualiztions.  The first index in this ArrayList contains data for
		 * that can be used for the keyword cloud.  The second index of the list
		 * contains data for the page rank graph and so on.
		 */
		ArrayList<?> visualizationList = search.getVisualizations(searchResults);
		
		/*
		 * Each element inside the ArrayList contains a HashMap.  In this case, 
		 * it is a HashMap where the key is the keyword and the value is its
		 * weight, i.e. the popularity of the word in the sample table.
		 */
		HashMap<String, Double> keywordsCloudMap = (HashMap<String, Double>) visualizationList.get(0);
		keywordsCloudJson 	= new JsonObject();
		JsonArray jsonArray = new JsonArray();
		SecureRandom sr 	= new SecureRandom();	// Used to pick a random font.

		if (keywordsCloudMap == null) {
			tagCloudWords = tagCloudWords.concat("<ul><li></li></ul>");
			return;
		}

		/*
		 * The keyword cloud framework requires the words to be contained inside
		 * an HTML unordered list.  This is the reason for appending <ul> and
		 * <li> to the string.  See http://www.goat1000.com/tagcanvas.php for
		 * more details.
		 */
		tagCloudWords 		= tagCloudWords.concat("<ul>");
		String[] fontStyles = {"Times New Roman", "Arial Black", "Sans-Serif"};
		int count 			= 0;
		
		for (String s: keywordsCloudMap.keySet()) {
			// Avoid overcrowding of words and limit to 25 words in the cloud.
			if (++count == 25) { break; }
			
			int i 			= sr.nextInt(3);
			JsonObject node = new JsonObject();

			node.addProperty("word", s);
			node.addProperty("color", "#ffffff");
			jsonArray.add(node);

			if (s.equals("[[TOTAL NUM DOCS]]")) { continue; }

			double weight 	= keywordsCloudMap.get(s);
			weight 		  	=  (weight * 40) + 12;
			String fontSize = Double.toString(weight);
			
			tagCloudWords 	= tagCloudWords.concat(
				"<li><a style='font-size: " + fontSize + 
				"pt; color: #ffffff; font-family: " +
				fontStyles[i] + ";'" +
				"href='ProcessQuery?query=" + s + 
				"&page=1&searchType=web'>" + s + "</a></li>"
			);
		}
		tagCloudWords = tagCloudWords.concat("</ul>");
		keywordsCloudJson.add("words", jsonArray);
	}

	/**
	 * Creates the Json object for the keywords space tree visualization.
	 * 
	 * @param query
	 * @param search
	 * @param searchResults
	 */
	@SuppressWarnings("unchecked")
	private void createKeywordsTree(String query, Search search, SearchResults searchResults) {
		ArrayList<?> visualizationList = search.getVisualizations(searchResults);
		HashMap<String, HashSet<String>> keywordsTreeMap = (HashMap<String, HashSet<String>>) visualizationList.get(1);

		if (keywordsTreeMap == null) {
			return;
		}

		
		keywordsTreeJson = new JsonObject();
		keywordsTreeJson.addProperty("id", "query");
		keywordsTreeJson.addProperty("name", query);

		JsonArray children = new JsonArray();
		keywordsTreeJson.add("children", children);

		int i = 0;
		for (String title : keywordsTreeMap.keySet()) {
			if (!keywordsTreeMap.get(title).isEmpty()) {

				JsonArray grandChildren = new JsonArray();
				JsonObject resultTitle = new JsonObject();

				String truncatedTitle = "";

				// Long titles don't fit nicely in the visualization tree node
				// so if the title is longer than 20 characters truncate it and
				// add "..."
				if (title.length() > 20) {
					truncatedTitle = title.substring(0, 21).concat("...");
				} else {
					truncatedTitle = title;
				}

				resultTitle.addProperty("id", title);
				resultTitle.addProperty("name", truncatedTitle);
				resultTitle.add("children", grandChildren);

				HashSet<String> keywords = keywordsTreeMap.get(title);

				for (String keyword : keywords) {
					JsonObject key = new JsonObject();
					key.addProperty("id", "k" + i++);
					key.addProperty("name", keyword);
					grandChildren.add(key);
				}

				children.add(resultTitle);
			}
		}
	}

	/**
	 * Creates the Json object for the page rank graph visualizaiton.
	 * 
	 * @param query
	 * @param search
	 * @param searchResults
	 */
	@SuppressWarnings("unchecked")
	private void createPageRankGraph(String query, Search search, SearchResults searchResults) {
		ArrayList<?> visualizationList = search.getVisualizations(searchResults);
		HashMap<String, EdgeLinks> pageRankMap = (HashMap<String, EdgeLinks>) visualizationList.get(2);

		if (pageRankMap == null) {
			return;
		}

		nodesArr = new JsonArray();

		// Colors for the nodes.  Selected randomly.
		String[] colorsArr = {
			"#003DF5","#B800F5","#F500B8",
			"#00B8F5","#3366FF","#F5003D",
			"#00F5B8","#FFCC33","#F53D00",
			"#00F53D","#B8F500","#F5B800"
		};

		HashMap <Integer,String> colorMap = new HashMap<Integer,String>();
		for (int i =0; i<colorsArr.length; i++){
			colorMap.put(i, colorsArr[i]);
		}

		SecureRandom sr = new SecureRandom();
		for (Map.Entry<String, EdgeLinks> urlEntry : pageRankMap.entrySet()) {
			
			String key      = urlEntry.getKey();  
			EdgeLinks value = urlEntry.getValue();
			double  dim     = value.getPageRank()*15000/23;
			
			// Adjusting the diameter of the node.
			dim = (dim > 70 ) ? dim / 15 : dim;
			
			JsonObject node  = new JsonObject();
			JsonArray adjArr = new JsonArray();
			JsonObject data  = new JsonObject();
			
			data.addProperty("$type", "circle");
			data.addProperty("$color",colorMap.get(sr.nextInt(colorsArr.length)));
			data.addProperty("$dim", dim );
			HashMap<String,Double> assocUrls = value.getEdgeUrls();

			int i	   = 0;
			int ranAdj = sr.nextInt(15);
			
			// Each node should have at least 3 edge connections if possible.
			ranAdj 	   = (ranAdj < 3) ? 3 : ranAdj;
			
			for (Map.Entry<String, Double> urlEdge : assocUrls.entrySet()){ 
				
				if(i < ranAdj) {
					i++;
					JsonObject adj     = new JsonObject();
					JsonObject empData = new JsonObject();
					
					adj.addProperty("nodeTo", urlEdge.getKey());
					adj.addProperty("nodeFrom", key);
					adj.add("data", empData); 
					adjArr.add(adj);  
				} else break;
			}
			
			node.add( "adjacencies",adjArr );
			node.add("data", data);
			node.addProperty("id",key);
			node.addProperty("name", key);
			nodesArr.add(node);
		}
	}

	/**
	 * Gets the corrected string if one exists and returns true if a correction
	 * was created and false otherwise.
	 * 
	 * @param searchResults
	 * @return
	 */
	private boolean createCorrection(SearchResults searchResults) {
		correction = searchResults.getCorrection();

		if (!correction.equals("")) {
			resultStr = resultStr.concat("Did you mean ");
			resultStr = resultStr.concat(
				"<a class='correction' href='ProcessQuery?query=" + 
				correction.trim() + 
				"&page=1&searchButton=Search&searchType=web'>" + 
				correction.trim() + "</a>"
			);
			
			resultStr = resultStr.concat("?<br><br>");
			return true;
		}
		
		return false;
	}

	/**
	 * Calls the methods from the Search class to create the results for a
	 * search query.  Also calls to the methods that make visualizations are 
	 * made from here.
	 * 
	 * @param query
	 * @param search
	 * @param page
	 * @param resultsPerPage
	 * @return
	 */
	@SuppressWarnings("unused")
	public Results getResults(String query, Search search, int page, int resultsPerPage) {
		this.query = query;
		SearchResults searchResults = search.search(query.toLowerCase(), page, resultsPerPage);
		ArrayList<String> results = searchResults.getResults();

		numberOfResults = searchResults.getNumResults();
		numberOfPages   = (int) Math.ceil((double)numberOfResults / (double)resultsPerPage);

		String rank  = "";
		String url   = "";
		String title = "";

		/*
		 * Attempt to get a spelling correction if no search results are
		 * returned.  If a correction is returned, recursively called this
		 * function and return the results if they exist.  Otherwise, return
		 * suggestions.
		 */
		if (results == null || results.isEmpty()) {

			if (correction.isEmpty()) {
				createCorrection(searchResults);
				corrected = true;
				
				if (!correction.isEmpty() && !correction.equals(query)) {
					getResults(correction.trim(), search, page, resultsPerPage);
				} else {
					resultStr = resultStr.concat(
						"No results found for <b>" + query + "</b>.<br><br>" +
						"Suggestions: <br>" +
						"<ul>" +
						"<li>Make sure all words are spelled correctly.</li>" +
						"<li>Try different keywords.</li>" +
						"<li>Try more general keywords.</li>" +
						"<li>Try fewer keywords.</li>" +
						"</ul>"
					);
				}
			} else {
				resultStr = resultStr.concat(
					"No results found for <b>" + query + "</b>.<br><br>" +
					"Suggestions: <br>" +
					"<ul>" +
					"<li>Make sure all words are spelled correctly.</li>" +
					"<li>Try different keywords.</li>" +
					"<li>Try more general keywords.</li>" +
					"<li>Try fewer keywords.</li>" +
					"</ul>"
				);
			}

		} else {

			int numResults = 0;
			timeElapsed = searchResults.getTime();
			createKeywordsCloud(search, searchResults);
			createKeywordsTree(query, search, searchResults);
			createPageRankGraph(query, search, searchResults);
			

			String range   = "";
			int startRange = ((page - 1) * resultsPerPage) + 1;
			
			if (numberOfResults < (resultsPerPage * page)) {
				range = startRange + " - " + numberOfResults + " ";
			} else {
				range = startRange + " - " + (startRange + resultsPerPage - 1);
			}

			String instead = corrected ? " instead" : "";
			
			resultStr = resultStr.concat(
				"Showing " +  range + 
				" of " + numberOfResults + " results " +
				" for <b>" + query.trim() + "</b>" + instead + ".<br><br>"
			);

			if (numberOfResults < resultsPerPage) {
				numResults = numberOfResults;
			} else {
				numResults = resultsPerPage;
			}

			for (String result : results) {
				String[] resultInfo = result.split("\\[ \\]");
				rank  = resultInfo[0];
				url   = resultInfo[1];
				title = resultInfo[2];
				String keywords = "";

				for (int i = 3; i < resultInfo.length; i++) {
					String urlQuery = resultInfo[i].replaceAll(" ", "+");
					keywords = keywords + "<a href='?query=" 
							+ urlQuery.trim() + "&page=1&searchType=web'>"
							+ resultInfo[i].trim() + "</a> <nbsp>";
				}

				// Append keywords associated with each result.
				String keyHTML = "";
				if (resultInfo.length > 3) {
					keyHTML = "<span class='keyword'>" + keywords + "</span><br>";
				}

				// Create the final result string.
				resultStr = resultStr.concat(
					"<a href='" + url + "'>" + title + "</a><br>" +
					"<span id='url'>" + url + "</span><br>" + keyHTML +
					"<span class='rank'><b>Rank:</b> " + rank + "</span><br><br>"
				);
			}
		}

		return this;
	}
}
