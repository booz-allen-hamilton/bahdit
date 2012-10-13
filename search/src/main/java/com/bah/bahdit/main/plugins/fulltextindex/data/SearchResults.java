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
package com.bah.bahdit.main.plugins.fulltextindex.data;

import java.util.ArrayList;

/**
 * SearchResults is the information retrieved to place on a single page of 
 * results after a query is searched.  Its variables include :  
 * - ArrayList of information for each URL (previously ordered by rank)
 * - correction if no results were found for the original query
 * - number of total results found (not necessarily the number sent back)
 * - the time needed to find the results and send back to the user
 */
public class SearchResults {
  
	private ArrayList<String> results;
	private String correction = "";
	private int numResults;
	private long timeElapsed;
	
	public SearchResults(ArrayList<String> results, String correction, int numResults) {
		super();
		this.results = results;
		this.correction = correction;
		this.numResults = numResults;
	}
	
	public SearchResults(ArrayList<String> results, String correction, int numResults, long time) {
    super();
    this.results = results;
    this.correction = correction;
    this.numResults = numResults;
    this.timeElapsed = time;
  }
	
	public long getTime() {
	  return timeElapsed;
	}
	
	public void setTime(long time) {
	  this.timeElapsed = time;
	}
	
	public ArrayList<String> getResults() {
		return results;
	}

	public void setResults(ArrayList<String> results) {
		this.results = results;
	}

	public String getCorrection() {
		return correction;
	}

	public void setCorrection(String result) {
		this.correction = result;
	}
	
	public int getNumResults() {
		return numResults;
	}

	public void setNumResults(int numResults) {
		this.numResults = numResults;
	}


}
