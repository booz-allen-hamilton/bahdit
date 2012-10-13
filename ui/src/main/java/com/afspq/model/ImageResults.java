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

import java.io.File;
import java.util.ArrayList;

import com.bah.bahdit.main.plugins.fulltextindex.data.SearchResults;
import com.bah.bahdit.main.search.Search;

public class ImageResults {

	private Object query;
	private String resultStr;
	
	private long timeElapsed;
	private int numberOfResults = 0;

	public ImageResults() {
		resultStr = "";
	}
	
	public String getQuery(){
		if (query instanceof String) {
			String q = (String)query;
			return q.substring(q.lastIndexOf("/") + 1);
		}
		else if (query instanceof File) {
			File f = (File)query;
			String fs = f.getAbsolutePath();
			return fs.substring(fs.lastIndexOf("/") + 1);
		}
		else
			return "";
	}

	public long getTimeElapsed() {
		return timeElapsed;
	}

	public void setTimeElapsed(long timeElapsed) {
		this.timeElapsed = timeElapsed;
	}

	public String getResult() {
		return resultStr;
	}

	public int getNumResults() {
		return numberOfResults;
	}



	public ImageResults getResults(Object query, Search search, int similar, boolean dragAndDrop) {
		
		// Rank , URL, title
		if(query instanceof String){
			query = ((String)query).toLowerCase();
		}
		
		this.query = query;
		SearchResults searchResults = (similar == 1) ? search.search(query, 1, 0) : search.search(query, 0, 0);
		
		ArrayList<String> results   = searchResults.getResults();

		timeElapsed 	= searchResults.getTime();
		numberOfResults = searchResults.getNumResults();

		if (results == null || results.isEmpty()) {
			resultStr = resultStr.concat(
				"No results found for <b>" + this.getQuery() + "</b>.<br><br>" +
				"Suggestions: <br>" +
				"<ul>" +
				"<li>Make sure all words are spelled correctly.</li>" +
				"<li>Try different keywords.</li>" +
				"<li>Try more general keywords.</li>" +
				"<li>Try fewer keywords.</li>" +
				"</ul>"
			);
			
		} else {
			if (!dragAndDrop && similar == 0) {
				resultStr = resultStr.concat(
					"<a href='ProcessQuery?imgQuery=" + this.getQuery() + 
					"&searchType=image&similar=1'>Search similar images</a><br>"
				);
			}
			
			resultStr = resultStr.concat(
				"Showing " + numberOfResults + 
				" results for <b>" + this.getQuery() + 
				"</b>.<br><br>"
			);

			for (String result: results) {
				String[] tmp = result.split("\\[ \\]");
				String location = tmp[0];
				String url = tmp[1];
				resultStr += "<a href='" + url + 
					"' target='blank' style='padding:10px;'><img src='" + 
					location + "' style='max-height:100px;max-width:100px;' title='" + url + "'/><a>";
			}
		}

		return this;
	}
}
