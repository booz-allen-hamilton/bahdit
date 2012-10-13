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

import java.util.HashMap;

/**
 * 
 * Outgoing links that a particular search result contains
 *
 */
public class EdgeLinks {

	private double pageRank;
	private HashMap<String, Double> edgeUrls;

	public EdgeLinks(double pr, HashMap<String, Double> urls){
		pageRank = pr;
		edgeUrls = urls;
	}

	public double getPageRank(){
		return this.pageRank;
	}

	public HashMap<String, Double> getEdgeUrls(){
		return this.edgeUrls;
	}
}
