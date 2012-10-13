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
/**
 * 
 * Cloud Analytics Intern Proj. 2012
 * Main Search Controller
 * Search.java
 * 
 */

package com.bah.bahdit.main.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import javax.servlet.ServletContext;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;

import com.bah.bahdit.main.plugins.fulltextindex.data.SearchResults;
import com.bah.bahdit.main.plugins.index.Index;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * 
 * Main search controller object for bahdit. A connection is established
 * and held. A search can be executed by calling search. Configuration
 * for a particular index happens in the search module.
 *
 */
public class Search {

	private static Log log = LogFactory.getLog(Search.class);
	
	public static final String CONFIGURATION_FILE = "properties.conf";

	public static final String QUERY = "QUERY";
	public static final String MAX_NGRAMS = "MAX_NGRAMS";
	public static final String NUM_RESULTS = "NUM_RESULTS";
	public static final String PAGERANK_TABLE = "PAGERANK_TABLE";
	public static final String DOMAIN_STOP = "DOMAIN_STOP";
	public static final String GENERAL_STOP = "GENERAL_STOP";

	public static final String INDEX = "INDEX";
	public static final String INSTANCE_NAME = "INSTANCE_NAME";
	public static final String USERNAME = "USERNAME";
	public static final String PASSWORD = "PASSWORD";
	public static final String ZK_SERVERS = "ZK_SERVERS";
	public static final String PAGE = "PAGE";

	private Index index = null;
	private Connector conn = null;
	private Properties properties = null;

	/**
	 * 
	 * The main search object that gets a connection and sets up 
	 * the Index and the configurations.
	 * 
	 * @param context - a servlet context from which the 
	 * 					search will grab its files from
	 * @param indexToUse - text constant found in the search module to use.
	 * @throws IOException 
	 */
	public Search(ServletContext context, String indexToUse) throws IOException {  
		properties = new Properties();  
		properties.load(context.getResourceAsStream(CONFIGURATION_FILE));
		Injector injector = Guice.createInjector(new SearcherModule(indexToUse));
		this.index = injector.getInstance(Index.class);
		this.conn = connect();
		index.configure(conn, properties, context);
	}

	/**
	 * 
	 * Calls the Index's search with a particular query object
	 * 
	 * @param query
	 * @param page
	 * @param resultsPerPage
	 * @return
	 */
	public SearchResults search(Object query, int page, int resultsPerPage) {
		return index.search(query, page, resultsPerPage);
	}

	/**
	 * 
	 * Gets the index's configuration
	 * 
	 * @param results - returns an arraylist of object 
	 * 					visualization form the index
	 * @return
	 */
	public ArrayList<?> getVisualizations(SearchResults results) {
		return index.getVisualizations(results);
	}

	/**
	 * 
	 * Connects to an accumulo instance in the config file 
	 * then returns that connection
	 * 
	 * @return
	 */
	private Connector connect(){
		Connector connector = null;
		String instanceName = properties.getProperty(INSTANCE_NAME);
		String zkServerNames = properties.getProperty(ZK_SERVERS);
		String user = properties.getProperty(USERNAME);
		String password = properties.getProperty(PASSWORD);

		Instance inst = new ZooKeeperInstance(instanceName, zkServerNames);

		try {
			connector = inst.getConnector(user, password.getBytes());
		} catch (AccumuloException e) {
			log.error(e.getMessage());
		} catch (AccumuloSecurityException e) {
			log.error(e.getMessage());
		}

		return connector;
	}
}
