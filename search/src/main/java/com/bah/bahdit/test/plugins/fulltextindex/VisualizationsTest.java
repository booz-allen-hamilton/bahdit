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
package com.bah.bahdit.test.plugins.fulltextindex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.bah.bahdit.main.plugins.fulltextindex.Visualizations;
import com.bah.bahdit.main.plugins.fulltextindex.data.EdgeLinks;
import com.bah.bahdit.main.plugins.fulltextindex.data.SearchResults;

/**
 * 
 * Functions to get visualizations for the FullTextIndex
 *
 */
public class VisualizationsTest {

	private static HashMap<String, Integer> createSampleTable() {

		HashMap<String, Integer> h = new HashMap<String, Integer>();

		h.put("key1", 1);
		h.put("key2", 2);
		h.put("key3", 3);
		h.put("key4", 4);
		h.put("key5", 5);
		h.put("key6", 6);
		h.put("key7", 7);
		h.put("key8", 8);
		h.put("key9", 9);
		h.put("key10", 10);

		return h;
	}

	@Test
	public void testGetTagCloud() {

		HashMap<String, Integer> sample = createSampleTable();

		ArrayList<String> arr = new ArrayList<String>();
		arr.add("0.1[ ]url[ ]title[ ]key1");
		arr.add("0.2[ ]url[ ]title[ ]key2");
		arr.add("0.3[ ]url[ ]title[ ]key3[ ]key4");
		arr.add("0.4[ ]url[ ]title");

		SearchResults sr = new SearchResults(arr, "", 4);

		HashMap<String, Double> tag = Visualizations.getTagCloud(sr, sample, null);
		assertEquals((Double)0.25, tag.remove("key1"));
		assertEquals((Double)0.5, tag.remove("key2"));
		assertEquals((Double)0.75, tag.remove("key3"));
		assertEquals((Double)1.0, tag.remove("key4"));
		assertTrue(tag.isEmpty());
	}

	@Test
	public void testGetKeywordsTree() {

		ArrayList<String> arr = new ArrayList<String>();
		arr.add("0.1[ ]url1[ ]title1[ ]key1");
		arr.add("0.2[ ]url2[ ]title2[ ]key2");
		arr.add("0.3[ ]url3[ ]title3[ ]key3[ ]key4");
		arr.add("0.4[ ]url4[ ]title4");

		SearchResults sr = new SearchResults(arr, "", 4, 1000);

		HashMap<String, HashSet<String>> tag = Visualizations.getKeywordsTree(sr, null);
		HashSet<String> title1 = tag.remove("title1");
		assertTrue(title1.remove("key1"));
		assertTrue(title1.isEmpty());
		HashSet<String> title2 = tag.remove("title2");
		assertTrue(title2.remove("key2"));
		assertTrue(title2.isEmpty());
		HashSet<String> title3 = tag.remove("title3");
		assertTrue(title3.remove("key3"));
		assertTrue(title3.remove("key4"));
		assertTrue(title3.isEmpty());
		HashSet<String> title4 = tag.remove("title4");
		assertTrue(title4.isEmpty());
		assertTrue(tag.isEmpty());
	}

	@Test
	public void testGetRankGraph(){
		MockInstance mi = new MockInstance();
		Connector conn = null;
		try {
			conn = mi.getConnector("test", "password".getBytes());
			conn.tableOperations().create("TestFrom");
		} catch (AccumuloException e2) {
			e2.printStackTrace();
		} catch (AccumuloSecurityException e2) {
			e2.printStackTrace();
		} catch (TableExistsException e) {
			e.printStackTrace();
		}

		Properties properties = new Properties();
		properties.setProperty("PR_URL_MAP_TABLE_PREFIX", "Test");

		BatchWriter writer = null;
		try {
			writer = conn.createBatchWriter("TestFrom", 1000000L, 1000L, 10);
		} catch (TableNotFoundException e1) {
			e1.printStackTrace();
		}

		ArrayList<String> arr = new ArrayList<String>();
		arr.add("0.1[ ]url1[ ]title1[ ]key1");
		arr.add("0.2[ ]url2[ ]title2[ ]key2");
		arr.add("0.3[ ]url3[ ]title3[ ]key3[ ]key4");
		arr.add("0.4[ ]url4[ ]title4");

		try {
			Mutation m = new Mutation("url1");
			m.put(new Text("url2"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url1");
			m.put(new Text("url3"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url1");
			m.put(new Text("url4"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url2");
			m.put(new Text("url1"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url2");
			m.put(new Text("url3"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url2");
			m.put(new Text("url4"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url3");
			m.put(new Text("url1"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url3");
			m.put(new Text("url2"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url3");
			m.put(new Text("url4"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url4");
			m.put(new Text("url1"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url4");
			m.put(new Text("url3"), new Text(""), new Value("".getBytes()));
			writer.addMutation(m);

			m = new Mutation("url4");
			m.put(new Text("url2"), new Text(""), new Value("".getBytes()));

			writer.addMutation(m);
		} catch (MutationsRejectedException e) {
			e.printStackTrace();
		}

		SearchResults sr = new SearchResults(arr, "", 4, 1000);

		HashMap<String, Double> pagerankTable = new HashMap<String, Double>();

		pagerankTable.put("url1", 1.0);
		pagerankTable.put("url2", 2.0);
		pagerankTable.put("url3", 3.0);
		pagerankTable.put("url4", 4.0);

		HashMap<String, EdgeLinks> graph = Visualizations.getRankGraph(conn, properties, sr, pagerankTable);

		EdgeLinks next = graph.remove("url1");
		HashMap<String, Double> edges = next.getEdgeUrls();
		assertTrue(edges.containsKey("url2"));
		assertTrue(edges.containsKey("url3"));
		assertTrue(edges.containsKey("url4"));
		assertEquals((Double).1, (Double)next.getPageRank());

		EdgeLinks next2 = graph.remove("url2");
		HashMap<String, Double> edges2 = next2.getEdgeUrls();
		assertTrue(edges2.containsKey("url1"));
		assertTrue(edges2.containsKey("url3"));
		assertTrue(edges2.containsKey("url4"));
		assertEquals((Double).2, (Double)next2.getPageRank());

		EdgeLinks next3 = graph.remove("url3");
		HashMap<String, Double> edges3 = next3.getEdgeUrls();
		assertTrue(edges3.containsKey("url2"));
		assertTrue(edges3.containsKey("url1"));
		assertTrue(edges3.containsKey("url4"));
		assertEquals((Double).3, (Double)next3.getPageRank());

		EdgeLinks next4 = graph.remove("url4");
		HashMap<String, Double> edges4 = next4.getEdgeUrls();
		assertTrue(edges4.containsKey("url2"));
		assertTrue(edges4.containsKey("url3"));
		assertTrue(edges4.containsKey("url1"));
		assertEquals((Double).4, (Double)next4.getPageRank());

	}
}
