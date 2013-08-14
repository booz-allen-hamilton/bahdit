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
package com.bah.bahdit.test.plugins.fulltextindex.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.bah.bahdit.main.plugins.fulltextindex.iterators.DocumentRanker;
import com.bah.bahdit.main.plugins.fulltextindex.utils.Utils;
import com.bah.bahdit.main.search.Search;



public class DocumentRankerTest {
  
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

  static Key nk(String row, String colf, String colq) {
    Text rowID = new Text(row);
    Text cf = new Text(colf);
    Text cq = new Text(colq);
    return new Key(rowID, cf, cq);
  }
  
  static void nkv(TreeMap<Key,Value> tm, String row, String colf, String colq, Double val) throws IOException {
    Key k = nk(row, colf, colq);
    tm.put(k, new Value(Utils.serialize(val)));
  }

  @Test
  public void emptyTableTest() throws IOException, ClassNotFoundException {
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    Map<String, String> options = new HashMap<String, String>();
    options.put(Search.QUERY, "apple");
    options.put(Search.NUM_RESULTS, "3");
    options.put(Search.PAGE, "1");
    
    DocumentRanker ai = new DocumentRanker();
    
    ai.init(new SortedMapIterator(tm1), options, null);
    ai.seek(new Range("apple"), EMPTY_COL_FAMS, false);
    
    assertFalse(ai.hasTop());
  }
  
  @Test
  public void emptyQueryTest() throws IOException, ClassNotFoundException {
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    Map<String, String> options = new HashMap<String, String>();
    options.put(Search.QUERY, "");
    options.put(Search.NUM_RESULTS, "3");
    options.put(Search.PAGE, "1");
    
    DocumentRanker ai = new DocumentRanker();
    
    ai.init(new SortedMapIterator(tm1), options, null);
    ai.seek(new Range("apple"), EMPTY_COL_FAMS, false);
    
    assertFalse(ai.hasTop());
  }
  
	@Test
	public void docRankerTest() throws IOException, ClassNotFoundException {
	  
	  TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    Map<String, String> options = new HashMap<String, String>();
    options.put(Search.QUERY, "apple");
    options.put(Search.NUM_RESULTS, "3");
    options.put(Search.PAGE, "1");
    
    nkv(tm1, "apple", "www.a.com", "[cosine similarity]", 0.05);
    nkv(tm1, "apple", "www.bing.com", "[cosine similarity]", 0.95);
    nkv(tm1, "apple", "www.b.com", "[cosine similarity]", 0.06);
    nkv(tm1, "apple", "www.google.com", "[cosine similarity]", 0.98);
    nkv(tm1, "apple", "www.c.com", "[cosine similarity]", 0.07);
    nkv(tm1, "apple", "www.yahoo.com", "[cosine similarity]", 0.91);
    nkv(tm1, "apple", "www.d.com", "[cosine similarity]", 0.08);
    nkv(tm1, "apple", "www.duckduckgo.com", "[cosine similarity]", 0.99);
    nkv(tm1, "apple", "www.e.com", "[cosine similarity]", 0.09);
    nkv(tm1, "boy", "www.google2.com", "boy", 1.0);
    
    DocumentRanker ai = new DocumentRanker();
    
    ai.init(new SortedMapIterator(tm1), options, null);
    ai.seek(new Range("apple"), EMPTY_COL_FAMS, false);
    
    assertTrue(ai.hasTop());
    assertEquals(nk("apple", "www.duckduckgo.com", "[cosine similarity]"), ai.getTopKey());
    assertEquals((Double)0.99, Utils.deserialize(ai.getTopValue().get()));
    
    
    ai.next();
    ai.next();
    
    assertTrue(ai.hasTop());
    assertEquals(nk("apple", "www.google.com", "[cosine similarity]"), ai.getTopKey());
    assertEquals((Double)0.98, Utils.deserialize(ai.getTopValue().get()));
    
    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk("apple", "www.bing.com", "[NUM_RESULTS] : 8"), ai.getTopKey());
    
    assertEquals((Double)0.95, Utils.deserialize(ai.getTopValue().get()));
	}
}
