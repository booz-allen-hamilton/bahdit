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
import java.text.DecimalFormat;
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

import com.bah.bahdit.main.plugins.fulltextindex.FullTextIndex;
import com.bah.bahdit.main.plugins.fulltextindex.iterators.RankCalculator;
import com.bah.bahdit.main.plugins.fulltextindex.utils.Utils;
import com.bah.bahdit.main.search.Search;

public class RankCalculatorTest {
  
  public static final String TOTAL_DOCS = "[[TOTAL NUM DOCS]]";
  
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

  static Key nk(String row, String colf, String colq) {
    Text rowID = new Text(row);
    Text cf = new Text(colf);
    Text cq = new Text(colq);
    return new Key(rowID, cf, cq);
  }
  
  static void nkv(TreeMap<Key,Value> tm, String row, String colf, String colq, String val) throws IOException {
    Key k = nk(row, colf, colq);
    tm.put(k, new Value(Utils.serialize(val)));
  }
  
  @Test
  public void emptyTableTest() throws IOException, ClassNotFoundException {
    
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    Map<String, String> options = new HashMap<String, String>();
    HashMap<String, Integer> sampleTable = new HashMap<String, Integer>();
    sampleTable.put("apple", 3);
    String sampleTableString = new String(Utils.serialize(sampleTable), FullTextIndex.ENCODING);
    
    options.put(Search.QUERY, "apple");
    options.put(Search.MAX_NGRAMS, "2");
    options.put(FullTextIndex.FT_SAMPLE, sampleTableString);
    RankCalculator ai = new RankCalculator();
    
    ai.init(new SortedMapIterator(tm1), options, null);
    ai.seek(new Range("apple"), EMPTY_COL_FAMS, false);
    
    assertFalse(ai.hasTop());
  }
  
  @Test
  public void emptyQueryTest() throws IOException, ClassNotFoundException {
    
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    Map<String, String> options = new HashMap<String, String>();
    options.put(Search.QUERY, "");
    options.put(Search.MAX_NGRAMS, "2");
    HashMap<String, Integer> sampleTable = new HashMap<String, Integer>();
    sampleTable.put("apple", 3);
    String sampleTableString = new String(Utils.serialize(sampleTable), FullTextIndex.ENCODING);
    options.put(FullTextIndex.FT_SAMPLE, sampleTableString);
    
    RankCalculator ai = new RankCalculator();
    
    ai.init(new SortedMapIterator(tm1), options, null);
    ai.seek(new Range("apple"), EMPTY_COL_FAMS, false);
    
    
    assertFalse(ai.hasTop());
  }
  
  @Test
  public void normalTest() throws IOException, ClassNotFoundException {
    
    DecimalFormat f = new DecimalFormat("##.00");
    
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    Map<String, String> options = new HashMap<String, String>();
    
    options.put(Search.QUERY, "apple");
    options.put(Search.MAX_NGRAMS, "1");
    
    HashMap<String, Integer> sample = new HashMap<String, Integer>();
    sample.put("apple", 10);
    sample.put("banana", 20);
    sample.put("boy", 30);
    sample.put("fox", 40);
    sample.put("bear", 50);
    sample.put(TOTAL_DOCS, 100);
    
    String sampleTableString = new String(Utils.serialize(sample), FullTextIndex.ENCODING);    
    options.put(FullTextIndex.FT_SAMPLE, sampleTableString);
    
    
    nkv(tm1, "apple", "www.bing.com", "apple", "5,0.25");
    nkv(tm1, "apple", "www.bing.com", "banana", "5,0.25");
    nkv(tm1, "apple", "www.bing.com", "boy", "2,0.1");
    nkv(tm1, "apple", "www.bing.com", "fox", "12,0.6");
    nkv(tm1, "apple", "www.bing.com", "bear", "1,0.05");
    nkv(tm1, "apple", "www.google.com", "apple", "5,0.25");
    nkv(tm1, "apple", "www.google.com", "boy", "2,0.1");
    nkv(tm1, "apple", "www.google.com", "fox", "12,0.6");
    nkv(tm1, "apple", "www.google.com", "bear", "1,0.05");
    nkv(tm1, "boy", "www.google2.com", "boy", "5,1.0");
    
    RankCalculator ai = new RankCalculator();
    ai.init(new SortedMapIterator(tm1), options, null);
    ai.seek(new Range("apple"), EMPTY_COL_FAMS, false);
    
    assertTrue(ai.hasTop());
    assertEquals(nk("apple", "www.bing.com", "apple"), ai.getTopKey());
    assertEquals(f.format(0.32), f.format(Utils.deserialize(ai.getTopValue().get())));
    
    ai.next();
    ai.next();
    
    assertTrue(ai.hasTop());
    assertEquals(nk("apple", "www.google.com", "apple"), ai.getTopKey());
    assertEquals(f.format(0.36), f.format(Utils.deserialize(ai.getTopValue().get())));
    
    ai.next();

    assertFalse(ai.hasTop());
  }
  
  @Test
  public void normalTest2() throws IOException, ClassNotFoundException {
    
    DecimalFormat f = new DecimalFormat("##.00");

    TreeMap<Key,Value> writer = new TreeMap<Key,Value>();
    Map<String, String> options = new HashMap<String, String>();
    options.put(Search.QUERY, "term1");
    options.put(Search.MAX_NGRAMS, "2");
    
    HashMap<String, Integer> sample = new HashMap<String, Integer>();
    sample.put("term1", 10);
    sample.put("term2", 20);
    sample.put("term3", 30);
    sample.put("term4", 40);
    sample.put("term5", 50);
    sample.put(TOTAL_DOCS, 100);
    
    String sampleTableString = new String(Utils.serialize(sample), FullTextIndex.ENCODING);    
    options.put(FullTextIndex.FT_SAMPLE, sampleTableString);
    
    nkv(writer, "term1", "g", "term1", "5,0.25");
    nkv(writer, "term1", "g", "term2", "5,0.25");
    nkv(writer, "term1", "g", "term3", "5,0.25");
    nkv(writer, "term1", "g", "term4", "5,0.25");
    nkv(writer, "term1", "f", "term1", "5,0.125");
    nkv(writer, "term1", "f", "term2", "10,0.25");
    nkv(writer, "term1", "f", "term3", "10,0.25");
    nkv(writer, "term1", "f", "term4", "10,0.25");
    nkv(writer, "term1", "f", "term5", "5,0.125");
    
    RankCalculator ai = new RankCalculator();
    
    ai.init(new SortedMapIterator(writer), options, null);
    ai.seek(new Range("term1"), EMPTY_COL_FAMS, false);
    
    assertTrue(ai.hasTop());
    assertEquals(nk("term1", "f", "term1"), ai.getTopKey());
    assertEquals(f.format(0.23), f.format(Utils.deserialize(ai.getTopValue().get())));
    
    
    ai.next();
    ai.next();
    
    assertTrue(ai.hasTop());
    assertEquals(nk("term1", "g", "term1"), ai.getTopKey());
    assertEquals(f.format(0.36), f.format(Utils.deserialize(ai.getTopValue().get())));
    
    ai.next();

    assertFalse(ai.hasTop());
  }

}
