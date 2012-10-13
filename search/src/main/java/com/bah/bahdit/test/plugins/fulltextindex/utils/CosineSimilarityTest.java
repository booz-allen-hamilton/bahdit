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
package com.bah.bahdit.test.plugins.fulltextindex.utils;

import static org.junit.Assert.assertEquals;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

import com.bah.bahdit.main.plugins.fulltextindex.utils.CosineSimilarity;
import com.bah.bahdit.main.plugins.fulltextindex.utils.Utils;


public class CosineSimilarityTest {
  
  public static final String TOTAL_DOCS = "[[TOTAL NUM DOCS]]";

  @Test
  public void testComputeCosineSimilarity() {

    // used for rounding doubles
    DecimalFormat f = new DecimalFormat("##.00");

    // empty query and doc
    String query0 = "";
    String doc0 = "";

    ArrayList<String> queryTerms = Utils.createNGrams(query0, 2);

    HashMap<String, Double> queryRatios = CosineSimilarity.queryRatios(queryTerms, query0);

    ArrayList<String> ngrams = Utils.createNGrams(doc0,2);
    HashMap<String, Integer> term = Utils.collectTerms(ngrams);
    HashMap<String, Double> ratio = Utils.collectRatios(term, doc0);

    HashMap<String, Integer> sample = new HashMap<String, Integer>();
    sample.put("apple", 10);
    sample.put("banana", 20);
    sample.put("boy", 30);
    sample.put("fox", 40);
    sample.put("bear", 50);
    sample.put(TOTAL_DOCS, 100);

    double result = CosineSimilarity.computeCosineSimilarity(ratio, queryRatios, queryTerms, sample);
    assertEquals(f.format(1.0), f.format(result));    


    // empty query
    String query1 = "";
    String doc1 = "null a";

    queryTerms = Utils.createNGrams(query1, 2);

    queryRatios = CosineSimilarity.queryRatios(queryTerms, query1);

    ngrams = Utils.createNGrams(doc1,2);
    term = Utils.collectTerms(ngrams);
    ratio = Utils.collectRatios(term, doc1);

    result = CosineSimilarity.computeCosineSimilarity(ratio, queryRatios, queryTerms, sample);
    assertEquals(f.format(0.0), f.format(result)); 

    // equal query and doc
    String query2 = "a a a";
    String doc2 = "a a a";

    queryTerms = Utils.createNGrams(query2, 2);

    queryRatios = CosineSimilarity.queryRatios(queryTerms, query2);

    ngrams = Utils.createNGrams(doc2,2);
    term = Utils.collectTerms(ngrams);
    ratio = Utils.collectRatios(term, doc2);

    result = CosineSimilarity.computeCosineSimilarity(ratio, queryRatios, queryTerms, sample);
    assertEquals(f.format(1.00), f.format(result));

    // diff query and doc
    String query3 = "a a";
    String doc3 = "a a fox fox a fox fox a a";
    
    queryTerms = Utils.createNGrams(query3, 2);

    queryRatios = CosineSimilarity.queryRatios(queryTerms, query3);

    ngrams = Utils.createNGrams(doc3,2);
    term = Utils.collectTerms(ngrams);
    ratio = Utils.collectRatios(term, doc3);
    
    result = CosineSimilarity.computeCosineSimilarity(ratio, queryRatios, queryTerms, sample);
    assertEquals(f.format(0.63), f.format(result));
  }

}
