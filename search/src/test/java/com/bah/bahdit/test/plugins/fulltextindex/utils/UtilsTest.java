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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

import com.bah.bahdit.main.plugins.fulltextindex.utils.Utils;


public class UtilsTest {

  @Test
  public void testGetWords() {
    String q = "aa bb cc dd ee";
    String[] words = Utils.getWords(q);
    assertEquals(5, words.length);
    assertEquals("aa", words[0]);
    assertEquals("bb", words[1]);
    assertEquals("cc", words[2]);
    assertEquals("dd", words[3]);
    assertEquals("ee", words[4]);
  }
  
  @Test
  public void testCreateNGrams() {

    String query0 = "";
    ArrayList<String> terms = Utils.createNGrams(query0,2);
    assertEquals(1, terms.size());

    String query1 = " ";
    terms = Utils.createNGrams(query1,2);
    assertEquals(0, terms.size());

    String query2 = "a b c d e                    f g          h";
    terms = Utils.createNGrams(query2,2);
    assertEquals(15, terms.size());
    assertEquals("a", terms.get(0));
    assertEquals("b", terms.get(1));
    assertEquals("c", terms.get(2));
    assertEquals("d", terms.get(3));
    assertEquals("e", terms.get(4));
    assertEquals("f", terms.get(5));
    assertEquals("g", terms.get(6));
    assertEquals("h", terms.get(7));
    assertEquals("a b", terms.get(8));
    assertEquals("b c", terms.get(9));
    assertEquals("c d", terms.get(10));
    assertEquals("d e", terms.get(11));
    assertEquals("e f", terms.get(12));
    assertEquals("f g", terms.get(13));
    assertEquals("g h", terms.get(14));

    String query3 = "a's b.c,d?e!f\"g";
    terms = Utils.createNGrams(query3,2);
    assertEquals(13, terms.size());
    assertEquals("a's", terms.get(0));
    assertEquals("b", terms.get(1));
    assertEquals("c", terms.get(2));
    assertEquals("d", terms.get(3));
    assertEquals("e", terms.get(4));
    assertEquals("f", terms.get(5));
    assertEquals("g", terms.get(6));
    assertEquals("a's b", terms.get(7));
    assertEquals("b c", terms.get(8));
    assertEquals("c d", terms.get(9));
    assertEquals("d e", terms.get(10));
    assertEquals("e f", terms.get(11));
    assertEquals("f g", terms.get(12));
  }

  @Test
  public void testCollectTerms() {

    String query0 = "";
    ArrayList<String> terms = Utils.createNGrams(query0,2);
    HashMap<String, Integer> collected = Utils.collectTerms(terms);
    assertEquals(1, collected.size());

    String query1 = " ";
    terms = Utils.createNGrams(query1,2);
    collected = Utils.collectTerms(terms);
    assertEquals(0, collected.size());

    String query2 = "a";
    terms = Utils.createNGrams(query2,2);
    collected = Utils.collectTerms(terms);
    assertEquals(1, collected.size());
    assertEquals(1, (int)collected.remove("a"));
    assertEquals(0, collected.size());

    String query3 = "a a a";
    terms = Utils.createNGrams(query3,2);
    collected = Utils.collectTerms(terms);
    assertEquals(2, collected.size());
    assertEquals(3, (int)collected.remove("a"));
    assertEquals(2, (int)collected.remove("a a"));
    assertEquals(0, collected.size());

    String query4 = "a b a";
    terms = Utils.createNGrams(query4,2);
    collected = Utils.collectTerms(terms);
    assertEquals(4, collected.size());
    assertEquals(2, (int)collected.remove("a"));
    assertEquals(1, (int)collected.remove("b"));
    assertEquals(1, (int)collected.remove("a b"));
    assertEquals(1, (int)collected.remove("b a"));
    assertEquals(0, collected.size());

    String query5 = "a's b.c,";
    terms = Utils.createNGrams(query5,2);
    collected = Utils.collectTerms(terms);
    assertEquals(5, collected.size());
    assertEquals(1, (int)collected.remove("a's"));
    assertEquals(1, (int)collected.remove("b"));
    assertEquals(1, (int)collected.remove("c"));
    assertEquals(1, (int)collected.remove("a's b"));
    assertEquals(1, (int)collected.remove("b c"));
    assertEquals(0, collected.size());
  }

  @Test
  public void testCollectRatios() {
    String query0 = "";
    ArrayList<String> terms = Utils.createNGrams(query0,2);
    HashMap<String, Integer> collected = Utils.collectTerms(terms);
    HashMap<String, Double> ratios = Utils.collectRatios(collected, query0);
    assertEquals(1, ratios.size());

    String query1 = " ";
    terms = Utils.createNGrams(query1,2);
    collected = Utils.collectTerms(terms);
    ratios = Utils.collectRatios(collected, query1);
    assertEquals(0, ratios.size());

    String query2 = "a";
    terms = Utils.createNGrams(query2,2);
    collected = Utils.collectTerms(terms);
    ratios = Utils.collectRatios(collected, query2);
    assertEquals(1, ratios.size());
    assertEquals((Double)1.0, ratios.remove("a"));
    assertEquals(0, ratios.size());

    String query3 = "a a a";
    terms = Utils.createNGrams(query3,2);
    collected = Utils.collectTerms(terms);
    ratios = Utils.collectRatios(collected, query3);
    assertEquals(2, ratios.size());
    assertEquals((Double)1.0, ratios.remove("a"));
    assertEquals((Double)(4.0 / 3.0), ratios.remove("a a"));
    assertEquals(0, ratios.size());

    String query4 = "a b a";
    terms = Utils.createNGrams(query4,2);
    collected = Utils.collectTerms(terms);
    ratios = Utils.collectRatios(collected, query4);
    assertEquals(4, ratios.size());
    assertEquals((Double)(2.0 / 3.0), ratios.remove("a"));
    assertEquals((Double)(1.0 / 3.0), ratios.remove("b"));
    assertEquals((Double)(2.0 / 3.0), ratios.remove("a b"));
    assertEquals((Double)(2.0 / 3.0), ratios.remove("b a"));
    assertEquals(0, ratios.size());

    String query5 = "a's b.c,";
    terms = Utils.createNGrams(query5,2);
    collected = Utils.collectTerms(terms);
    ratios = Utils.collectRatios(collected, query5);
    assertEquals(5, ratios.size());
    assertEquals((Double)(1.0 / 3.0), ratios.remove("a's"));
    assertEquals((Double)(1.0 / 3.0), ratios.remove("b"));
    assertEquals((Double)(1.0 / 3.0), ratios.remove("c"));
    assertEquals((Double)(2.0 / 3.0), ratios.remove("a's b"));
    assertEquals((Double)(2.0 / 3.0), ratios.remove("b c"));
    assertEquals(0, ratios.size());
    
    
    String query6 = "booz allen";
    terms = Utils.createNGrams(query6,2);
    collected = Utils.collectTerms(terms);
    ratios = Utils.collectRatios(collected, query6);
    assertEquals(3, ratios.size());
    assertEquals((Double)(1.0 / 2.0), ratios.remove("booz"));
    assertEquals((Double)(1.0 / 2.0), ratios.remove("allen"));
    assertEquals((Double)(2.0 / 2.0), ratios.remove("booz allen"));
  }

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {

    // test 0 object hashmap
    HashMap<String, String> testMap = new HashMap<String, String>();
    assertEquals(0, testMap.size());

    byte[] r = Utils.serialize(testMap);
    @SuppressWarnings("unchecked")
    HashMap<String, String> testMap2 = 
    (HashMap<String, String>)Utils.deserialize(r);
    assertEquals(0, testMap2.size());


    // test 1 object hashmap
    testMap = new HashMap<String, String>();
    testMap.put("a", "a");
    assertEquals(1, testMap.size());

    r = Utils.serialize(testMap);
    @SuppressWarnings("unchecked")
    HashMap<String, String> t3= (HashMap<String, String>)Utils.deserialize(r);
    assertEquals(1, t3.size());
    assertEquals("a", t3.remove("a"));
    assertEquals(0, t3.size());

    // test 5 object hashmap
    testMap = new HashMap<String, String>();
    testMap.put("a", "a");
    testMap.put("b", "b");
    testMap.put("c", "c");
    testMap.put("d", "d");
    testMap.put("a", "g");
    testMap.put("f", "f");
    assertEquals(5, testMap.size());

      r = Utils.serialize(testMap);
      @SuppressWarnings("unchecked")
      HashMap<String, String> t4 = 
      (HashMap<String, String>)Utils.deserialize(r);
      assertEquals(5, t4.size());
      assertEquals("g", t4.remove("a"));
      assertEquals("b", t4.remove("b"));
      assertEquals("c", t4.remove("c"));
      assertEquals("d", t4.remove("d"));
      assertEquals("f", t4.remove("f"));
      assertEquals(0, t4.size());
  }

  @Test
  public void testDotProduct() {

    double[] a = {1.0, 2.0, 3.0};
    double[] b = {3.0, 2.0, 1.0};
    double dotProduct = Utils.dotProduct(a, b);
    assertEquals((Double)10.0, (Double)dotProduct);

    double[] a1 = {1.0};
    double[] b1 = {1.0};
    dotProduct = Utils.dotProduct(a1, b1);
    assertEquals((Double)1.0, (Double)dotProduct);

  }

}
