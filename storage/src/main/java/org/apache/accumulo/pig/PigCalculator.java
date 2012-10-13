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
package org.apache.accumulo.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;

import com.bah.bahdit.main.plugins.fulltextindex.FullTextIndex;
import com.bah.bahdit.main.plugins.fulltextindex.utils.CosineSimilarity;
import com.bah.bahdit.main.plugins.fulltextindex.utils.Utils;
import com.bah.bahdit.main.search.Search;


/**
 * A copy of Rank Calculator from Bahdit used to specify queries 
 * with Pig Storage. Needs to be tweaked to return key value pairs 
 * relevant to Apache Pig.
 */

public class PigCalculator extends WrappingIterator {

  private SortedKeyValueIterator<Key, Value> source;
  private Set<String> ngrams;
  private Key topKey;
  private Value topValue;
  private String query;
  private ArrayList<String> queryTerms;
  private HashMap<String, Double> queryRatios;
  private HashMap<String, Integer> sampleTable;
  private HashMap<String, Double> pagerankTable;
  private boolean first = true;
  private boolean reportLast = true;
  private double maxPR;

  private static final String MAX_PR = "[[MAX_PR]]";

  @Override
  public Key getTopKey() {
    return topKey;
  }

  @Override
  public Value getTopValue() {
    return topValue;
  }

  @Override
  public boolean hasTop() {
    return topKey != null;
  }

  @Override
  /**
   * Combines the next range of rows with the same document column family
   */
  public void next() throws IOException {
    // if the first time called, ignore.  This is because seek() will call 
    // execute(), which is one more than necessary.
    if (first) first = false;
    else rank();
  }

  @Override
  /**
   * calls cosim() to get to the next() function.  Sets the topKey and 
   * topValue variables.
   */
  public void seek(Range range, Collection<ByteSequence> seekColFam, 
      boolean inclusive) throws IOException {
    source.seek(range, seekColFam, inclusive);
    rank();
  }

  @SuppressWarnings("unchecked")
  @Override
  /**
   * Creates a new CosimCalculator and gets the query and query vector for 
   * comparing to the document vectors
   * 
   * @param source
   * @param options - the query will be stored in here
   * @param env
   */
  public void init(SortedKeyValueIterator<Key, Value> source,
      Map<String, String> options, IteratorEnvironment envir)
          throws IOException {

    super.init(source, options, envir);

    // get the max n grams
    int n = Integer.parseInt(options.get(Search.MAX_NGRAMS));

    // get the query from the options map
    query = options.get(Search.QUERY);
    queryTerms = Utils.createNGrams(query, n);
    queryRatios = CosineSimilarity.queryRatios(queryTerms, query);

    // get the sample table from the options map
    String sample = options.get(FullTextIndex.FT_SAMPLE);
    byte[] bST = null;
    try {
      bST = sample.getBytes(FullTextIndex.ENCODING);
      sampleTable = (HashMap<String, Integer>)Utils.deserialize(bST);
    } catch (ClassNotFoundException e) {
      sampleTable = null;
    }

    // get the pagerank table from the options map
    String pagerank = options.get(Search.PAGERANK_TABLE);
    byte[] bPR = pagerank.getBytes(FullTextIndex.ENCODING);
    try {
      pagerankTable = (HashMap<String, Double>)Utils.deserialize(bPR);
    } catch (ClassNotFoundException e) {
      pagerankTable = null;
    }

    // get the max page rank of the table for normalizing
    Double max_PR = pagerankTable != null ? pagerankTable.get(MAX_PR) : null;
    maxPR = max_PR != null ? max_PR : 0.0;
    
    this.source = source;

    // creates ngrams from the query
    this.ngrams = new HashSet<String>(Utils.createNGrams(query, n));
  }

  /**
   * Finds the documents that contain all the search terms
   * If a document contains those terms, it is put into a Map and the cosine 
   * similarity of the document is calculated.  The key and the cosine 
   * similarity are then returned as a single row.
   */
  private void rank() throws IOException {

    // if true, signals that we've scanned a whole column family and it matches
    boolean foundDoc = false;

    // also make sure we don't run out of records
    while (!foundDoc && source.hasTop() ) {



      HashMap<String, Double> terms = new HashMap<String, Double>();
      Set<String> searchTerms = new HashSet<String>(ngrams);

      Key firstKey = source.getTopKey();
      Key currentKey = source.getTopKey();
      Value currentValue = source.getTopValue();

      while (inSameDocumentAndTerm(currentKey, firstKey)) {

        String term = currentKey.getColumnQualifier().toString();
        searchTerms.remove(term);

        // split by commas (i.e. "40 0.4" -> ["40", "0.4"])
        String groupedValues = null;
        try {
          groupedValues = (String) Utils.deserialize(currentValue.get());
        } catch (ClassNotFoundException e) { e.printStackTrace(); }

        String[] values = groupedValues.split(",");

        // get term and ratio, calculate tf-idf and place in hashmap for later
        try {
          Double ratio = Double.parseDouble(values[1]);
          terms.put(term, ratio);
        } catch(ArrayIndexOutOfBoundsException e) { e.printStackTrace(); }

        // move source to next, resets current key and value to be the new
        // top key and value
        currentKey = currentSourceTopOrNull(firstKey);

        if (currentKey == null) break;

        currentValue = source.getTopValue();
      }

      // all of the search terms have been accounted for
      if (searchTerms.size() == 0) {

        foundDoc = true;

        // calculate cosine similarity with our hashmap
        Double cosim = CosineSimilarity.computeCosineSimilarity(terms, 
            queryRatios, queryTerms, sampleTable);

        String cf = firstKey.getColumnFamily().toString();
        String url = cf.substring(0, cf.indexOf("[ ]"));
        Double pagerank;
        if (pagerankTable == null || pagerankTable.size() == 0 || maxPR == 0.0)
          pagerank = 0.0;
        // get the pagerank and normalize to span 0 to 1
        else
          pagerank = pagerankTable.get(url) / maxPR;

        Double rank = Utils.rank(cosim, pagerank);
        
        // get pagerank table for pagerank calculations
        String termsString = null;
        try {
        	termsString = new String(Utils.serialize(pagerankTable), FullTextIndex.ENCODING);
        } catch (Exception e) {}
     
        topKey = new Key(new Key(firstKey.getRow(), firstKey.getColumnFamily(), new Text(termsString)));
        topValue = new Value(Utils.serialize(rank));
      }
    }

    // set top key and value to null after the last document has been reported
    if(!source.hasTop()) {
      if(reportLast && foundDoc)
        reportLast = false;
      else {
        topKey = null;
        topValue = null;
      }
    }
  }

  /**
   * Makes sure the execute method is still the same rowid and document
   */
  private boolean inSameDocumentAndTerm(Key currentKey, Key firstKey) {

    boolean notNull = (currentKey != null);
    boolean sameRow = currentKey.getRow().equals(firstKey.getRow());
    Text currentCF = currentKey.getColumnFamily();
    Text firstCF = firstKey.getColumnFamily();
    boolean sameCF = currentCF.equals(firstCF);

    return notNull && sameRow && sameCF && source.hasTop();
  }

  /**
   * Advances the source pointer and returns the next key, if the key is part 
   * of the same document as the first key to which it is being compared.
   * Otherwise, return null.
   */
  private Key currentSourceTopOrNull(Key firstKey) throws IOException {

    if(source.hasTop()) {

      if(source.getTopKey().equals(firstKey, PartialKey.ROW_COLFAM)) {

        source.next();
        return source.hasTop() ? source.getTopKey() : null;
      } 

      return null;
    } 
    return null;
  }
}
