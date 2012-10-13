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
package com.bah.bahdit.main.plugins.fulltextindex.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.bah.bahdit.main.plugins.fulltextindex.data.DocVector;

/**
 * CosineSimilarity is used for finding the cosine similarity between a query 
 * and a document.  It will create a vector based on the query string and a 
 * vector based on the terms and frequencies of a document.  It then takes the 
 * dot product and magnitude product of these two vectors, then returns their 
 * quotient.
 * 
 */
public class CosineSimilarity {

  private static final String TOTAL_DOCS = "[[TOTAL NUM DOCS]]";
  
  /**
   * 
   * @param queryTerms - arraylist of terms in the query
   * @param query - the string of the query
   * @return hashmap of terms in query to their ratios
   */
  public static HashMap<String, Double> queryRatios(ArrayList<String> queryTerms, String query) {
    // collects same n-grams and saves their frequency ratios
    HashMap<String, Integer> queryFreqs = Utils.collectTerms(queryTerms);
    return Utils.collectRatios(queryFreqs, query);
  }

  /**
   * creates a vector with given terms, ratios, and sample
   * 
   * @param ratios - hashmap of terms and their ratios
   * @param sample - sample used for tf-idf
   * @param doc - list of terms to create vector for
   * @return DocVector
   */
  private static DocVector createVector (HashMap<String, Double> ratios, 
      HashMap<String, Integer> sample, ArrayList<String> doc) {

    // get the total number of documents
    int totalDocs = sample.get(TOTAL_DOCS);

    // creates a single-dimensional matrix to serve as the empty vector
    double[] pageRatio = new double[doc.size()];

    // loop through keys of doc, get tf-idf for each key
    for (int i = 0; i < pageRatio.length; i++) {

      String key = doc.get(i);
      Double ratio = ratios.get(key);
      Integer s = null;
      if (sample != null) s = sample.get(key);

      // if the query term is in here, place its tf-idf in matrix
      if (ratio != null) {

        // use ratio if sample doesn't contain the key
        if (s == null) pageRatio[i] = ratio;

        // otherwise, use the sample's value to calculate tf-idf
        else pageRatio[i] = Utils.calcTFIDF(ratio, totalDocs, s);
      }

      // otherwise place 0 in matrix
      else pageRatio[i] = 0;
    }

    // create the docvector
    return new DocVector(doc, pageRatio);
  }

  /**
   * creates a DocVector for a certain document
   * 
   * @param input - hashmap containing words and frequencies in a doc
   * @param sample - the sample used for tf-idf
   * @param queryTerms - arraylist of terms to provide corresponding order
   * @return DocVector containing the vector of the page
   */
  private static DocVector createPageVector(HashMap<String, Double> freqRatios, 
      HashMap<String, Integer> sample, ArrayList<String> queryTerms) {

    HashSet<String> docTerms = new HashSet<String>(freqRatios.keySet());

    // get rid of duplicate terms between query and document
    for (String s : queryTerms)
      docTerms.add(s);

    // create arraylist of terms in query and document
    ArrayList<String> doc = new ArrayList<String>(docTerms);

    return createVector(freqRatios, sample, doc);
  }

  /**
   * Computes the cosine similarity between a query and document
   * 
   * @param input - hashmap of terms to their frequencies
   * @param queryRatios - ratios of query terms
   * @param queryTerms - list of query terms
   * @param sample - sample used for tf-idf
   * @return cosine similarity between query and doc vectors
   */
  public static double computeCosineSimilarity(HashMap<String, Double> input,
      HashMap<String, Double> queryRatios, ArrayList<String> queryTerms, 
      HashMap<String, Integer> sample) {

    // create the vectors for the document and the query
    DocVector page = createPageVector(input, sample, queryTerms);
    DocVector query = createVector(queryRatios, sample, page.getTerms());

    // get the matrix forms of the vectors
    double[] queryVector = query.getVector();
    double[] pageVector = page.getVector();

    // if multiple words, calculate cosine similarity
    double dotProduct = Utils.dotProduct(queryVector, pageVector);
    double magProduct = Utils.magProduct(queryVector, pageVector);

    // no divide-by-zero errors
    if (magProduct == 0.0) return 0.0;

    return dotProduct / magProduct;
  }
}