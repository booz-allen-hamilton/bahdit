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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

public class Utils {

  public static final double w = 0.5;

  /**
   * if inputString = " " : result = empty arraylist
   * if inputString = ""  : result = arraylist of size 1 with "" as element
   * 
   * @param inputString - string to be split into n-grams
   * 
   * @return arraylist of n-grams from up to constant max n-gram value
   */
  public static ArrayList<String> createNGrams(String inputString, int n) {
    ArrayList<String> terms = new ArrayList<String>();
    String[] words = getWords(inputString);

    if (words.length == 0) return terms;

    // makes sure the max n-gram is within the bounds of the query
    if (n <= 0) n = 1;
    else if (n > words.length) n = words.length;

    // creates n-grams from 1 to n
    for (int x = 1; x <= n; x++)
      for (int i = 0; i < words.length - x + 1; i++)
        terms.add(concat(words, i, i + x));

    return terms;
  }

  /**
   * @param input - arraylist of n-grams in a string
   * 
   * @return hashmap of words mapped to their frequency in the input string
   */
  public static HashMap<String, Integer> collectTerms(ArrayList<String> input) {

    ArrayList<String> docWords = input;
    HashMap<String, Integer> terms = new HashMap<String, Integer>();

    for (String s : docWords) {

      // inserts new key if doesn't previously exist
      if (!terms.containsKey(s))
        terms.put(s, 1);

      // updates the frequency count
      else
        terms.put(s, terms.get(s) + 1);

    }

    return terms;
  }

  /**
   * Returns terms and their ratios (frequency divided by document length)
   * 
   * @param terms - hashmap of terms and their frequencies
   * @param input - the string of the document
   * @return hashmap of terms and their ratios
   */
  public static HashMap<String, Double> collectRatios
  (HashMap<String, Integer> terms, String input) {

    HashMap<String, Double> freqRatios = new HashMap<String, Double>();

    // get the document length
    String[] words = getWords(input);
    double wordCount = words.length;

    // for all keys, get the frequencies and convert into ratios
    for (Entry<String, Integer> e : terms.entrySet()) {
      String key = e.getKey();
      double freq = e.getValue();

      // get length of term in words
      // i.e. "apple fox" in "apple fox bear" => 2/3 rather than 1/3
      double multiplier = (key.split(" ")).length;

      // convert freqs to ratios
      double ratio = (freq * multiplier) / wordCount;
      freqRatios.put(key, ratio);
    }

    return freqRatios;
  }


  /**
   * Produces an array of strings, separated by the regex (mostly punctuation)
   * 
   * @param input - the string version of a document or query
   * @return an array of all the words in the input
   */
  public static String[] getWords(String input) {

    // splits based on punctuation, not contractions
    String regexp = "[^-\'\\w]+";

    return (input.toLowerCase()).split(regexp);
  }

  /**
   * Turns an object into a byte array
   * Object must be serializable
   * 
   * @param o - object to be serialized
   * @return byte array representation of the object
   * @throws IOException
   */
  public static byte[] serialize(Object o) throws IOException {
    ByteArrayOutputStream ba = new ByteArrayOutputStream(1000);
    ObjectOutputStream oba = new ObjectOutputStream(ba);
    oba.writeObject(o);
    return ba.toByteArray();
  }

  /**
   * Turns a byte array into its original object form
   * 
   * @param b - byte array to be read as object
   * @return object from the byte array representation
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static Object deserialize(byte[] b) throws IOException, 
  ClassNotFoundException {
    ByteArrayInputStream ba = new ByteArrayInputStream(b);
    ObjectInputStream oba = new ObjectInputStream(ba);
    return oba.readObject();
  }

  /**
   * Calculates the dot product between two vectors and returns as double
   * 
   * @param a - first vector
   * @param b - second vector
   * @return dot product
   */
  public static double dotProduct(double[] a, double[] b) {

    double dotProduct = 0;

    // gets a_1*b_1 + a_2*b_2 + ... + a_n*b_n
    for (int i = 0; i < a.length; i++)
      dotProduct += a[i] * b[i];

    return dotProduct;
  }

  /**
   * Calculates the magnitude of two vectors return their product
   * 
   * @param a - first vector
   * @param b - second vector
   * @return mag_a * mag_b
   */
  public static double magProduct(double[] a, double[] b) {

    double magA = 0;
    double magB = 0;

    for (int i = 0; i < a.length; i++) {
      magA += a[i] * a[i];
      magB += b[i] * b[i];
    }

    return Math.sqrt(magA * magB);
  }

  /**
   * @param tf - term frequency / doc word size
   * @param numDocs - number of total documents
   * @param numContainingDocs - number of documents containing the term
   * @return tf-idf value
   */
  public static double calcTFIDF (double tf, int numDocs, 
      int numContainingDocs) {
    if (numContainingDocs == 0) return 0;
    if (numDocs == numContainingDocs) numContainingDocs++;
    double idf = Math.log((double)numDocs / (double)(numContainingDocs));
    return tf * idf;
  }

  /**
   * used by : createNGrams
   * 
   * @param words - an array of single words
   * @param start - beginning index
   * @param end   - ending index
   * 
   * @return concatenated string 
   */
  private static String concat(String[] words, int start, int end) {

    StringBuilder str = new StringBuilder();
    str.append(words[start]);

    for (int i = start + 1; i < end; i++)
      str.append(" " + words[i]);

    return str.toString();
  }

  /**
   * Returns the linear combination of pagerank and cosine similarity
   * Both ranks are normalized between 0 and 1
   * Combines the two rankings for the user based on constant "w"
   * 
   * @param cosim
   * @param pagerank
   * 
   * @return final ranking
   */
  public static Double rank(Double cosim, Double pagerank) {
    return (w * pagerank) + ((1 - w) * cosim);
  }
}
