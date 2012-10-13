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

import java.util.ArrayList;

/**
 * DocVector is used to store all the terms and their TF-IDF weights in a 
 * vector format.  The corresponding term and weight are associated with the 
 * same index in different collections. 
 * 
 * Used in : RankCalculator.java
 */
public class DocVector {

  private ArrayList<String> terms;
  private double[] vector;

  public DocVector(ArrayList<String> inputTerms, double[] inputVector) {
    terms = inputTerms;
    vector = inputVector;
  }

  public ArrayList<String> getTerms() {
    return terms;
  }

  public double[] getVector() {
    return vector;
  }
}