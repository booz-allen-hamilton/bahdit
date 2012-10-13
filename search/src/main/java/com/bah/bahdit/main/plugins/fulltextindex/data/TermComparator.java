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

import java.util.Comparator;

/**
 * TermComparator compares two rankings for the priority queue 
 *
 */
public class TermComparator implements Comparator<Term> {
  
  private static final String DELIMITER = "[ ]";
  
  @Override
  /**
   * Ranks first by their rankings.  If rankings are the same, then compare by
   * URL length (shorter > longer).  If URL length is the same, compare lastly 
   * by string lexicographical order 
   */
  public int compare(Term t1, Term t2) {
    
    int valueComp = t1.getValue().compareTo(t2.getValue());
    
    if (valueComp == 0) {
      
      // get column families
      String st1 = t1.getKey().getColumnFamily().toString();
      String st2 = t2.getKey().getColumnFamily().toString();
      
      // isolate URLs
      String url1 = st1.substring(0, st1.indexOf(DELIMITER));
      String url2 = st2.substring(0, st2.indexOf(DELIMITER));
      
      // get URL lengths
      Integer urlLength1 = url1.length();
      Integer urlLength2 = url2.length();
      
      // reverse so shorter links are ranked higher
      int lengthComp = urlLength2.compareTo(urlLength1);
      
      if (lengthComp == 0)
        return url1.compareTo(url2);
      
      return lengthComp;
    }
    return valueComp;
  }
}
