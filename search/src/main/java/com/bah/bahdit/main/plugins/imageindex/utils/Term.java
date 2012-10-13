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
package com.bah.bahdit.main.plugins.imageindex.utils;

import org.apache.accumulo.core.data.Key;

/**
 * Term is used to place a URL and its rank into a priority queue and compare 
 * to other terms and rankings
 */
public class Term {
  
  private Key key;
  private Double value;
  
  public Key getKey() {
    return key;
  }

  public Double getValue() {
    return value;
  }

  public Term(Key key, Double value) {
    this.key = key;
    this.value = value;
  }
}
