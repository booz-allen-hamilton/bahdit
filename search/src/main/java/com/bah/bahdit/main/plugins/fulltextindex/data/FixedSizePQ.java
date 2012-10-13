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
import java.util.TreeSet;

/**
 * FixedSizePQ is a priority queue with a fixed value - It will keep 
 * the top "k" elements in sorted order.  The fixed size priority queue uses a 
 * tree set as its base structure.
 * 
 * Used for : getting the rankings of the pages from the search query
 */
@SuppressWarnings("serial")
public class FixedSizePQ<E> extends TreeSet<E> {

  private int elementsLeft;

  public FixedSizePQ(int maxSize, Comparator<E> comparator) {
    super(comparator);
    this.elementsLeft = maxSize;
  }

  /**
   * Adds the element to the priority queue if there is room or if it is 
   * ranked higher than the lowest element already in the priority queue.  In
   * the latter case, pushes out the lowest element
   * 
   * @return true if element was added, false otherwise
   */
  @Override
  public boolean add(E e) {
    
    // max size was initiated to zero => just return false
    if (elementsLeft == 0 && size() == 0)
      return false;
    
    // queue isn't full
    else if (elementsLeft > 0) {
      // only decrement elementsLeft if successfully added
      if (super.add(e))
        elementsLeft--;
      return true;
    } 
    
    // queue is full
    else {
      // compare to the least of these elements
      int compared = super.comparator().compare(e, this.first());

      // new element is larger than the least in queue => pull the 
      // least and add new one to queue
      if (compared > 0) {
        pollFirst();
        super.add(e);
        return true;
      } 
      
      // new element is less than the least in queue
      else return false;
    }
  }
}