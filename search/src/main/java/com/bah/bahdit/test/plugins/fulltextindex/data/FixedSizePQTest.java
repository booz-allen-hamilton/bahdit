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
package com.bah.bahdit.test.plugins.fulltextindex.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;

import org.junit.Test;

import com.bah.bahdit.main.plugins.fulltextindex.data.FixedSizePQ;


public class FixedSizePQTest {

  class DoubleComparator implements Comparator<Double> {
    @Override
    public int compare(Double t1, Double t2) {
      return t1.compareTo(t2);
    }
  }
  
  class IntComparator implements Comparator<Integer> {
    @Override
    public int compare(Integer t1, Integer t2) {
      return t1.compareTo(t2);
    }
  }
  
	@Test
	public void testIntQueue() {

		FixedSizePQ<Integer> queue = 
		    new FixedSizePQ<Integer>(5, new IntComparator());

		assertTrue(queue.add(40));
		assertTrue(queue.add(15));
		assertTrue(queue.add(20));
		assertTrue(queue.add(10));
		assertTrue(queue.add(55));
		assertTrue(queue.add(60));
		assertTrue(queue.add(25));
		assertTrue(queue.add(30));
		assertTrue(queue.add(35));
		assertTrue(queue.add(45));
		assertTrue(queue.add(50));
		assertFalse(queue.add(10));
		assertEquals(5, queue.size());
		assertEquals(40, (int)queue.first());
		assertEquals(60, (int)queue.last());
		assertFalse(queue.isEmpty());

	}

	@Test
	public void testDoubleQueue() {
		FixedSizePQ<Double> doubleQ = 
		    new FixedSizePQ<Double>(3, new DoubleComparator());

		double a1 = 2.1;
		double a2 = 2.2;
		double a3 = 5.2;
		double a4 = 2.3;
		double a5 = 2.4;
		double a6 = 9.4;
		double a7 = 56.2;
		double a8 = 5.2;

		assertTrue(doubleQ.add(a1));
		assertTrue(doubleQ.add(a2));
		assertTrue(doubleQ.add(a3));
		assertTrue(doubleQ.add(a4));
		assertTrue(doubleQ.add(a5));
		assertTrue(doubleQ.add(a6));
		assertTrue(doubleQ.add(a7));
		assertFalse(doubleQ.add(a8));
		assertEquals((Double)5.2, doubleQ.first());
		assertEquals((Double)56.2, doubleQ.last());
		assertEquals((Double)56.2, doubleQ.pollLast());
		assertEquals((Double)9.4, doubleQ.last());
	}

	@Test
	public void testDoubleSmallList() {
		FixedSizePQ<Double> doubleQ = 
		    new FixedSizePQ<Double>(6, new DoubleComparator());	
		double a1 = 2.1;
		double a2 = 2.2;
		double a3 = 5.2;

		assertTrue(doubleQ.add(a1));
		assertTrue(doubleQ.add(a2));
		assertTrue(doubleQ.add(a3));
		assertEquals((Double)5.2, doubleQ.last());
		assertEquals((Double)2.1, doubleQ.first());
	}
}
