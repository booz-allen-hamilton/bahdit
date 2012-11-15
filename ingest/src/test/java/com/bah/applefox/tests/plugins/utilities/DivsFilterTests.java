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
package com.bah.applefox.tests.plugins.utilities;

import java.util.HashSet;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import com.bah.applefox.main.plugins.utilities.*;

public class DivsFilterTests {

	@Test
	public void testFilter() {
		String s1 = "<html><div id=\"dontIgnore\"> this should be counted </div>";
		s1 += "<div id='ignore'> this should not be counted <div id='anything'>" +
				"nor should this</div></div>";
		s1 += "but this should be in </html>";
		HashSet<String> removeIDs = new HashSet<String>();
		removeIDs.add("ignore");
		removeIDs.add("notInIt");
		String s2 = DivsFilter.filterDivs(s1, removeIDs);
		
		assertTrue(s2.contains("this should be counted"));
		assertFalse(s2.contains("this should not be counted"));
		assertFalse(s2.contains("nor should this"));
		assertTrue(s2.contains("but this should be in"));
	}
}
