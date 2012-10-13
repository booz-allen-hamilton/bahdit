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
package com.bah.applefox.tests.plugins.webcrawler;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;

import org.junit.Test;

import com.bah.applefox.main.plugins.webcrawler.utilities.ParseRobotsTXT;

public class ParseRobotsTXTTests {

	@Test
	public void testRobotsTXT() throws IOException {
		// At the current time, thinkgeek.com has a robots.txt file with 9
		// disallows for all crawlers, and disallows all for rogerbot
		ParseRobotsTXT r1 = new ParseRobotsTXT(new URL(
				"http://www.thinkgeek.com"), "TestCrawl");
		assertEquals(9, r1.getDisallows().size());

		assertEquals(0, r1.getDelay());

		ParseRobotsTXT r2 = new ParseRobotsTXT(new URL(
				"http://www.thinkgeek.com"), "rogerbot");

		assertEquals(1, r2.getDisallows().size());
	}

}
