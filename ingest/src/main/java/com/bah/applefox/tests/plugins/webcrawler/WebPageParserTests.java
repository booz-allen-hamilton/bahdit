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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import com.bah.applefox.main.plugins.webcrawler.utilities.WebPageParser;

public class WebPageParserTests {

	@Test
	public void testGetLinks() throws IOException {
		// Test the local Web Page
		// Local web page needs the three doc*.html files found in this source folder
		String url = "http://c02fgtfedf8v.usae.bah.com/~zacharyauld/doc1.html";
		WebPageParser p1 = new WebPageParser(url, "TestCrawler");

		assertEquals(2, p1.getChildLinks().size());
		assertEquals("http://c02fgtfedf8v.usae.bah.com/~zacharyauld/doc1.html",
				p1.getParent().toString());
		assertEquals("TestCrawler", p1.getUserAgent());
	}

	@Test
	public void testParse() throws IOException {
		String url = "http://c02fgtfedf8v.usae.bah.com/~zacharyauld/doc1.html";
		WebPageParser p1 = new WebPageParser(url, "TestCrawler");

		p1.parse();
		assertEquals(3, p1.getLinks().size());

	}

	@Test
	public void testGetPageContents() throws IOException {
		String url = "http://c02fgtfedf8v.usae.bah.com/~zacharyauld/doc3.html";
		WebPageParser p1 = new WebPageParser(url, "TestCrawler");

		BufferedReader temp = new BufferedReader(new FileReader(new File(
				"/Users/zacharyauld/Sites/doc3.html")));

		StringBuffer bodyContents = new StringBuffer();
		char cbuf[] = new char[100];
		int len = 0;

		while ((len = temp.read(cbuf)) != -1) {
			bodyContents.append(cbuf, 0, len);
		}

		assertEquals(false, p1.isParsed());

		assertEquals(bodyContents.toString(), p1.getBody());

		assertEquals(true, p1.isParsed());
		
		temp.close();

	}
}
