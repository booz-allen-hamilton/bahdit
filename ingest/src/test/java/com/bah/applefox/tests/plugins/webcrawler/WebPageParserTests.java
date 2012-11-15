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
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import com.bah.applefox.main.plugins.webcrawler.utilities.PageCrawlException;
import com.bah.applefox.main.plugins.webcrawler.utilities.WebPageCrawl;

public class WebPageParserTests {

	private static AbstractHandler handler = new AbstractHandler() {

		@Override
		public void handle(String target, HttpServletRequest request,
				HttpServletResponse response, int dispatch) throws IOException,
				ServletException {
			File file = new File("src/test/resources" + target);
			((Request)request).setHandled(true);
			if(!file.exists()){
				response.setStatus(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
			response.setStatus(HttpServletResponse.SC_OK);
			FileInputStream fis = new FileInputStream(file);
			int read = 0;
			byte[] buffer = new byte[1024];
			OutputStream sos = response.getOutputStream();
			while ((read = fis.read(buffer)) >= 0) {
				sos.write(buffer, 0, read);
			}
			fis.close();
			response.flushBuffer();
		}
	};
	private static Server server;
	private static int port;

	@BeforeClass
	public static void startServer() throws Exception {
		server = new Server(0);
		server.setHandler(handler);
		server.start();
		port = server.getConnectors()[0].getLocalPort();
	}
	
	@AfterClass
	public static void stopServer() throws Exception{
		server.stop();
	}

	@Test
	public void testGetLinks() throws IOException, PageCrawlException {
		// Test the local Web Page
		// Local web page needs the three doc*.html files found in this source
		// folder
		String url = "http://localhost:" + port + "/doc1.html";
		WebPageCrawl p1 = new WebPageCrawl(url, "TestCrawler", Collections.<String>emptySet());

		assertEquals(3, p1.getChildLinks().size());
		assertEquals("http://localhost:" + port + "/doc1.html", p1.getParent()
				.toString());
		assertEquals("TestCrawler", p1.getUserAgent());
	}

	@Test
	public void testParse() throws IOException, PageCrawlException {
		String url = "http://localhost:" + port + "/doc1.html";
		WebPageCrawl p1 = new WebPageCrawl(url, "TestCrawler", Collections.<String>emptySet());

		assertEquals(4, p1.getLinks().size());

	}

	@Test
	public void testGetPageContents() throws IOException, PageCrawlException {
		String url = "http://localhost:" + port + "/doc3.html";
		WebPageCrawl p1 = new WebPageCrawl(url, "TestCrawler", Collections.<String>emptySet());

		BufferedReader temp = new BufferedReader(new FileReader(new File(
				"src/test/resources/doc3.html")));

		String links = "TestLink1 TestLink2";
		String bodyContents = "hurr hurr hurr durr durr durr";

		assertEquals(Arrays.asList(links,bodyContents), p1.getBody());


		temp.close();

	}
}
