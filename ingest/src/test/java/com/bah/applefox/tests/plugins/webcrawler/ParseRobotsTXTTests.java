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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

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
import com.bah.applefox.main.plugins.webcrawler.utilities.RobotsTXT;

public class ParseRobotsTXTTests {

	private static AbstractHandler handler = new AbstractHandler() {

		@Override
		public void handle(String target, HttpServletRequest request,
				HttpServletResponse response, int dispatch) throws IOException,
				ServletException {
			response.setStatus(HttpServletResponse.SC_OK);
			((Request) request).setHandled(true);
			File file = new File("src/test/resources/thinkgeek-bots.txt");
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
	public static void stopServer() throws Exception {
		server.stop();
	}

	@Test
	public void testRobotsTXT() throws IOException, PageCrawlException {
		// At the current time, thinkgeek.com has a robots.txt file with 9
		// disallows for all crawlers, and disallows all for rogerbot
		RobotsTXT r1 = RobotsTXT.get(new URL("http://localhost:" + port
				+ "/robots.txt"), "TestCrawl");
		assertTrue(r1.allowed("/brain"));
		assertFalse(r1.allowed("/brain/"));
		assertEquals(0, r1.getDelay());

		RobotsTXT r2 = RobotsTXT.get(new URL("http://localhost:" + port
				+ "/robots.txt"), "rogerbot");

		assertFalse(r2.allowed("/"));
	}

}
