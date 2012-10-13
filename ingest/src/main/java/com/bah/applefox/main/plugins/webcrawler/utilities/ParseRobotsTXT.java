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
package com.bah.applefox.main.plugins.webcrawler.utilities;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The ParseRobotsTXT class is used to parse the robots.txt file from a website
 * The robots.txt file is used to specify which files and directories cannot be
 * accessed by a robot, along with the expected delay time between page requests
 * 
 */
public class ParseRobotsTXT {
	// The error log
	private static Log log = LogFactory.getLog(ParseRobotsTXT.class);
	// The page's URL
	private URL url = null;
	// The UserAgent field
	private String UserAgent = null;
	// The expected crawl delay (if not specified, assumed to be 0)
	private int Crawl_delay = 0;
	// The location of the sitemap
	private String sitemap = null;

	private String pageContents;

	/**
	 * Constructor for the ParseRobotsTXT object
	 * 
	 * @param url
	 *            - the url to get the robots.txt file from
	 * @param UserAgent
	 *            - the UserAgent request property field item
	 * @throws MalformedURLException
	 */
	public ParseRobotsTXT(URL url, String UserAgent)
			throws MalformedURLException {

		// Sets url to the parent URL's robot.txt file
		this.url = new URL(url.getProtocol() + "://" + url.getHost()
				+ "/robots.txt");
		// Sets the UserAgent field
		this.UserAgent = UserAgent;
	}

	/**
	 * Returns all of the disallowed links for a robot
	 * 
	 * @return - allDisallowed
	 */
	public HashSet<URL> getDisallows() throws IOException {
		// If the url or UserAgent were not properly constructed, return an
		// empty HashSet
		if (url == null || UserAgent == null) {
			return new HashSet<URL>();
		}

		// Sets a new WebPageParser to get the contents of the robots.txt
		// page
		setPageContents();

		// Returns the parsed HashSet of URLs
		return parseDisallows(pageContents, UserAgent);

	}

	/**
	 * Private function for parsing page content into disallowed links
	 * 
	 * @param contents
	 *            - the page contents
	 * @param UserAgent
	 *            - the UserAgent request property field
	 * @return - a list of all disallowed links
	 */
	private HashSet<URL> parseDisallows(String contents, String UserAgent) {

		// Set the contents to lower case for ease of comparison
		String tempString = contents.toLowerCase();
		// Set UserAgetn to lower case for ease of comparison
		UserAgent = UserAgent.toLowerCase();

		// check for a specific disallow group for this UserAgent field
		if (tempString.contains("user-agent: " + UserAgent)) {
			// Parse out the user agent section
			int len = 12 + UserAgent.length();
			tempString = tempString.substring(tempString.indexOf("user-agent: "
					+ UserAgent)
					+ len);

		} else if (tempString.contains("user-agent: *")) {
			// If no specific disallow section is found, but there is a general
			// disallow, parse that
			tempString = tempString.substring(tempString
					.indexOf("user-agent: *") + 13);

		} else {
			// If there are no applicable disallows, return an empty HashSet of
			// URLs
			return new HashSet<URL>();
		}

		// If there is another user-agent field, parse that out and all that
		// follows it
		if (tempString.contains("user-agent:")) {
			tempString = tempString.substring(0,
					tempString.indexOf("user-agent"));
		}

		// The disallowed links
		HashSet<URL> disallowed = new HashSet<URL>();

		// Splits the applicable data by new lines
		String[] lines = tempString.split("\\r?\\n");

		for (String s : lines) {

			// Check each line that is a disallow or delay line
			if (s.startsWith("disallow: ")) {
				// Remove "disallow: "
				s = s.substring(10);

				// Try to add the disallowed links

				try {
					if (!s.contains("#")) {
						disallowed.add(new URL(url.getProtocol()
								+ "://"
								+ url.getHost().substring(0,
										url.getHost().length()) + s.trim()));
					} else {

						disallowed.add(new URL(url.getProtocol()
								+ "://"
								+ url.getHost().substring(0,
										url.getHost().length())
								+ s.substring(0, s.indexOf("#")).trim()));

					}

				} catch (MalformedURLException e) {
					log.error(e.getMessage());
				}

			} else if (s.startsWith("crawl-delay: ")) {
				// Remove "crawl-delay: "
				s = s.substring(13);
				Crawl_delay = Integer.parseInt(s);
			} else if (s.startsWith("sitemap: ")) {
				s = s.substring(9);
				sitemap = s;

			}

		}

		if (sitemap == null || !sitemap.endsWith(".xml")) {
			sitemap = "";
		}

		return disallowed;

	}

	/**
	 * Gets the delay time that the robots.txt file requests
	 * 
	 * @return - delay in seconds
	 */
	public int getDelay() throws IOException {

		if (!(Crawl_delay > 0)) {
			if (pageContents != null && pageContents.length() != 0) {
				parseDisallows(pageContents, UserAgent);
			} else {
				setPageContents();
				parseDisallows(pageContents, UserAgent);
			}
		}

		return Crawl_delay;
	}

	/**
	 * Gets the location of the sitemap specified by robots.txt
	 * 
	 * @return - the location of the xml file
	 */
	public String getSitemap() throws IOException {
		if (sitemap == null) {
			if (pageContents != null && pageContents.length() != 0) {
				parseDisallows(pageContents, UserAgent);
			} else {
				setPageContents();
				parseDisallows(pageContents, UserAgent);
			}
		}

		return sitemap;
	}

	/**
	 * Private function to set the contents of the page [called in parse()]
	 * 
	 * @throws IOException
	 * 
	 */
	private void setPageContents() throws IOException {

		// Open the URL Connection and assign it the request property fields of
		// "User-Agent" and UserAgent's value
		URLConnection con = url.openConnection();

		con.setRequestProperty("User-Agent", UserAgent);

		// Sets the connection timeout (in milliseconds)
		con.setConnectTimeout(1000);

		// Tries to match the character set of the Web Page
		String charset = "utf-8";
		try {
			Matcher m = Pattern.compile("\\s+charset=([^\\s]+)\\s*").matcher(
					con.getContentType());
			charset = m.matches() ? m.group(1) : "utf-8";
		} catch (Exception e) {
			log.error("Page had no specified charset");
		}

		// Reader derived from the URL Connection's input stream, with the
		// given character set
		Reader r = new InputStreamReader(con.getInputStream(), charset);

		// String Buffer used to append each chunk of Web Page data
		StringBuffer buf = new StringBuffer();

		// Tries to get an estimate of bytes available
		int BUFFER_SIZE = 0;
		BUFFER_SIZE = con.getInputStream().available();

		// If BUFFER_SIZE is too small, increases the size
		if (BUFFER_SIZE <= 1000) {
			BUFFER_SIZE = 1000;
		}

		// Character array to hold each chunk of Web Page data
		char[] ch = new char[BUFFER_SIZE];

		// Read the first chunk of Web Page data
		int len = r.read(ch);

		// Loops until end of the Web Page is reached
		while (len != -1) {

			// Appends the data chunk to the string buffer and gets the next
			// chunk
			buf.append(ch, 0, len);
			len = r.read(ch, 0, BUFFER_SIZE);

		}

		// Sets the pageContents to the newly created string
		pageContents = buf.toString();

	}
}
