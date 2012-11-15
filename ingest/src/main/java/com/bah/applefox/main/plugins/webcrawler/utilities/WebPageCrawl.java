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

//Imports
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * The WebPageCrawl class contains the tools needed to extract URL links from a
 * Web Page, along with getting body contents and getting only child URLs
 * 
 */

public class WebPageCrawl {

	private static final Log log = LogFactory.getLog(WebPageCrawl.class);

	// The parent URL of the page
	private final URI parentURL;

	// The UserAgent field of the server request property
	private final String UserAgent;

	// The title of the web page;
	private final String title;

	// The encoded contents of the page
	private final List<String> pageContents;

	// ArrayList of all links found on the page, in string format
	private final List<String> allLinks;

	// ArrayList of all image locations found on the page, in string format
	private final List<String> allImages;

	private final RobotsTXT robotsTXT;

	/**
	 * Constructor for the WebPageParser object. Runs the page crawl when it is
	 * invoked.
	 * 
	 * @param parentURL
	 *            - the URL of the page to be parsed
	 * @param UserAgent
	 *            - the UserAgent of the server request property
	 * @throws PageCrawlException
	 *             If an error occurs crawling the page.
	 */
	public WebPageCrawl(String parentURL, String UserAgent,
			Set<String> authorityLimits) throws PageCrawlException {
		// get the url, check if we can crawl this page.
		URL url;
		try {
			url = new URL(parentURL);
		} catch (MalformedURLException e) {
			throw new PageCrawlException(e);
		}
		robotsTXT = RobotsTXT.get(url, UserAgent);
		this.UserAgent = UserAgent;
		try {
			this.parentURL = url.toURI();
		} catch (URISyntaxException e) {
			throw new PageCrawlException(e);
		}

		if (robotsTXT.allowed(parentURL)) {
			URLConnection con;
			try {
				con = url.openConnection();
			} catch (MalformedURLException e) {
				throw new PageCrawlException(e);
			} catch (IOException e) {
				throw new PageCrawlException(e);
			}
			con.setRequestProperty("User-Agent", UserAgent);
			InputStream stream = null;
			try {
				try {
					stream = con.getInputStream();
				} catch (IOException e) {
					throw new PageCrawlException(e);
				}
				Parser parser = new AutoDetectParser();
				ParagraphContentHandler paragraphGetter = new ParagraphContentHandler();
				BodyContentHandler bodyHandler = new BodyContentHandler(
						paragraphGetter);
				// link handlers also do image tags
				LinkContentHandler linkHandler = new LinkContentHandler();
				ContentHandler overallHandler = new TeeContentHandler(
						bodyHandler, linkHandler);
				Metadata metadata = new Metadata();
				ParseContext context = new ParseContext();
				try {
					parser.parse(stream, overallHandler, metadata, context);
				} catch (IOException e) {
					throw new PageCrawlException(e);
				} catch (SAXException e) {
					throw new PageCrawlException(e);
				} catch (TikaException e) {
					throw new PageCrawlException(e);
				}
				// WE FINALLY GET THE DATA!
				String docTitle = metadata.get("title");
				this.title = docTitle != null ? docTitle : "";
				pageContents = paragraphGetter.getParagraphs();
				List<String> images = new ArrayList<String>();
				List<String> links = new ArrayList<String>();
				for (Link link : linkHandler.getLinks()) {
					URI linkURL = this.parentURL.resolve(link.getUri());
					if (authorityLimits.size() > 0
							&& !authorityLimits
									.contains(linkURL.getAuthority())) {
						continue;
					}
					String protocol = linkURL.getScheme();
					if (!protocol.equals("http") && !protocol.equals("https"))
						continue;
					if (link.isImage()) {
						images.add(linkURL.toString());
					}
					if (link.isAnchor()) {
						links.add(linkURL.toString());
					}
				}
				allImages = Collections.unmodifiableList(images);
				allLinks = Collections.unmodifiableList(links);
			} finally {
				if (stream != null) {
					try {
						stream.close();
					} catch (IOException e) {
						throw new PageCrawlException(e);
					}
				}
			}
		} else {
			title = "";
			pageContents = Collections.emptyList();
			allImages = Collections.emptyList();
			allLinks = Collections.emptyList();
		}

	}

	/**
	 * Returns the Parent URL as constructed
	 * 
	 * @return - parentURL
	 */
	public URL getParent() {
		try {
			return parentURL.toURL();
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns the UserAgent as constructed
	 * 
	 * @return - UserAgent
	 */
	public String getUserAgent() {
		return UserAgent;
	}

	/**
	 * Returns the title of the web page
	 * 
	 * @return - title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * Gets all allowed links from the Web Page
	 * 
	 * @return - allLinks
	 */
	public Set<String> getLinks() {
		// temp is used to eliminate duplicate links
		HashSet<String> temp = new HashSet<String>();
		temp.addAll(allLinks);
		return temp;
	}

	/**
	 * Gets all allowed child links from the Web Page
	 * 
	 * @return - allLinks (excluding non-child links and parent link)
	 */
	public Set<String> getChildLinks() {
		// temp is used to eliminate duplicate links
		HashSet<String> temp = new HashSet<String>();

		// Ensure a link is a child link, then add it to temp
		for (String u : allLinks) {
			try {
				if (new URL(u).getHost().equals(parentURL.getHost())) {
					temp.add(u);
				}
			} catch (MalformedURLException e) {
				// This catch statement should never be reached because the URLs
				// were already confirmed, but if it is write to the error log
				log.error(e.getMessage());
			}
		}
		return temp;
	}

	public List<String> getBody() {
		// If the Web Page has not been parsed, parse it
		return pageContents;
	}

	public Set<String> getImages() {

		// temp is used to eliminate duplicate links
		HashSet<String> temp = new HashSet<String>();
		temp.addAll(allImages);
		return temp;
	}

	/**
	 * Gets all child images from the Web Page
	 * 
	 * @return - allLinks (excluding non-child images)
	 */
	public Set<String> getChildImages() {

		// temp is used to eliminate duplicate links
		HashSet<String> temp = new HashSet<String>();

		// Ensure a link is a child link, then add it to temp
		for (String u : allImages) {
			try {
				if (new URL(u).getHost().equals(parentURL.getHost())) {
					temp.add(u);
				}
			} catch (MalformedURLException e) {
				// This catch statement should never be reached because the URLs
				// were already confirmed, but if it is write to the error log
				log.error(e.getMessage());
			}
		}
		return temp;
	}

}
