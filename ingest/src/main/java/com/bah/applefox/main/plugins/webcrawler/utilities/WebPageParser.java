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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bah.applefox.main.plugins.utilities.DivsFilter;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * The WebPageParser class contains the tools needed to extract URL links from a
 * Web Page, along with getting body contents and getting only child URLs
 * 
 */

public class WebPageParser {

	private static Log log = LogFactory.getLog(WebPageParser.class);

	// All links in xml/html start with this
	private final String LINK_START = "href=";

	// All images start with this
	private final String IMAGE_START = "img src=";

	// The parent URL of the page
	private URL parentURL;

	// The UserAgent field of the server request property
	private String UserAgent;

	// The title of the web page;
	private String title = "";

	// The encoded contents of the page
	private String pageContents = "";

	// Boolean signifying whether or not the page has been parsed;
	private boolean isParsed = false;

	// ArrayList of all links found on the page, in string format
	private ArrayList<String> allLinks = new ArrayList<String>();

	// ArrayList of all image locations found on the page, in string format
	private ArrayList<String> allImages = new ArrayList<String>();

	// ArrayList of all directories that robots are not allowed to access (as
	// specified in robots.txt)
	private ArrayList<URL> disallows = new ArrayList<URL>();

	/**
	 * Constructor for the WebPageParser object
	 * 
	 * @param parentURL
	 *            - the URL of the page to be parsed
	 * @param UserAgent
	 *            - the UserAgent of the server request property
	 */
	public WebPageParser(String parentURL, String UserAgent) {

		// Gives value to the attributes

		// Try to create the URL value of parentURL
		// If this fails, write to the error log if possible
		try {
			this.parentURL = new URL(parentURL);
		} catch (MalformedURLException e) {
			log.error(e.getMessage());
			this.parentURL = null;
		}

		this.UserAgent = UserAgent;

	}

	/**
	 * Parses the information out of the Web Page, may be called before any of
	 * the get functions to improve speed of getLinks and getChildLinks
	 * functions
	 */
	public void parse() {

		// Sets the page contents for later use

		setPageContents();

		// If page contents were successfully set, check for allowed links
		if (pageContents.length() > 0) {
			// Sets the links that this robot is not allowed to visit
			setDisallows();
			setLinks(pageContents);
			setTitle(pageContents);
			setImages(pageContents);
			// If either of the above calls throws an error, it is due to a flaw
			// in the errorLog
		}

		// Add extra setter functions here

		isParsed = true;
	}

	public void parseWithoutDivs(String divsFile) {

		// Sets the page contents for later use

		setPageContents();

		// Remove the div tags
		pageContents = DivsFilter.filterDivs(pageContents, getExDivs(divsFile));

		// If page contents were successfully set, check for allowed links
		if (pageContents.length() > 0) {
			// Sets the links that this robot is not allowed to visit
			setDisallows();
			setLinks(pageContents);
			setTitle(pageContents);
			setImages(pageContents);
			// If either of the above calls throws an error, it is due to a flaw
			// in the errorLog
		}

		// Add extra setter functions here

		isParsed = true;
	}

	/**
	 * Private function to set the disallowed paths as per the robots.txt file
	 * 
	 */
	private void setDisallows() {
		// Use ParseRobotsTXT to get all disallowed links for a given parentURL
		// and UserAgent
		ParseRobotsTXT parser;
		try {
			parser = new ParseRobotsTXT(parentURL, UserAgent);
			disallows.addAll(parser.getDisallows());
		} catch (MalformedURLException e) {
			log.error(e.getMessage());
		} catch (IOException e) {
			// If the robots.txt file was blank, or no URLs could be found,
			// assume there is not active robots.txt file and write to the error
			// log
			log.error(e.getMessage());
		}
	}

	private void setTitle(String page) {
		// Find the title tag
		int index = page.toLowerCase().indexOf("<title>");
		if (index != -1) {
			// If the opening title tag exists, find the closing tag
			String tempTitle = page.substring(index + 7);
			index = tempTitle.toLowerCase().indexOf("</title>");
			if (index != -1) {
				// Set the title
				title = tempTitle.substring(0, index);
			}
		} else if (parentURL.toString().toLowerCase().endsWith(".pdf")) {
			index = parentURL.toString().lastIndexOf("/");
			if (index != -1) {
				title = parentURL.toString().substring(index + 1);
				title = title.substring(0, title.length() - 4);
				title = title.replaceAll("_", " ");
			}
		}
	}

	/**
	 * Returns whether or not the Web Page has been parsed
	 * 
	 * @return - isParsed
	 */
	public boolean isParsed() {
		return isParsed;
	}

	/**
	 * Returns the Parent URL as constructed
	 * 
	 * @return - parentURL
	 */
	public URL getParent() {
		return parentURL;
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
	public HashSet<String> getLinks() {
		// If the Web Page has not been parsed, parse it
		if (isParsed = false) {
			this.parse();
		}

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
	public HashSet<String> getChildLinks() {
		// If the Web Page has not been parsed, parse it

		if (isParsed == false) {
			this.parse();

		}

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

	public String getBody() {
		// If the Web Page has not been parsed, parse it

		if (isParsed == false) {
			this.parse();

		}

		return pageContents;
	}

	/**
	 * Private function to set the contents of the page [called in parse()]
	 * 
	 */
	private void setPageContents() {

		try {

			// Open the URL Connection and assign it the request property fields
			// of "User-Agent" and UserAgent's value
			URLConnection con = parentURL.openConnection();
			con.setRequestProperty("User-Agent", UserAgent);

			// Get the file path, and eliminate unreadable documents
			String filePath = parentURL.toString();

			// Reads content only if it is a valid format

			if (!(filePath.endsWith(".pdf") || filePath.endsWith(".doc")
					|| filePath.endsWith(".jsp") || filePath.endsWith("rss") || filePath
						.endsWith(".css"))) {
				// Sets the connection timeout (in milliseconds)
				con.setConnectTimeout(1000);

				// Tries to match the character set of the Web Page
				String charset = "utf-8";
				try {
					Matcher m = Pattern.compile("\\s+charset=([^\\s]+)\\s*")
							.matcher(con.getContentType());
					charset = m.matches() ? m.group(1) : "utf-8";
				} catch (Exception e) {
					log.error("Page had no specified charset");
				}

				// Reader derived from the URL Connection's input stream, with
				// the
				// given character set
				Reader r = new InputStreamReader(con.getInputStream(), charset);

				// String Buffer used to append each chunk of Web Page data
				StringBuffer buf = new StringBuffer();

				// Tries to get an estimate of bytes available
				int BUFFER_SIZE = con.getInputStream().available();

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

					// Appends the data chunk to the string buffer and gets the
					// next chunk
					buf.append(ch, 0, len);
					len = r.read(ch, 0, BUFFER_SIZE);
				}

				// Sets the pageContents to the newly created string
				// Remove comments
				pageContents = buf.toString().replaceAll("<!--.*-->", "");
			}
		} catch (UnsupportedEncodingException e) {
			log.error(e.getMessage());

			// Assume the body contents are blank if the character encoding is
			// not supported
			pageContents = "";
		} catch (IOException e) {
			log.error(e.getMessage());

			// Assume the body contents are blank if the Web Page could not be
			// accessed
			pageContents = "";
		}

	}

	/**
	 * Private function to set all allowed links on a page
	 * 
	 * @param pageCode
	 *            - the source code of the page
	 * @throws IOException
	 *             - thrown if there is an error writing to the errorLog
	 */
	private void setLinks(String pageCode) {

		// An ArrayList to store possible URLs
		ArrayList<String> parsedParts = new ArrayList<String>();

		// An array of all string sections that start with the given constant
		String[] parts = pageCode.split(LINK_START);

		// A temporary string to signify a URL
		String url;

		parts[0] = "";

		for (String s : parts) {
			if (s.length() > 0) {
				s = s.substring(1);
			}
			// Matching criteria for a hyperlink's end quote
			if (s.indexOf("\"") != -1) {

				// Get useful data of the potential url
				url = s.substring(0, s.indexOf("\""));

				// Checks for absolute and relative urls
				String leftHalf = parentURL.toString();
				String wholeLink = "temp";

				if (url.startsWith("http:") || url.startsWith("https:")) {
					wholeLink = url;
				} else if (url.startsWith("../")) {
					if (leftHalf.endsWith("/")) {
						leftHalf = leftHalf.substring(0, leftHalf.length() - 1);
					}
					while (url.startsWith("../")) {
						url = url.substring(3);
						leftHalf = leftHalf.substring(0,
								leftHalf.lastIndexOf("/"));
					}
					wholeLink = leftHalf + "/" + url;
				} else {
					wholeLink = formAbsoluteURLfromRelative(url);
				}

				try {
					parsedParts.add(new URL(wholeLink).toString());
				} catch (MalformedURLException e) {
					// If the given string is not a URL, there could be an issue
					// with the URL formatting, so write to error log
					log.error(e.getMessage());

				}

			}
		}

		// Go through all found links
		for (String urlLink : parsedParts) {
			allLinks.add(urlLink);

			String temp = urlLink.toLowerCase();

			for (URL tempDis : disallows) {
				if (temp.contains(tempDis.getPath().toLowerCase())) {
					// If this is not a permited link, remove it
					allLinks.remove(urlLink);
				}
			}

			// If the link is only a page attribute, ignore it
			if (urlLink.contains("#")) {
				allLinks.remove(urlLink);
			}

			// If the URL is a variant of a the original page with javascript,
			// ignore it.
			if (temp.contains("javascript") && temp.contains("(")
					&& temp.contains(")")) {
				allLinks.remove(urlLink);
			}

			// If the URL is an email address, ignore it
			if (temp.contains("mailto:")) {
				allLinks.remove(urlLink);
			}

			// If the URL is a duplicate of document type, ignore it
			if (temp.contains("<!")) {
				allLinks.remove(urlLink);
			}

			// If the URL is not a valid file type, remove it
			if (temp.endsWith(".css") || temp.endsWith(".jpg")
					|| temp.endsWith(".gif") || temp.endsWith(".ico")) {
				allLinks.remove(urlLink);
			}

			// If the URL is a redirect, remove it
			String urlPath = null;
			try {
				urlPath = new URL(urlLink).getPath()
						+ new URL(urlLink).getQuery();
			} catch (MalformedURLException e) {
				log.error(e.getStackTrace());
			}
			urlPath = urlPath.toLowerCase();
			if (urlPath.contains(parentURL.getHost().toLowerCase()
					.substring(parentURL.getHost().indexOf(".")))) {
				allLinks.remove(urlLink);
			}

		}
	}

	/**
	 * Private Method used to turn an relative URL into an absolute URL
	 * 
	 * @param relativeURL
	 *            - the relative URL found
	 * @return - the method's attempt at forming an absolute URL
	 */
	private String formAbsoluteURLfromRelative(String relativeURL) {
		String Absolute = "";
		String parentString = parentURL.getProtocol() + "://"
				+ parentURL.getHost() + parentURL.getPath();

		if (!relativeURL.startsWith("/")) {
			if (parentString.endsWith("/")) {
				Absolute = parentString + relativeURL;
			} else {
				parentString = parentString.substring(0,
						parentString.lastIndexOf("/") + 1);
				Absolute = parentString + relativeURL;
			}
		} else {
			Absolute = parentURL.getProtocol() + "://" + parentURL.getHost()
					+ relativeURL;
		}

		return Absolute;
	}

	/**
	 * Private function to set all images on a page
	 * 
	 * @param pageCode
	 *            - the source code of the page
	 */
	private void setImages(String pageCode) {

		// An ArrayList to store possible URLs
		ArrayList<String> parsedParts = new ArrayList<String>();

		// An array of all string sections that start with the given constant
		String[] parts = pageCode.split(IMAGE_START);

		// A temporary string to signify a URL
		String url;

		for (String s : parts) {
			s = s.substring(1);
			// Matching criteria for an image tag's end quote
			if (s.indexOf("\"") != -1) {

				// Get useful data of the potential url
				url = s.substring(0, s.indexOf("\""));
				String alt = "";
				try {
					int altLocation = s.indexOf("alt=");
					if (altLocation != -1) {
						String tempAlt = s.substring(altLocation + 5);
						if (tempAlt.indexOf("\"") != -1) {
							alt = tempAlt.substring(0, tempAlt.indexOf("\""));
						}
					}
				} catch (Exception e) {
					log.equals("Failure to get image alt text");
				}

				String id = "";
				try {
					int idLocation = s.indexOf("id=");
					if (idLocation != -1) {
						String tempID = s.substring(idLocation + 4);
						if (tempID.indexOf("\"") != -1) {
							id = tempID.substring(0, tempID.indexOf("\""));
						}
					}
				} catch (Exception e) {
					log.equals("Failure to get image id");
				}

				String title = "";
				try {
					int titleLocation = s.indexOf("title=");
					if (titleLocation != -1) {
						String tempTitle = s.substring(titleLocation + 7);
						if (tempTitle.indexOf("\"") != -1) {
							id = tempTitle
									.substring(0, tempTitle.indexOf("\""));
						}
					}
				} catch (Exception e) {
					log.equals("Failure to get image title");
				}

				// Checks for absolute and relative urls
				String leftHalf = parentURL.toString();
				String wholeLink = "temp";

				if (url.startsWith("http:") || url.startsWith("https:")) {
					wholeLink = url;
				} else if (url.startsWith("../")) {
					if (leftHalf.endsWith("/")) {
						leftHalf = leftHalf.substring(0, leftHalf.length() - 1);
					}
					while (url.startsWith("../")) {
						url = url.substring(3);
						leftHalf = leftHalf.substring(0,
								leftHalf.lastIndexOf("/"));
					}
					wholeLink = leftHalf + "/" + url;
				} else {
					wholeLink = formAbsoluteURLfromRelative(url);
				}

				String name = "";
				int nameIndex = url.lastIndexOf("/");
				if (nameIndex != -1) {
					name = url.substring(nameIndex + 1);
					nameIndex = name.lastIndexOf(".");
					if (nameIndex != -1) {
						// Replace all of the replacements for space in a URL
						name = name.substring(0, nameIndex);
						name = name.replaceAll("_", " ");
						name = name.replaceAll("\\+", " ");
						name = name.replaceAll("%20", " ");
						name = name.replaceAll("-", " ");
					}

				}

				try {
					parsedParts.add((new URL(wholeLink).toString() + " " + alt
							+ " " + title + " " + id + " " + name).trim());
					System.out.println(parsedParts.get(parsedParts.size() - 1));
				} catch (MalformedURLException e) {
					// If the given string is not a URL, there could be an issue
					// with the URL formatting, so write to error log
					log.error(e.getMessage());

				}

			}
		}

		// Go through all found links
		for (String urlLink : parsedParts) {
			allImages.add(urlLink);

			String temp = urlLink.toLowerCase();
			int index = temp.indexOf(" ");
			if (index != -1) {
				temp = temp.substring(0, temp.indexOf(" "));
			}

			// If the link is only a page attribute, ignore it
			if (urlLink.contains("#")) {
				allImages.remove(urlLink);
			}

			// If the URL is a variant of a the original page with javascript,
			// ignore it.
			if (temp.contains("javascript") && temp.contains("(")
					&& temp.contains(")")) {
				allImages.remove(urlLink);
			}

			// If the URL is an email address, ignore it
			if (temp.contains("mailto:")) {
				allImages.remove(urlLink);
			}

			// If the URL is a duplicate of document type, ignore it
			if (temp.contains("<!")) {
				allImages.remove(urlLink);
			}

			// If the URL is not a valid file type, remove it
			if (!(temp.endsWith(".jpg") || temp.endsWith(".jpeg")
					|| temp.endsWith(".jpe") || temp.endsWith(".jfif")
					|| temp.endsWith(".jfi") || temp.endsWith(".jif")
					|| temp.endsWith(".png") || temp.endsWith(".gif")
					|| temp.endsWith(".dib") || temp.endsWith(".bmp") || temp
						.endsWith(".wbmp"))) {
				allImages.remove(urlLink);
			}

			// If the URL is a redirect, remove it
			String urlPath = null;
			try {
				urlPath = new URL(urlLink).getPath()
						+ new URL(urlLink).getQuery();
				urlPath = urlPath.toLowerCase();
				if (urlPath.contains(parentURL.getHost().toLowerCase()
						.substring(parentURL.getHost().indexOf(".")))) {
					allImages.remove(urlLink);
				}
			} catch (MalformedURLException e) {
				log.error(e.getStackTrace());
			}
		}
	}

	public HashSet<String> getImages() {
		// If the Web Page has not been parsed, parse it
		if (isParsed = false) {
			this.parse();
		}

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
	public HashSet<String> getChildImages() {
		// If the Web Page has not been parsed, parse it

		if (isParsed == false) {
			this.parse();

		}

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

	/**
	 * Gets the ids of the div tags that should be excluded from the data on the
	 * page. This is useful for pages with repetitive generic headers and
	 * footers, allowing for more accurate results
	 * 
	 * @return - HashSet of ids
	 */
	private HashSet<String> getExDivs(String divsFile) {
		HashSet<String> divs = new HashSet<String>();
		System.out.println("Getting stop divs");
		try {
			// Read in the file
			BufferedReader reader = new BufferedReader(new FileReader(new File(
					divsFile)));

			// Temporary variable for the ids
			String temp;

			// Set temp to the ids
			while ((temp = reader.readLine()) != null) {
				// Add temp to divs
				divs.add(temp);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
		} catch (IOException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
		}
		return divs;
	}

}
