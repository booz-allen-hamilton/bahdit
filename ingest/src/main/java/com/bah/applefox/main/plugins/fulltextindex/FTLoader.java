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
package com.bah.applefox.main.plugins.fulltextindex;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.bah.applefox.main.Loader;
import com.bah.applefox.main.plugins.utilities.AccumuloUtils;
import com.bah.applefox.main.plugins.utilities.DivsFilter;
import com.bah.applefox.main.plugins.utilities.IngestUtils;
import com.bah.applefox.main.plugins.webcrawler.utilities.PageCrawlException;
import com.bah.applefox.main.plugins.webcrawler.utilities.WebPageCrawl;

/**
 * A MapReduce job that loads the NGrams from the pages indicated by the URLs in
 * the URLs table into the Data Table. The data contained follows the format:
 * Row ID: word + timestamp | Column Family: parent URL | Column Qualifier:
 * another word on the page | timestamp | Value: number of times the word occurs
 * on the page
 * 
 */
public class FTLoader extends Loader {
	private static String dTable, urlCheckedTable, articleFile, divsFile;
	private static int maxNGrams;
	private static long longSuffix;
	private static HashSet<String> stopWords;
	private static Log log = LogFactory.getLog(FTLoader.class);
	private static HashSet<String> exDivs;

	/**
	 * MapperClass extends the Mapper class. It performs the map functionality
	 * of MapReduce.
	 * 
	 */
	public static class MapperClass extends Mapper<Key, Value, Key, Value> {
		/**
		 * Gets a URL from the URLs table in Accumulo, feeds that data into
		 * addToDataBaseTable
		 * 
		 */
		@Override
		public void map(Key key, Value value, Context context) {

			// Get the row of the key (the url) and append it to text
			Text currentURL = new Text();
			ByteSequence row = key.getRowData();
			currentURL
					.append(row.getBackingArray(), row.offset(), row.length());

			Value val = new Value("0".getBytes());

			try {
				// scan the table to ensure the url has not been checked
				Scanner scan = AccumuloUtils.connectRead(urlCheckedTable);
				scan.setRange(new Range(currentURL.toString()));

				if (!scan.iterator().hasNext()) {
					// If yet unchecked, check the url by passing it to the
					// reduce job
					context.write(new Key(currentURL.toString()), val);
				}

			} catch (NullPointerException e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			} catch (AccumuloException e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			} catch (AccumuloSecurityException e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			} catch (TableNotFoundException e) {
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
			} catch (InterruptedException e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			}
		}
	}

	/**
	 * ReducerClass extends Reducer and would perform the Reduce functionality
	 * of MapReduce, but in this case it is only a place holder.
	 * 
	 */
	public static class ReducerClass extends Reducer<Key, Value, Key, Value> {
		public void reduce(Key key, Iterable<Value> values, Context context)
				throws IOException, InterruptedException {

			try {
				// Add a consistent suffix to data in the table to ensure even
				// splits
				longSuffix = new Date().getTime();

				// Add the data to the table with this method
				if (addToDataBaseTable(key.getRow().toString())) {

					// Write off the url as having been checked
					BatchWriter w = AccumuloUtils
							.connectBatchWrite(urlCheckedTable);
					Mutation m = new Mutation(key.getRow().toString());
					m.put("0", "0", new Value("0".getBytes()));
					w.addMutation(m);
					w.close();
				}
			} catch (AccumuloException e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			} catch (AccumuloSecurityException e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			} catch (TableNotFoundException e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			} catch (TableExistsException e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			} catch (Exception e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			}
		}

	}

	/**
	 * run takes the comandline args as arguments (in this case from a
	 * configuration file), creates a new job, configures it, initiates it,
	 * waits for completion, and returns 0 if it is successful (1 if it is not)
	 * 
	 * @param args
	 *            the commandline arguments (in this case from a configuration
	 *            file)
	 * 
	 * @return 0 if the job ran successfully and 1 if it isn't
	 */
	public int run(String[] args) throws Exception {
		try {
			// Initialize variables
			FTLoader.articleFile = args[8];
			FTLoader.maxNGrams = Integer.parseInt(args[9]);
			FTLoader.stopWords = getStopWords();
			FTLoader.dTable = args[10];
			FTLoader.urlCheckedTable = args[11];
			FTLoader.divsFile = args[20];
			FTLoader.exDivs = getExDivs();

			// Give the job a name
			String jobName = this.getClass().getSimpleName() + "_"
					+ System.currentTimeMillis();

			// Create job and set the jar
			Job job = new Job(getConf(), jobName);
			job.setJarByClass(this.getClass());

			String urlTable = args[5];

			job.setInputFormatClass(AccumuloInputFormat.class);
			InputFormatBase.setZooKeeperInstance(job.getConfiguration(),
					args[0], args[1]);
			InputFormatBase.setInputInfo(job.getConfiguration(), args[2],
					args[3].getBytes(), urlTable, new Authorizations());

			job.setMapperClass(MapperClass.class);
			job.setMapOutputKeyClass(Key.class);
			job.setMapOutputValueClass(Value.class);

			job.setReducerClass(ReducerClass.class);
			job.setNumReduceTasks(Integer.parseInt(args[4]));

			job.setOutputFormatClass(AccumuloOutputFormat.class);
			job.setOutputKeyClass(Key.class);
			job.setOutputValueClass(Value.class);

			AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(),
					args[0], args[1]);
			AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), args[2],
					args[3].getBytes(), true, urlTable);

			job.waitForCompletion(true);

			return job.isSuccessful() ? 0 : 1;
		} catch (IOException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
		} catch (InterruptedException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
		} catch (ClassNotFoundException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
		}
		return 1;
	}

	/**
	 * This method is used to add all information parsed by tika into the
	 * Accumulo table
	 * 
	 * @param url
	 *            - the URL of the page that has been parsed
	 * @param tikaParsed
	 *            - all of the engrams from the page
	 * @throws TikaException
	 * @throws SAXException
	 */
	private static boolean addToDataBaseTable(String url) {
		try {
			// Connect to the data table
			BatchWriter writer = AccumuloUtils.connectBatchWrite(dTable);

			// Let the user know the url is being added
			System.out.println("Adding " + url + " with prefix " + longSuffix);

			// Get the input stream (in case it is not an html document
			InputStream urlInput = new URL(url).openStream();

			// Set the page contents (used for filtering if it is an html
			// document)
			String pageContents = getPageContents(new URL(url));

			// If the document is HTML
			if (exDivs.size() != 0
					&& pageContents.toLowerCase().contains("<html>")) {
				// Filter out some divs (especially generic headers/footers,
				// etc.)
				pageContents = DivsFilter.filterDivs(pageContents, exDivs);
				urlInput = new ByteArrayInputStream(pageContents.getBytes());
			}

			// Parse with tika
			Parser parser = new AutoDetectParser();
			Metadata metadata = new Metadata();
			ParseContext context = new ParseContext();
			ContentHandler handler = new BodyContentHandler();

			parser.parse(urlInput, handler, metadata, context);

			// Get the keywords of the page and its title
			String keywords = metadata.get("keywords");
			String title = metadata.get("title");
			if (title == null) {
				WebPageCrawl p;
				try {
					p = new WebPageCrawl(url, "", Collections.<String>emptySet());
				} catch (PageCrawlException e) {
					log.info(e);
					return false;
				}
				title = p.getTitle();
			}

			// If there are keywords, delimit the commas, otherwise make it a
			// blank screen (not null)
			if (keywords != null) {
				keywords = keywords.replaceAll(",", "[ ]");
			} else {
				keywords = "";
			}

			// Make everything lower case for ease of search
			String plainText = handler.toString().toLowerCase();

			// Split it into <Key,Value> pairs of NGrams, with the Value being
			// the count of the NGram on the page
			HashMap<String, Integer> tikaParsed = IngestUtils
					.collectTerms(IngestUtils
							.createNGrams(plainText, maxNGrams));

			// A counter for the final number of words
			Integer totalWords = 0;

			// A HashMap for the final NGrams
			HashMap<String, Integer> finalParsed = new HashMap<String, Integer>();

			for (String i : tikaParsed.keySet()) {
				int freq = tikaParsed.get(i);
				totalWords += freq;
				// erase stop words
				if (stopWords != null && !stopWords.contains(i)) {
					finalParsed.put(i, tikaParsed.get(i));
				} else if (stopWords == null) {
					finalParsed.put(i, tikaParsed.get(i));
				}
			}

			System.out.println("Tika Parsed: " + finalParsed.keySet().size());
			System.out.println("Starting");
			int counter = 0;

			String namedURL = url + "[ ]" + title + "[ ]" + keywords;

			for (String row : finalParsed.keySet()) {
				row = row + " " + longSuffix;
				for (String CQ : finalParsed.keySet()) {
					String groupedVal = new String();
					Integer wc = finalParsed.get(CQ);
					double freq = wc.doubleValue() / totalWords.doubleValue();
					groupedVal = wc + "," + freq;
					Value val = new Value(IngestUtils.serialize(groupedVal));

					Mutation m = new Mutation(row);
					m.put(namedURL, CQ, new Date().getTime(), val);
					writer.addMutation(m);
					counter++;
				}

			}

			System.out.println("Wrote " + counter
					+ " Key-Value pairs to Accumulo.");

			writer.close();
			System.out.println("Stopped writing");
		} catch (AccumuloException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
			return false;
		} catch (AccumuloSecurityException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
			return false;
		} catch (TableNotFoundException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
			return false;
		} catch (TableExistsException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
			return false;
		} catch (MalformedURLException e) {
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
			return false;
		} catch (SAXException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
			return false;
		} catch (TikaException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
			return false;
		}
		return true;
	}

	/**
	 * Gets the words that are supposed to be removed from the article file
	 * (Words such as the, a, an, etc. that are unimportant to the search
	 * engine)
	 * 
	 * 
	 * @return articles - All the words that shouldn't be included in the data
	 * @throws IOException
	 */
	private HashSet<String> getStopWords() {
		HashSet<String> articles = new HashSet<String>();
		System.out.println("getting stop words");
		try {
			// Read the file
			BufferedReader reader = new BufferedReader(new FileReader(new File(
					articleFile)));

			// String to temporarily store each word
			String temp;

			// Add each word to temp
			while ((temp = reader.readLine()) != null) {
				// Add temp to articles
				articles.add(temp);
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
		return articles;
	}

	/**
	 * Gets the ids of the div tags that should be excluded from the data on the
	 * page. This is useful for pages with repetitive generic headers and
	 * footers, allowing for more accurate results
	 * 
	 * @return - HashSet of ids
	 */
	private HashSet<String> getExDivs() {
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

	/** This method is used to get the page source from the given URL
	 * @param url - the url from which to get the contents
	 * @return - the page contents
	 */
	private static String getPageContents(URL url) {

		String pageContents = "";
		try {

			// Open the URL Connection
			URLConnection con = url.openConnection();

			// Get the file path, and eliminate unreadable documents
			String filePath = url.toString();

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
				pageContents = buf.toString();
			}
		} catch (UnsupportedEncodingException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}

			// Assume the body contents are blank if the character encoding is
			// not supported
			pageContents = "";
		} catch (IOException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}

			// Assume the body contents are blank if the Web Page could not be
			// accessed
			pageContents = "";
		}

		return pageContents;
	}
}
