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
package com.bah.applefox.main.plugins.webcrawler;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.bah.applefox.main.Ingester;
import com.bah.applefox.main.plugins.utilities.AccumuloUtils;
import com.bah.applefox.main.plugins.webcrawler.utilities.PageCrawlException;
import com.bah.applefox.main.plugins.webcrawler.utilities.WebPageCrawl;

/**
 * A MapReduce job that crawls the domains inidicated by the seed URLs and loads
 * all the URLs into the URLs Table in accumulo
 * 
 */
public class WebCrawler extends Ingester {
	private static String table, table2, table3, userAgent;
	private static final Log LOG = LogFactory.getFactory().getLog(
			WebCrawler.class);

	/**
	 * MapperClass extends the Mapper class. It performs the map functionality
	 * of MapReduce.
	 * 
	 */
	public static class MapperClass extends Mapper<Key, Value, Key, Value> {
		private Text currentURL = new Text();

		/**
		 * Finds a URL that has yet to be crawled for URLs in the URLs Table and
		 * crawls it for more unique URLs to add to the URLs table and adds
		 * them.
		 * 
		 */
		@Override
		public void map(Key key, Value value, Context context)
				throws IOException, InterruptedException {
			if (value.compareTo("0".getBytes()) == 0) {
				currentURL = new Text();
				context.getCounter(MATCH_COUNTER.URLS_PARSED).increment(1);
				System.out.println(context
						.getCounter(MATCH_COUNTER.URLS_PARSED).getValue());
				ByteSequence cf = key.getRowData();
				currentURL.append(cf.getBackingArray(), cf.offset(),
						cf.length());

				System.out.println("Parsing " + currentURL.toString());

				BatchWriter w;
				BatchWriter w2;
				BatchWriter w3;
				Value v = new Value();

				WebPageCrawl p;
				try {
					p = new WebPageCrawl(currentURL.toString(), userAgent, Collections.<String>emptySet());
				} catch (PageCrawlException e1) {
					LOG.info("Unable to crawl " + currentURL);
					return;
				}

				Set<String> links = p.getChildLinks();
				String title = p.getTitle();

				v.set(title.getBytes());
				Mutation m = new Mutation(currentURL.toString());
				m.put("0", "0", v);

				try {
					w = AccumuloUtils.connectBatchWrite(table);
					w.addMutation(m);
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}

				try {
					// String linkString = "1.0 ";
					// TODO: create an output format that is able to write to
					// multiple accumulo tables
					Scanner scan = AccumuloUtils.connectRead(table);
					w = AccumuloUtils.connectBatchWrite(table);
					w2 = AccumuloUtils.connectBatchWrite(table2);
					w3 = AccumuloUtils.connectBatchWrite(table3);
					v.set("0".getBytes());
					for (String link : links) {
						// linkString += link + ",";

						m = new Mutation(currentURL.toString());
						m.put(link, "0", v);
						w2.addMutation(m);
						w2.flush();

						m = new Mutation(link);
						m.put(currentURL.toString(), "0", v);
						w3.addMutation(m);
						w3.flush();

						scan.setRange(new Range(link));
						if (!scan.iterator().hasNext()) {
							if (link.endsWith("/")) {
								scan.setRange(new Range(link.subSequence(0,
										link.length() - 1)));
								if (!scan.iterator().hasNext()) {
									System.out.println("Adding " + link);
									m = new Mutation(link);
									m.put("0", "0", v);
									w.addMutation(m);
									w.flush();
								}
							} else {
								scan.setRange(new Range(link + "/"));
								if (!scan.iterator().hasNext()) {
									System.out.println("Adding " + link);
									m = new Mutation(link);
									m.put("0", "0", v);
									w.addMutation(m);
									w.flush();
								}
							}

						}
					}

					w.close();
					w2.close();
					w3.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static enum MATCH_COUNTER {
		URLS_PARSED
	};

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

		userAgent = args[6];

		String jobName = this.getClass().getSimpleName() + "_"
				+ System.currentTimeMillis();

		Job job = new Job(getConf(), jobName);
		job.setJarByClass(this.getClass());

		String clone = args[5];
		String clone2 = args[12];
		table = clone;

		AccumuloUtils.setSplitSize(args[24]);
		table2 = clone2 + "From";
		table3 = clone2 + "To";

		job.setInputFormatClass(AccumuloInputFormat.class);
		InputFormatBase.setZooKeeperInstance(job.getConfiguration(), args[0],
				args[1]);
		InputFormatBase.setInputInfo(job.getConfiguration(), args[2],
				args[3].getBytes(), clone, new Authorizations());

		job.setMapperClass(MapperClass.class);
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);

		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setOutputKeyClass(Key.class);
		job.setOutputValueClass(Value.class);
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(),
				args[0], args[1]);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), args[2],
				args[3].getBytes(), true, clone);

		job.waitForCompletion(true);

		return job.isSuccessful() ? 0 : 1;
	}
}
