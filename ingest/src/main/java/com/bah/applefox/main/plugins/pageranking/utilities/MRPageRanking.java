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
package com.bah.applefox.main.plugins.pageranking.utilities;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import com.bah.applefox.main.plugins.utilities.AccumuloUtils;
import com.bah.applefox.main.plugins.utilities.IngestUtils;

/**
 * This is the main worker class for computing page rank. The initalized tables
 * are read in, the page rank is computed for each link and placed in a new
 * table. The new table is necessary for accurate results. This is based on the
 * PageRank algorithm
 * 
 */
public class MRPageRanking extends Configured implements Tool {
	private static String tablePrefix;
	private static String outboundLinks;
	
	private static Log log = LogFactory.getLog(MRPageRanking.class);
	
	public static class MapperClass extends Mapper<Key, Value, Key, Value> {

		@Override
		public void map(Key key, Value value, Context context)
				throws IOException, InterruptedException {

			try {

				// Check for the link counts
				Scanner scan = AccumuloUtils.connectRead(outboundLinks);
				scan.setRange(new Range(key.getColumnFamily()));
				if (scan.iterator().hasNext()) {
					// Set val to outbound link counts
					Integer val = (Integer) IngestUtils.deserialize(scan
							.iterator().next().getValue().get());
					// Check for page rank of outbound link
					Scanner prScanner = AccumuloUtils.connectRead(tablePrefix
							+ "Old");
					prScanner.setRange(new Range(key.getColumnFamily()));
					if (prScanner.iterator().hasNext()) {
						// The new page rank 
						Double pr = (Double) IngestUtils.deserialize(prScanner
								.iterator().next().getValue().get());
						context.write(new Key(key.getRow()), new Value(
								IngestUtils.serialize(new Double(pr / val))));
					}
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
			} catch (ClassNotFoundException e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			}

		}
	}

	public static class ReducerClass extends
			Reducer<Key, Value, Text, Mutation> {

		public void reduce(Key key, Iterable<Value> values, Context context) {
			Mutation m = new Mutation(key.getRow());

			try {
				Double total = 0.0;

				Scanner scan = AccumuloUtils.connectRead(tablePrefix + "New");
				scan.setRange(new Range(key.getRow()));
				if (scan.iterator().hasNext()) {
					total = (Double) IngestUtils.deserialize(scan.iterator()
							.next().getValue().get());
				} else {
					total = 0.0;
				}

				Iterator<Value> itr = values.iterator();

				while (itr.hasNext()) {
					Double add = (Double) IngestUtils.deserialize(itr.next()
							.get());
					total = total + add;
				}

				m.put("0", "0", new Value(IngestUtils.serialize(total)));
				context.write(new Text(tablePrefix + "New"), m);

			} catch (IOException e) {
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
			} catch (InterruptedException e) {
				if (e.getMessage() != null) {
					log.error(e.getMessage());
				} else {
					log.error(e.getStackTrace());
				}
			}

		}
	}

	public int run(String[] args) throws Exception {

		String jobName = this.getClass().getSimpleName() + "_"
				+ System.currentTimeMillis();

		Job job = new Job(getConf(), jobName);
		job.setJarByClass(this.getClass());

		tablePrefix = args[13];
		outboundLinks = args[15];

		job.setInputFormatClass(AccumuloInputFormat.class);
		AccumuloInputFormat.setZooKeeperInstance(job.getConfiguration(),
				args[0], args[1]);

		AccumuloInputFormat.setInputInfo(job.getConfiguration(), args[2],
				args[3].getBytes(), args[12] + "To", new Authorizations());

		job.setMapperClass(MapperClass.class);
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);

		job.setReducerClass(ReducerClass.class);
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Key.class);
		job.setOutputValueClass(Value.class);
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(),
				args[0], args[1]);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), args[2],
				args[3].getBytes(), true, tablePrefix + "New");

		job.waitForCompletion(true);

		return job.isSuccessful() ? 0 : 1;
	}
}
