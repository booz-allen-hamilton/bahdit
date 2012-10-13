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

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
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
 * This class is used to initialize everything necessary for page rank, except
 * the URL counts. If the tables do not already exist, this class must be used
 * for page rank to work.
 * 
 */
public class InitializePRTables extends Configured implements Tool {

	private static String tablePrefix;
	private static Log log = LogFactory.getLog(InitializePRTables.class);
	
	public static class MapperClass extends Mapper<Key, Value, Key, Value> {

		@Override
		public void map(Key key, Value value, Context context)
				throws IOException, InterruptedException {
			// Write the row and column family as the <Key, Value> pair to
			// reduce
			context.write(new Key(key.getRow()), new Value(key
					.getColumnFamily().toString().getBytes()));
		}
	}

	public static class ReducerClass extends
			Reducer<Key, Value, Text, Mutation> {

		public void reduce(Key key, Iterable<Value> values, Context context) {
			try {
				// Initialize the value of initial page rank to 1
				Value v = new Value(IngestUtils.serialize(new Double(1.0)));
				Mutation m = new Mutation(key.getRow());
				m.put("0", "0", v);
				context.write(null, m);
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

	public int run(String[] args) throws Exception {

		tablePrefix = args[13];

		String jobName = this.getClass().getSimpleName() + "_"
				+ System.currentTimeMillis();

		Job job = new Job(getConf(), jobName);
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(AccumuloInputFormat.class);
		InputFormatBase.setZooKeeperInstance(job.getConfiguration(), args[0],
				args[1]);
		InputFormatBase.setInputInfo(job.getConfiguration(), args[2],
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
				args[3].getBytes(), true, tablePrefix + "Old");

		AccumuloUtils.connectBatchWrite(tablePrefix + "New");

		job.waitForCompletion(true);

		return job.isSuccessful() ? 0 : 1;
	}
}
