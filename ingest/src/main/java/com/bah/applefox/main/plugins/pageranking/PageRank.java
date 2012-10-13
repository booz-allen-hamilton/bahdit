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
package com.bah.applefox.main.plugins.pageranking;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

import com.bah.applefox.main.plugins.pageranking.utilities.CountURLs;
import com.bah.applefox.main.plugins.pageranking.utilities.DampenTable;
import com.bah.applefox.main.plugins.pageranking.utilities.InitializePRTables;
import com.bah.applefox.main.plugins.pageranking.utilities.PRtoFile;
import com.bah.applefox.main.plugins.pageranking.utilities.MRPageRanking;
import com.bah.applefox.main.plugins.utilities.AccumuloUtils;

/**
 * This class contains one method, the method that initializes the tables
 * necessary for computing page rank, then iterates the given number of times to
 * calculate the ranks to varying degrees of precision.
 * 
 * 
 */
public class PageRank {

	private static Log log = LogFactory.getLog(PageRank.class);

	/**
	 * This method controls everything necessary to calculate page rank and
	 * store it to a file
	 * 
	 * @param args
	 *            - the string values to pass in
	 * @param iterations
	 *            - the number of iterations to run
	 * @param urlSplit
	 *            - the size of the split for all page rank tables
	 * @return - whether or not the page rank was successfully calculated
	 */
	public static boolean createPageRank(String[] args, int iterations,
			String urlSplit) {
		try {

			AccumuloUtils.setSplitSize(urlSplit);

			ToolRunner.run(new CountURLs(), args);

			ToolRunner.run(new InitializePRTables(), args);
			for (int i = 0; i < iterations; i++) {

				Instance inst = new ZooKeeperInstance(args[0], args[1]);
				Connector conn = inst.getConnector(args[2], args[3].getBytes());

				AccumuloUtils.connectBatchWrite(args[13] + "New");

				ToolRunner.run(new MRPageRanking(), args);
				ToolRunner.run(new DampenTable(), args);

				conn.tableOperations().delete(args[13] + "Old");
				conn.tableOperations().rename(args[13] + "New",
						args[13] + "Old");

			}

			if (!PRtoFile.writeToFile(args)) {
				return false;
			}

		} catch (Exception e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
			return false;
		}
		return true;
	}
}
