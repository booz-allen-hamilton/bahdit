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
package com.bah.applefox.main.plugins.utilities;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.security.Authorizations;

/**
 * 
 * A simple Library used for testing and as a help to connect to testing
 * accumulo tables
 * 
 */
public class AccumuloUtils {
	private static String ZOOSERVERS = "localhost";
	private static String INSTANCENAME = "bah";
	private static String USER = "root";
	private static String PASSW = "bah";
	private static String SPLIT_SIZE = "1G";

	public static Scanner connectRead(String TableName)
			throws AccumuloException, AccumuloSecurityException,
			TableNotFoundException {
		Instance inst = new ZooKeeperInstance(INSTANCENAME, ZOOSERVERS);
		Connector conn = inst.getConnector(USER, PASSW);
		String table = TableName;

		Scanner scanner = conn.createScanner(table, new Authorizations());

		return scanner;
	}

	public static BatchWriter connectBatchWrite(String TableName)
			throws AccumuloException, AccumuloSecurityException,
			TableNotFoundException, TableExistsException {
		Instance inst = new ZooKeeperInstance(INSTANCENAME, ZOOSERVERS);
		Connector conn = inst.getConnector(USER, PASSW);
		String table = TableName;

		if (!conn.tableOperations().exists(table)) {
			conn.tableOperations().create(table);
			conn.tableOperations().setProperty(table, "table.split.threshold",
					SPLIT_SIZE);
			Iterable<Entry<String, String>> temp = conn.tableOperations()
					.getProperties(table);
			Iterator<Entry<String, String>> itr = temp.iterator();
			String splitSize = "";
			while (itr.hasNext()) {
				Entry<String, String> val = itr.next();
				if (val.getKey().equals("table.split.threshold")) {
					splitSize = val.getValue();
				}
			}
			System.out.println("Table split threshold: " + splitSize);
		}

		return conn.createBatchWriter(table, 1000000L, 1000L, 10);

	}

	public static void setZooserver(String zSer) {
		ZOOSERVERS = zSer;
	}

	public static void setInstanceName(String iName) {
		INSTANCENAME = iName;
	}

	public static void setInstancePassword(String iPass) {
		PASSW = iPass;
	}

	public static void setUser(String User) {
		USER = User;
	}

	public static void setSplitSize(String SplitSize) {
		SPLIT_SIZE = SplitSize;
	}
}
