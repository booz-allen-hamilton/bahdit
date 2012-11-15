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
package com.bah.bahdit.test;

import java.util.ArrayList;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * 
 * A simple Library used for testing and as a help to 
 * connect to testing accumulo tables
 * 
 */
public class TestUtils {
	public static final String TESTTABLE = "mainTable";
	public static final String ZOOSERVERS = "localhost";
	public static final String INSTANCENAME = "bah";
	public static final String USER = "root";
	public static final String PASSW = "bah";


	public static BatchWriter connectBatchWrite() 
	    throws AccumuloException, AccumuloSecurityException, 
	    TableNotFoundException, TableExistsException{
	  
		Instance inst = new ZooKeeperInstance(INSTANCENAME, ZOOSERVERS);
		Connector conn = inst.getConnector(USER, PASSW);
		String table = TESTTABLE;

		if(!conn.tableOperations().exists(table))
			conn.tableOperations().create(table);

		return conn.createBatchWriter(table, 1000000L, 1000L, 10);
	}

	public static void loadSampleTable() {
		try {
			ArrayList<String> terms = new ArrayList<String>();
			terms.add("term1");
			terms.add("term2");
			terms.add("term3");
			terms.add("term4");
			terms.add("term5");
			terms.add("term6");
			terms.add("term7");
			terms.add("term8");
			terms.add("term9");
			terms.add("term10");
			terms.add("term11");
			terms.add("term12");
			ArrayList<String> urls = new ArrayList<String>();
			urls.add("www.google.com");
			urls.add("www.google2.com");
			urls.add("www.google3.com");
			urls.add("www.google4.com");
			urls.add("www.google5.com");
			urls.add("www.google6.com");
			urls.add("www.google7.com");
			urls.add("www.google8.com");
			urls.add("www.google9.com");
			urls.add("www.google10.com");
			urls.add("www.google11.com");
			urls.add("www.google12.com");
			ArrayList<String> values = new ArrayList<String>();
			values.add("10,0.0");
			values.add("10,0.1");
			values.add("10,0.2");
			values.add("10,0.3");
			values.add("10,0.4");
			values.add("10,0.05");
			values.add("10,0.6");
			values.add("10,0.7");
			values.add("10,0.8");
			values.add("10,0.9");
			values.add("10,0.95");
			values.add("10,0.99");
			

			BatchWriter writer = connectBatchWrite();


			for(int i = 0; i < terms.size() - 1; ++i) {
				for(int j = 0; j < urls.size() - 1; ++j) {
					for(int k = 0; k < terms.size(); ++k) {
					  Text rowID = new Text(terms.get(i));
					  Text cf = new Text(urls.get(j));
					  Text cq = new Text(terms.get(k));
					  Value val = new Value(values.get(j).getBytes());
						Mutation m = new Mutation(rowID);
						m.put(cf, cq, val);
						writer.addMutation(m);
					}
				}
			}
			
			writer.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
