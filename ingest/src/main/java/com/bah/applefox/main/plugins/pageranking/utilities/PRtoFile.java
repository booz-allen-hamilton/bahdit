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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bah.applefox.main.plugins.utilities.AccumuloUtils;
import com.bah.applefox.main.plugins.utilities.IngestUtils;

/**
 * This class writes the finished page rank table to a file, used by the search
 * engine component. The table is written to a HashMap<String, Double> which is
 * then written as an object to a file.
 * 
 */
public class PRtoFile {

	private static Log log = LogFactory.getLog(PRtoFile.class);
	
	private static HashMap<String, Double> createMap(String[] args)
			throws AccumuloException, AccumuloSecurityException,
			TableNotFoundException {
		HashMap<String, Double> ret = new HashMap<String, Double>();
		String readTable = args[13] + "Old";

		Scanner scan = AccumuloUtils.connectRead(readTable);

		Iterator<Entry<Key, Value>> itr = scan.iterator();

		while (itr.hasNext()) {
			Entry<Key, Value> temp = itr.next();
			try {
				Double val = (Double) IngestUtils.deserialize(temp.getValue()
						.get());
				ret.put(temp.getKey().getRow().toString(), val);
				System.out.println("Adding to Map: "
						+ temp.getKey().getRow().toString() + " with rank: "
						+ val);
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
			}
		}

		Double max = 0.0;

		Collection<Double> values = ret.values();

		ArrayList<Double> tempValues = new ArrayList<Double>();
		tempValues.addAll(values);
		Collections.sort(tempValues);
		Collections.reverse(tempValues);

		max = tempValues.get(0);

		ret.put("[[MAX_PR]]", max);

		return ret;
	}

	public static boolean writeToFile(String[] args) {
		String fileName = args[16];
		File f = new File(fileName);
		try {
			f.createNewFile();
			OutputStream file = new FileOutputStream(f);
			OutputStream buffer = new BufferedOutputStream(file);
			ObjectOutput out = new ObjectOutputStream(buffer);
			out.writeObject(createMap(args));
			out.flush();
			out.close();
		} catch (IOException e) {
			if (e.getMessage() != null) {
				log.error(e.getMessage());
			} else {
				log.error(e.getStackTrace());
			}
			return false;
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
		}

		return true;

	}
}
