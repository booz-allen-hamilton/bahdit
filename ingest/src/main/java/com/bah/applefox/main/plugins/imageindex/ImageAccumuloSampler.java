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
package com.bah.applefox.main.plugins.imageindex;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.bah.applefox.main.plugins.utilities.AccumuloUtils;
import com.bah.applefox.main.plugins.utilities.IngestUtils;
import com.bah.applefox.main.plugins.utilities.SamplerCreator;
import com.bah.applefox.main.plugins.utilities.TotalDocFinder;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * This is the class used to create the sample table. The table consists of a
 * unique word as the Key of a HashMap and the number of occurrences as the
 * Value.  The HashMap<String, Integer> is then written to a file.
 * 
 */
public class ImageAccumuloSampler {

	// Global variables relevant to instance information
	private String sampleFile;
	private String dataTable;
	private String urlTable;

	// The error log
	private static Log log = LogFactory.getLog(ImageAccumuloSampler.class);

	/**
	 * Constructor method
	 * 
	 * @param sampleFile
	 *            - the location to write the sample table
	 * @param dataTable
	 *            - the name of the data table
	 * @param urlTable
	 *            - the name of the url table
	 */
	public ImageAccumuloSampler(String sampleFile, String dataTable,
			String urlTable) {
		// Initialize variables
		this.sampleFile = sampleFile;
		this.dataTable = dataTable;
		this.urlTable = urlTable;
	}

	/**
	 * Overridden method to create the sample
	 */
	public void createSample() {
		try {
			// HashMap to write the sample table to
			HashMap<String, Integer> output = new HashMap<String, Integer>();

			// Scan the data table
			Scanner scan = AccumuloUtils.connectRead(dataTable);

			Map<String, String> properties = new HashMap<String, String>();
			IteratorSetting cfg2 = new IteratorSetting(11,
					SamplerCreator.class, properties);
			scan.addScanIterator(cfg2);

			for (Entry<Key, Value> entry : scan) {
				try {
					// Write the data from the table to the sample table
					String row = entry.getKey().getRow().toString();
					int value = output.containsKey(row) ? output.get(row) : 0;
					value += 1;
					output.put(row, value);
				} catch (Exception e) {
					if (e.getMessage() != null) {
						log.error(e.getMessage());
					} else {
						log.error(e.getStackTrace());
					}
				}
			}

			// get the total number of docs from the urls table
			Scanner scann = AccumuloUtils.connectRead(urlTable);

			IteratorSetting cfg3 = new IteratorSetting(11,
					TotalDocFinder.class, properties);
			scann.addScanIterator(cfg3);

			for (Entry<Key, Value> entry : scann) {
				try {
					output.put(entry.getKey().getRow().toString(),
							(Integer) IngestUtils.deserialize(entry.getValue()
									.get()));
				} catch (Exception e) {
					if (e.getMessage() != null) {
						log.error(e.getMessage());
					} else {
						log.error(e.getStackTrace());
					}
				}
			}

			// Create the sample table file
			System.out.println(output.size());
			System.out.println("sample file: " + sampleFile);
			File f = new File(sampleFile);
			f.createNewFile();

			// use buffering
			OutputStream file = new FileOutputStream(f);
			OutputStream buffer = new BufferedOutputStream(file);
			ObjectOutput out = new ObjectOutputStream(buffer);
			out.writeObject(output);
			out.flush();
			out.close();
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
		}
	}

}
