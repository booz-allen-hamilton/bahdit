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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;


public class TotalDocFinder extends WrappingIterator {

	private static final String TOTAL_DOCS = "[[TOTAL NUM DOCS]]";
	private SortedKeyValueIterator<Key, Value> source;
	private Key topKey;
	private Value topValue;
	private boolean first = true;
	private boolean reportLast = true;

	@Override
	public Key getTopKey() {
		return topKey;
	}

	@Override
	public Value getTopValue() {
		return topValue;
	}

	@Override
	public boolean hasTop() {
		return topKey != null;
	}

	@Override
	/**
	 * if the first time called, ignore.  This is because seek() will call 
	 * execute(), which is one more than necessary.
	 */
	public void next() throws IOException {
		if (first)
			first = false;
		else
			execute();
	}

	@Override
	/**
	 * calls execute() to get to the next() function.  Sets the topKey and 
	 * topValue variables.
	 */
	public void seek(Range range, Collection<ByteSequence> seekColFam,
			boolean inclusive) throws IOException {
		source.seek(range, seekColFam, inclusive);
		execute();
	}

	@Override
	/**
	 * create new instance of CosimCalculator
	 */
	public TotalDocFinder deepCopy(IteratorEnvironment env) {
		return (TotalDocFinder) super.deepCopy(env);
	}

	@Override
	/**
	 * Creates a new CosimCalculator and gets the query and query vector for 
	 * comparing to the document vectors
	 * 
	 * @param source
	 * @param options - the query will be stored in here
	 * @param env
	 */
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment envir)
			throws IOException {

		super.init(source, options, envir);
		this.source = source;
		System.out.println("init");
	}

	/**
	 * Finds the documents that contain all the search terms If a document
	 * contains those terms, it is put into a Map and the cosine similarity of
	 * the document is calculated. The key and the cosine similarity are then
	 * returned as a single row.
	 * 
	 * @throws ClassNotFoundException
	 */
	private void execute() throws IOException {

		// if true, signals that we've scanned a whole column family and it
		// matches
		int count = 0;

		// also make sure we don't run out of records
		while (source.hasTop()) {
			count++;
			source.next();
		}

		topKey = new Key(TOTAL_DOCS);
		topValue = new Value(IngestUtils.serialize(count));

		// set top key and value to null after the last document has been
		// reported
		if (!source.hasTop()) {
			if (reportLast)
				reportLast = false;
			else {
				topKey = null;
				topValue = null;
			}
		}
	}
}
