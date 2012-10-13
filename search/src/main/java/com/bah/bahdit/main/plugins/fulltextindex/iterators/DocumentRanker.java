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
package com.bah.bahdit.main.plugins.fulltextindex.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;

import com.bah.bahdit.main.plugins.fulltextindex.data.FixedSizePQ;
import com.bah.bahdit.main.plugins.fulltextindex.data.Term;
import com.bah.bahdit.main.plugins.fulltextindex.data.TermComparator;
import com.bah.bahdit.main.plugins.fulltextindex.utils.Utils;
import com.bah.bahdit.main.search.Search;

/**
 * The DocumentRanker is used for the last iteration in phase 2.  It takes 
 * results from the previous iteration and ranks them by their value using a 
 * Fixed Sized Priority Queue.  Only returns the number requested in the 
 * properties.  Also, uses the page number to figure out which range of values 
 * to return to the user. 
 * 
 * (e.x. page = 4, num results = 10 => 31 - 40 ranked)
 * 
 * Properties to be supplied:
 * num results = the number of results the user wishes to have returned.
 * page = the page number of results that the user wants
 * 
 */
public class DocumentRanker extends WrappingIterator {
  
  private static final String NUM_RESULTS = "[NUM_RESULTS]";

	private SortedKeyValueIterator<Key, Value> source;
	private Key topKey;
	private Value topValue;
	private FixedSizePQ<Term> termsPQ;
	private ArrayList<Term> results;
	boolean first = true;
	private int page = 1;
	private int count = 0;
	private int numResultsRequested;

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

	/**
	 * Creates a new DocumentRanker
	 * Gets the number of results the user wishes to be returned
	 * Initializes the Priority Queue
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

		page = Integer.parseInt(options.get(Search.PAGE));

		// get how many results the client wants returned
		String numResults = options.get(Search.NUM_RESULTS);
		numResultsRequested = Integer.parseInt(numResults);

		// make new fixed size priority queue to rank the cosims
		termsPQ = new FixedSizePQ<Term>(page * numResultsRequested, new TermComparator());
	}


	/**
	 * Gets the next highest ranked document
	 */
	@Override
	public void next() throws IOException {
		// if the first time called, ignore.  This is because seek() will call 
		// execute(), which is one more than necessary.
		if (first) first = false;
		else getRank();
	}


	/**
	 * Calls getRank() to get to the next() function.  Sets the topKey and 
	 * topValue variables.
	 */
	@Override
	public void seek(Range range, Collection<ByteSequence> seekColFam, 
			boolean inclusive)  {
		try { 
			source.seek(range, seekColFam, inclusive);
			rankTerms();
			getRank();
		} 
		catch (Exception e) { }
	}

	/**
	 * Ranks all the terms by placing them in the priority queue
	 */
	private void rankTerms() throws IOException, ClassNotFoundException {

		if(source.hasTop()) {
			while (source.hasTop()) {
				++count;
				termsPQ.add(new Term(source.getTopKey(), 
						(Double)Utils.deserialize(source.getTopValue().get())));
				source.next();
			}
			// randomly counted one extra
			--count;
		}

		results = new ArrayList<Term>();

		// if there are less results than requested, request fewer results
		if(numResultsRequested * page > count) {
			numResultsRequested = count - numResultsRequested * (page-1);
		}

		// get all the results from the priority queue
		while(!termsPQ.isEmpty())
			results.add(termsPQ.pollFirst());

		// handles edge case of results being too small
		if(numResultsRequested >= results.size())
			numResultsRequested = results.size() - 1;
	}

	/**
	 * Dumps the contents of the PQ to the user.
	 */
	private void getRank() throws IOException {

		// print out the all the results for the client
		if (!results.isEmpty()) {
			Term nextTerm = results.remove(results.size() - 1);
			topKey = nextTerm.getKey();
			// place the number of found results in the last key
			if(results.size() == 0)
				topKey = new Key(topKey.getRow(), topKey.getColumnFamily(), new Text(NUM_RESULTS + " : " + String.valueOf(count)));
			topValue = new Value(Utils.serialize(nextTerm.getValue()));
		} 
		else {
			topKey = null;
			topValue = null;
		}
	}



}