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
package com.bah.bahdit.main.plugins.imageindex.utils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.log4j.Logger;

import com.bah.bahdit.main.plugins.imageindex.ImageIndex;
import com.bah.bahdit.main.search.Search;

/**
 * 
 */
public class SimiliarImageRanker extends WrappingIterator {

	private static final Logger log = Logger.getLogger(SimiliarImageRanker.class);

	private SortedKeyValueIterator<Key, Value> source;
	private Key topKey;
	private Value topValue;
	boolean first = true;
	private int distance;
	private HashSet<String> query;
	private PriorityQueue<Term> termsPQ;

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
	 * Creates a new SimiliarImageRanker
	 * Initializes the Priority Queue
	 */
	@SuppressWarnings("unchecked")
	public void init(SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options, IteratorEnvironment envir)
					throws IOException {

		super.init(source, options, envir);
		this.source = source;

		distance = Integer.parseInt(options.get(ImageIndex.IMG_HASH_DISTANCE));

		String rangeString = options.get(Search.QUERY);
		byte[] bR;
		if (rangeString == null) query = null;
		else {
			bR = rangeString.getBytes(ImageIndex.ENCODING);
			try {
				query = (HashSet<String>)Utils.deserialize(bR);
			} catch (ClassNotFoundException e) {
				query = null;
			}
		}

		termsPQ = new PriorityQueue<Term>(100, new ImageTermComparator());
	}


	/**
	 * Gets the next highest ranked image
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


		while (source.hasTop()) {
			try{
				String key = source.getTopKey().getRow().toString();

				log.trace("STARTING KEY : " + key);
				boolean found = false;
				double difference = 0;
				try{
					for(String s : query){


						difference = 0;

						int len = (key.length() < s.length())?key.length():s.length();

						try{

							for (int i = 0; i < len; i++) {

								if (key.charAt(i) != s.charAt(i)){ 
									++difference;
								}
							}
						} catch(Exception e){
							log.trace(e.getMessage() + "WOOPScloser");
						}

						if(difference < distance){
							found = true;
							break;
						}
					}
				} catch(Exception e){
					log.trace("STRING");
				}
				try{
				if(found){ 
					termsPQ.add(new Term(source.getTopKey(),difference));
				}
				} catch(Exception e){
					log.trace("ADD");
				}
				try{
				source.next();
				} catch(Exception e){
					log.trace("SOURCE");
				}
			} catch(Exception e){
				log.trace("WOOPS");
			}
		}


	}

	/**
	 * Dumps the contents of the PQ to the user.
	 */
	private void getRank() throws IOException {

		Term next = null;
		log.trace("SPITTING IT BACK TO THE USER! NUMBER : " + termsPQ.size());
		if (!termsPQ.isEmpty() && (next = termsPQ.peek()) != null) {
			log.trace(next.getKey().toString() + " " + next.getValue().toString());
			topKey = next.getKey();
			topValue = new Value(Utils.serialize(next.getValue()));
			termsPQ.remove();
		} 
		else {
			topKey = null;
			topValue = null;
		}

	}



}