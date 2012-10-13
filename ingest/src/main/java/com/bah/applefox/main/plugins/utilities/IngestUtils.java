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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This is a utility class used to create NGrams from a given input string, and
 * is used for serialization/deserialization of objects before writing to the
 * Accumulo table.
 * 
 */
public class IngestUtils {

	/**
	 * if inputString = " " : result = empty arraylist if inputString = "" :
	 * result = arraylist of size 1 with "" as element
	 * 
	 * @param inputString
	 *            - string to be split into n-grams
	 * 
	 * @return arraylist of n-grams from up to constant max n-gram value
	 */
	public static ArrayList<String> createNGrams(String inputString, int nGrams) {

		ArrayList<String> terms = new ArrayList<String>();
		String[] words = getWords(inputString);

		if (words.length == 0)
			return terms;

		// makes sure the max n-gram is within the bounds of the query
		if (nGrams <= 0)
			nGrams = 1;
		else if (nGrams > words.length)
			nGrams = words.length;

		// creates n-grams from 1 to n
		for (int x = 1; x <= nGrams; x++)
			for (int i = 0; i < words.length - x + 1; i++)
				terms.add(concat(words, i, i + x));

		return terms;
	}

	/**
	 * produces an array of strings, separated by the regex (mostly punctuation)
	 * 
	 * @param input
	 *            - the string version of a document or query
	 * @return an array of all the words in the input
	 */
	public static String[] getWords(String input) {

		// splits based on punctuation, not contractions
		String regexp = "[^-\'\\w]+";
		String[] words = input.split(regexp);

		if (words.length == 0)
			return new String[0];

		String[] words2 = new String[words.length - 1];

		// gets rid of a null character that appears during tika parsing
		for (int i = 1; i < words.length; i++) {
			words2[i - 1] = words[i];
		}
		return words2;
	}

	/**
	 * Turns an object into a byte array Object must be serializable
	 * 
	 * @param o
	 *            - object to be serialized
	 * @return byte array representation of the object
	 * @throws IOException
	 */
	public static byte[] serialize(Object o) throws IOException {
		ByteArrayOutputStream ba = new ByteArrayOutputStream(1000);
		ObjectOutputStream oba = new ObjectOutputStream(ba);
		oba.writeObject(o);
		return ba.toByteArray();
	}

	/**
	 * Turns a byte array into its original object form
	 * 
	 * @param b
	 *            - byte array to be read as object
	 * @return object from the byte array representation
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object deserialize(byte[] b) throws IOException,
			ClassNotFoundException {
		ByteArrayInputStream ba = new ByteArrayInputStream(b);
		ObjectInputStream oba = new ObjectInputStream(ba);
		return oba.readObject();
	}

	/**
	 * used by : createNGrams
	 * 
	 * @param words
	 *            - an array of single words
	 * @param start
	 *            - beginning index
	 * @param end
	 *            - ending index
	 * 
	 * @return concatenated string
	 */
	protected static String concat(String[] words, int start, int end) {

		StringBuilder str = new StringBuilder();
		str.append(words[start]);

		for (int i = start + 1; i < end; i++)
			str.append(" " + words[i]);

		return str.toString();
	}

	/**
	 * @param input
	 *            - arraylist of n-grams in a string
	 * 
	 * @return hashmap of words mapped to their frequency in the input string
	 */
	public static HashMap<String, Integer> collectTerms(ArrayList<String> input) {

		ArrayList<String> docWords = input;
		HashMap<String, Integer> terms = new HashMap<String, Integer>();

		for (String s : docWords) {

			// inserts new key if doesn't previously exist
			if (!terms.containsKey(s))
				terms.put(s, 1);

			// updates the frequency count
			else
				terms.put(s, terms.get(s) + 1);

		}

		return terms;
	}
}
