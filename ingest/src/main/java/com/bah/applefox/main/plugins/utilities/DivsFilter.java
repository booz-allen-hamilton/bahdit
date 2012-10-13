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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is used to filter specified div tags from a block of html code,
 * leaving everything but the unwanted sections untouched
 * 
 * 
 */
public class DivsFilter {

	/**
	 * This method filters the code by div tag, removing divs with the ids
	 * contained in removeIDs
	 * 
	 * @param htmlCode
	 *            - The source code
	 * @param removeIDs
	 *            - the IDs to remove
	 * @return - the filtered code
	 */
	public static String filterDivs(String htmlCode, HashSet<String> removeIDs) {

		// String to store the final filtered code
		String finalResult = "";

		// Gets all of the line numbers and div tags out of the code
		HashMap<Integer, String> tags = getDivTags(htmlCode);

		// Keeps track of depth if the current code section is in an unwanted
		// div tag
		int divDepth = 0;

		// The index of where the filtering last left off
		int index = 0;

		// Get the keySet of the tags(the tags themselves, not line numbers)
		Set<Integer> keySet = tags.keySet();

		// Add all of the keys to an ArrayList, then sort them for numerical
		// ordering
		ArrayList<Integer> keySetArray = new ArrayList<Integer>();
		keySetArray.addAll(keySet);
		Collections.sort(keySetArray);

		for (Integer tagLine : keySetArray) {
			// Set the current tag
			String divTag = tags.get(tagLine);

			// If the div depth is 0 (i.e. the code is outside of an unwanted
			// div tag) add the most recent block of code, up to the beginning
			// of the current tag
			if (divDepth == 0) {
				finalResult += htmlCode.substring(index, tagLine);
			}

			if (divTag.toLowerCase().startsWith("<div")) {
				// If the tag is an opening div
				if (divDepth != 0) {
					// If it is a div within an unwanted div, increment the div
					// tag
					// depth
					++divDepth;
				} else if (removeIDs.contains(getTagAttribute(divTag, "id"))) {
					// Otherwise if it is an unwanted tag, increment div depth
					++divDepth;
				} else {
					// Finally, if the above cases are not true, add the tag
					// because
					// it is wanted
					finalResult += divTag;
				}
			} else if (divDepth > 0) {
				// This will be a closing div within an unwanted div, indicating
				// the div depth goes one layer up
				--divDepth;
			}

			// Increment the index to the end of the tag
			index = tagLine + divTag.length();
		}

		if (divDepth == 0) {
			// Add the latest block of code if it is wanted
			finalResult += htmlCode.substring(index);
		}

		return finalResult;

	}

	/**
	 * This method is used to get the value of the given attribute within the
	 * tag. If the attribute is not found, a blank string is returned. The given
	 * tag should be a valid HTML tag, in the form <tag content and attributes>
	 * 
	 * @param tag
	 *            - the valid HTML tag
	 * @param attribute
	 *            - the attribute to find
	 * @return - the value of the given attribute
	 */
	public static String getTagAttribute(String tag, String attribute) {

		// Try to find the attribute within the tag
		int attributeIndex = tag.toLowerCase().indexOf(
				" " + attribute.toLowerCase() + "=");
		if (attributeIndex == -1) {
			attributeIndex = tag.toLowerCase().indexOf(
					" " + attribute.toLowerCase() + " =");
		}

		// This block of code ensures the attribute is truly the attribute, not
		// something within the value of another attribute
		// (such as <div title="id='test'" id='temp'>) which is valid
		while (attributeIndex != -1
				&& ((tag.substring(0, attributeIndex).split("\"").length - 1) % 2 == 1 || (tag
						.substring(0, attributeIndex).split("'").length - 1) % 2 == 1)) {
			attributeIndex++;
			int tempIndex = tag.substring(attributeIndex).toLowerCase()
					.indexOf(" " + attribute.toLowerCase() + "=");
			if (tempIndex == -1) {
				tempIndex = tag.toLowerCase().indexOf(
						" " + attribute.toLowerCase() + " =");
			}
			attributeIndex = tempIndex;
		}

		// If the attribute is found within the tag
		if (attributeIndex != -1) {
			// Set attribute index to the start of the value, with the "="
			// included
			attributeIndex += attribute.length();

			// Create id, a string representing from the beginning of the
			// attribute value to the end of the tag
			String id = tag.substring(attributeIndex + 1);
			// Remove the "=" and leading and trailing white space
			id = id.substring(1);
			id = id.trim();

			// Trim the tag to just the value
			if (id.startsWith("'")) {
				// If it starts with an apostrophe:
				// Remove the apostrophe, and get the value from that index to
				// the next apostrophe
				id = id.substring(1);
				if (id.indexOf("'") != -1) {
					return id.substring(0, id.indexOf("'"));
				} else {
					// Not a valid tag attribute value
					// Problem with tag formatting
					return "";
				}
			} else if (id.startsWith("\"")) {
				// If it starts with a quote:
				// Remove the quote, and get the value from that index to the
				// next quote
				id = id.substring(1);
				if (id.indexOf("\"") != -1) {
					return id.substring(0, id.indexOf("\""));
				} else {
					// Not a valid tag attribute value
					// Problem with tag formatting
					return "";
				}
			} else {
				// Not a valid tag attribute value
				return "";
			}
		} else {
			// Tag attribute not found
			return "";
		}
	}

	/**
	 * This method finds all of the div tags found in the HTML code, and returns
	 * the entire tag (attributes and values included) with the Map index being
	 * the tag's beginning index in the HTML code.
	 * 
	 * @param code
	 *            - the code to search for div tags
	 * @return - HashMap of location (Integer) and tag (String)
	 */
	public static HashMap<Integer, String> getDivTags(String code) {

		// The HashMap of div tags to return
		HashMap<Integer, String> ret = new HashMap<Integer, String>();

		// Find the first potential tag in the given code
		int index = code.indexOf("<");
		int substringIndex = code.indexOf("<");

		// If there are no potential tags in the given code, skip to the end
		if (index == -1) {
			index = code.length();
		}

		while (index < code.length()) {

			// Check if the potential code is a comment, div, or other tag
			if (code.substring(index).startsWith("<!--")) {
				// If it is a comment, ignore the whole comment (set index to
				// the end of the comment, or end of the document if no end of
				// the comment is found)
				int endIndex = code.substring(index).indexOf("-->");
				if (endIndex != -1) {
					index = code.substring(index).indexOf("-->") + 3 + index;
				} else {
					index = code.length();
				}
			} else if (code.substring(index).toLowerCase().startsWith("<div")
					|| code.substring(index).toLowerCase().startsWith("</div")) {
				// If it is either an opening div or closing div tag, set end
				// index to the end of the tag
				int endIndex = code.substring(index).indexOf(">") + 1;
				if (endIndex != 0) {
					// If the end of the tag is found, add it to the HashMap
					ret.put(new Integer(index),
							code.substring(index, endIndex + index));
					index = endIndex + index;
				} else {
					// Otherwise, skip to the end of the document
					index = code.length();
				}

			} else {
				// Set index and end index to the location after the next ">"
				if (code.substring(index).indexOf(">") != -1) {
					int endIndex = code.substring(index).indexOf(">") + index
							+ 1;
					index = endIndex;
				} else {
					index = code.length();
				}
			}
			
			if (index < code.length()) {
				// Set the endix to the next index of "<"
				substringIndex = code.substring(index).indexOf("<");
				if (substringIndex != -1) {
					index = substringIndex + index;
				} else {
					// If there are no more "<", skip to the end
					index = code.length();
				}
			}

		}

		return ret;
	}
}
