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
/**
 * 
 * Cloud Analytics Intern Proj. 2012
 * Guice Dependency Injection Module
 * SearchModule.java
 * 
 */

package com.bah.bahdit.main.search;

import com.bah.bahdit.main.plugins.fulltextindex.FullTextIndex;
import com.bah.bahdit.main.plugins.imageindex.ImageIndex;
import com.bah.bahdit.main.plugins.index.Index;
import com.google.inject.AbstractModule;

/**
 * 
 * Used by Guice for Dependency Injection. In order to add a new index all 
 * you have to do is edit this file and then bind your index class to the main 
 * index class. 
 * 
 */
public class SearcherModule extends AbstractModule {

	public static final String FULL_TEXT_INDEX = "FULL_TEXT_INDEX"; 
	public static final String IMAGE_INDEX = "IMAGE_INDEX"; 

	private String indexToUse = "";

	public SearcherModule(String index){
		this.indexToUse = index;
	}

	@Override 
	protected void configure() {
		if(indexToUse.equals(FULL_TEXT_INDEX)){
			bind(Index.class).to(FullTextIndex.class);
		} else if (indexToUse.equals(IMAGE_INDEX)){
			bind(Index.class).to(ImageIndex.class);
		}
	}
}