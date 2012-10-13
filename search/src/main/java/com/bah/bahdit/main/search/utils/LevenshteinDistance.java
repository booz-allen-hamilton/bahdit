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
package com.bah.bahdit.main.search.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletContext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *  LevenshteinDistance is where the correction for a query is created.  A 
 *  correction is necessary if the terms in the query are not found in the 
 *  sampling table, and therefore is not found in the Accumulo table either.  
 *  We use Apache Lucene's spell checker in order to find the best single 
 *  correction for the query and returns this to the user instead.
 */
public class LevenshteinDistance {

  /**
   * Given a context to place all of the spell check files in, this method
   * sets up the spellchecker object using the sample table
   * 
   * @param context - the context of the current servlet
   * @param sampleTable - the full text sample table
   * @return - a spellchecker object
   */
  public static SpellChecker createSpellChecker(ServletContext context, 
      HashMap<String, Integer> sampleTable) {

    SpellChecker spellChecker = null;

    // write terms from sample table to text file, to be basis of dictionary
    File f = new File("dictionary" + System.nanoTime() + ".txt");
    try {
      f.createNewFile();
      BufferedWriter out = new BufferedWriter(new FileWriter(f));

      for (String entry : sampleTable.keySet()) {
        out.write(entry + "\n");
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    String dPath = System.getProperty("user.dir") + "/spellcheck" + System.nanoTime();

    File dir = new File(dPath);
    Directory directory = null;

    try {
      directory = FSDirectory.open(dir);
    } catch (IOException e3) {
      e3.printStackTrace();
    }

    try {
      spellChecker = new SpellChecker(directory);
    } catch (IOException e2) {
      e2.printStackTrace();
    }

    StandardAnalyzer a = new StandardAnalyzer(Version.LUCENE_40);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, a);
    boolean fullMerge = true;
    PlainTextDictionary dict = null;

    try {
      dict = new PlainTextDictionary(f);
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
    }

    try {
      spellChecker.indexDictionary(dict, config, fullMerge);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return spellChecker;
  }
}
