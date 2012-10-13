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
package com.bah.applefox.ingest.temp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class CountFileLines {

	static int spc_count = -1;

	static int javaLineCounts = 0;
	static int jsLineCounts = 0;
	
	static void Process(File aFile) throws IOException {
		spc_count++;
		String spcs = "";
		for (int i = 0; i < spc_count; i++)
			spcs += " ";
		if (aFile.isFile()) {
			if (aFile.getName().toLowerCase().endsWith(".java")) {
				String fileContent = readFileAsString(aFile.getAbsolutePath());
				String[] lines = fileContent.split("\n");
				for(String s : lines){
					if(s.length() > 0){
						javaLineCounts++;
					}
				}
				
			} else if(aFile.getName().toLowerCase().endsWith(".js")){
				String fileContent = readFileAsString(aFile.getAbsolutePath());
				String[] lines = fileContent.split("\n");
				for(String s : lines){
					if(s.length() > 0){
						jsLineCounts++;
					}
				}
			}
		} else if (aFile.isDirectory()) {
			File[] listOfFiles = aFile.listFiles();
			if (listOfFiles != null) {
				for (int i = 0; i < listOfFiles.length; i++)
					Process(listOfFiles[i]);
			} else {
				System.out.println(spcs + " [ACCESS DENIED]");
			}
		}
		spc_count--;
	}

	private static String readFileAsString(String filePath)
			throws java.io.IOException {
		StringBuffer fileData = new StringBuffer(1000);
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		char[] buf = new char[1024];
		int numRead = 0;
		while ((numRead = reader.read(buf)) != -1) {
			String readData = String.valueOf(buf, 0, numRead);
			fileData.append(readData);
			buf = new char[1024];
		}
		reader.close();
		return fileData.toString();
	}

	public static void main(String[] args) throws IOException {
		String nam = "/Users/zacharyauld/Desktop/Bahdit";
		File aFile = new File(nam);
		Process(aFile);
		
		System.out.println(jsLineCounts);
		System.out.println(javaLineCounts);
		System.out.println();
		
		nam = "/Users/zacharyauld/Documents/workspace/PlugableIngestWithDivsRemoved";
		aFile = new File(nam);
		Process(aFile);

		System.out.println(jsLineCounts);
		System.out.println(javaLineCounts);
		System.out.println();
		System.out.println(jsLineCounts + javaLineCounts);
	}

}