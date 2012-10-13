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
package com.afspq.model;

import java.util.ArrayList;

public class PigResults {

	private String query;
	private String table;
	private String loadVar;
	private String storageFile;
	private String tableVar;
	private String storeVar;
	private ArrayList<String> rows;
	private String script;
	private String schemaVar;
	private ArrayList<String> cols;

	public PigResults(String table, String query, String loadVar, String schemaVar, String script, 
			String tableVar, String storeVar, ArrayList<String> rows, ArrayList<String> cols) {
		
		this.table = (table == null)?"":table;
		this.query = (query == null)?"":query;
		this.loadVar = (loadVar == null)?"":loadVar;
		this.schemaVar = (schemaVar == null)?"":schemaVar;
		this.script = (script == null)?"Write your Pig script here! :)":script;
		this.tableVar = (tableVar == null)?"":tableVar;
		this.storeVar = (storeVar == null)?"":storeVar;
		this.rows = rows;
		this.cols = cols;
	}

	public String getHeaders(){
		if(cols == null || cols.size() == 0)
			return "";
		String result = "<tr>";
		for(String ch : cols)
			result += "<th>" + ch + "</th>"; 
		 result += "</tr>";
		 return result;
	}

	public String getResultRows(){
		if(rows == null)
			return "";
		int numRows = rows.size();
		String stringRows = "";
		for(int i = 0; i < numRows; i++){
			String[] row = rows.get(i).split("~");
			stringRows += "<tr class=\"gradeA\">";
			for(String c : row){
				stringRows += "<td>" + c + "</td>";
			}
			stringRows += "</tr>";
		}
		return stringRows;
	}
	
	public String getSchemaVar() {
		return schemaVar;
	}
	
	public String getQuery() {
		return query;
	}

	public String getTable() {
		return table;
	}

	public String getLoadVar() {
		return loadVar;
	}

	public String getStorageFile() {
		return storageFile;
	}

	public String getStoreVar() {
		return storeVar;
	}

	public String getScript() {
		return script;
	}

	public ArrayList<String> getRows() {
		return rows;
	}

	public String getTableVar() {
		return tableVar;
	}

}
