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
package com.afspq.web;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import com.afspq.model.PigResults;
import com.bah.bahdit.main.plugins.fulltextindex.FullTextIndex;

/**
 * Servlet implementation class ProcessQuery
 */
public class PigQuery extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static PigServer pigServer = null;
	private String STORAGE_RESULTS = "/tmp/accumulo-pig/tmp";
	private static final String CONFIG_FILE = "properties.conf";
	private static ServletContext context;

	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		context = config.getServletContext();
		try {
			pigServer = new PigServer(ExecType.LOCAL);
		} catch (ExecException e1) {
			e1.printStackTrace();
		}

		try {
			pigServer.registerJar(config.getServletContext().getRealPath("jars/accumulo-core-1.4.0.jar"));
			pigServer.registerJar(config.getServletContext().getRealPath("jars/cloudtrace-1.4.0.jar"));
			pigServer.registerJar(config.getServletContext().getRealPath("jars/libthrift-0.6.1.jar"));
			pigServer.registerJar(config.getServletContext().getRealPath("jars/zookeeper-3.3.1.jar"));
			pigServer.registerJar(config.getServletContext().getRealPath("jars/accumulo-pig.jar"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		response.setContentType("text/html");		

		String table = request.getParameter("table");
		String query = request.getParameter("query");
		String loadVar = request.getParameter("loadVar");
		String tableVar = request.getParameter("tableVar");
		String storeVar = request.getParameter("storeVar");
		String script = request.getParameter("script");
		String schemaVar = request.getParameter("schemaVar");

		String resultsBin = STORAGE_RESULTS + System.nanoTime();

		if(table != null && query != null && loadVar != null && schemaVar != null && storeVar != null &&
				!table.equals("") && !loadVar.equals("") && !storeVar.equals("") && !schemaVar.equals("")) {

			ArrayList<String> cols = runMyQuery(table, query, loadVar, schemaVar, script, tableVar, storeVar, resultsBin);

			ArrayList<String> rows = new ArrayList<String>();
			String[] resultFiles = new File(resultsBin).list();
			for(String file : resultFiles) {
				if(file.charAt(0) == '.' || file.charAt(0) == '_' ) continue;
				FileInputStream stor = new FileInputStream(resultsBin + "/" + file);
				DataInputStream sin = new DataInputStream(stor);
				BufferedReader sbr = new BufferedReader(new InputStreamReader(sin));
				String strLine;
				while ((strLine = sbr.readLine()) != null) 
					rows.add(strLine);
			}
			request.setAttribute("results", new PigResults(table, query, loadVar, schemaVar, script, tableVar, storeVar, rows, cols));
		} else {
			request.setAttribute("results", new PigResults(table, query, loadVar, schemaVar, script, tableVar, storeVar, null, null));
		}

		deleteDir(new File(resultsBin));
		
		request.getRequestDispatcher("pig.jsp").forward(request, response);
	}

	private static ArrayList<String> runMyQuery(String table, String query, String loadVar, String schemaVar, String script, String tableVar, String storeVar, String resultsBin) throws IOException {

		String queryString = "";
		if(!query.equals("")){
			// Fix the properties file
			Properties p = new Properties();
			p.load(new FileInputStream(context.getRealPath(CONFIG_FILE)));
			if(p.getProperty(FullTextIndex.FT_SAMPLE).lastIndexOf('/') == -1){
				p.setProperty(FullTextIndex.FT_SAMPLE, context.getRealPath(p.getProperty(FullTextIndex.FT_SAMPLE)));
			} else {
				p.setProperty(FullTextIndex.FT_SAMPLE, context.getRealPath(p.getProperty(FullTextIndex.FT_SAMPLE)
						.substring(p.getProperty(FullTextIndex.FT_SAMPLE).lastIndexOf('/'))));
			}
			p.store(new FileOutputStream(context.getRealPath(CONFIG_FILE)), null);
			queryString = " WITH QUERY " + query;
		}

		pigServer.registerQuery(loadVar + " = LOAD '" + table +" CONFIG " + 
				context.getRealPath(CONFIG_FILE) + queryString + " AS " + schemaVar.split(",").length + 
				"' USING org.apache.accumulo.pig.AccumuloStorage()" + "AS (" + schemaVar + ");");

		if(!script.equals("Write your Pig script here! :)") && !script.equals("")){
			for(String s : script.split(";")) 
				pigServer.registerQuery(s + ";");
		}
		
		pigServer.registerQuery("STORE " + storeVar + " INTO '" + resultsBin + "' USING PigStorage('~');");

		String schema = pigServer.dumpSchema(storeVar).toString();
		String[] cols = schema.substring(schema.indexOf('{') + 1, schema.lastIndexOf('}')).split(",");
		ArrayList<String> columns = new ArrayList<String>();
		for(String s : cols){
			columns.add(s.split(":")[0].trim());
		}
		
		if(!tableVar.equals("")){
			pigServer.registerQuery("STORE " + storeVar + " INTO 'YO23 CONFIG " + context.getRealPath(CONFIG_FILE) + 
					"' using org.apache.accumulo.pig.AccumuloStorage();");
		}
		
		return columns;
	}
	
	public boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
    
        return dir.delete();
    }
}
