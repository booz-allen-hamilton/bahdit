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

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.disk.DiskFileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import com.afspq.model.ImageResults;
import com.afspq.model.Results;
import com.bah.bahdit.main.search.Search;
import com.bah.bahdit.main.search.SearcherModule;

/**
 * Servlet implementation class ProcessQuery
 */
public class ProcessQuery extends HttpServlet {

	private static final long serialVersionUID = 1L;

	Search fullTextSearch = null;
	Search imageSearch = null;

	// This Happens Once and is Reused
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		try {
			fullTextSearch = new Search(config.getServletContext(), SearcherModule.FULL_TEXT_INDEX);
			imageSearch = new Search(config.getServletContext(), SearcherModule.IMAGE_INDEX);
		} catch(Exception ex) {
			System.out.println(new Date().toString() + "\nABORT: Servlet initialization failed.");
			System.exit(1);
		}
	}

	@SuppressWarnings({"unused", "rawtypes" })
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		boolean isMultipart = ServletFileUpload.isMultipartContent(request);

		if (isMultipart) {
			DiskFileItemFactory factory = new DiskFileItemFactory();
			ServletFileUpload upload = new ServletFileUpload(factory);

			try {
				String root = getServletContext().getRealPath("/");
				File path = new File(root + "/uploads");
				List items = upload.parseRequest(request);

				if (!path.exists()) {
					boolean status = path.mkdirs();
				}

				
				if(items.size() > 0){
					DiskFileItem file = (DiskFileItem) items.get(0);
					File uploadedFile = new File(path + "/" + file.getName());
					
					file.write(uploadedFile);
					request.setAttribute("results", new ImageResults().getResults(uploadedFile,imageSearch, 0, true));
					request.getRequestDispatcher("imageResults.jsp").forward(request, response);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		response.setContentType("text/html");
		String searchType = request.getParameter("searchType");

		if(searchType != null && searchType.equals("image")) {
			request.setAttribute("results", new ImageResults()
			       .getResults(request.getParameter("imgQuery"), imageSearch, Integer.parseInt(request.getParameter("similar")), false));
			request.getRequestDispatcher("imageResults.jsp").forward(request, response);
			
		} else if (searchType != null && searchType.equals("web")) {
			request.setAttribute("results", new Results()
				   .getResults(request.getParameter("query"), fullTextSearch, Integer.parseInt(request.getParameter("page")), 8));
			request.getRequestDispatcher("results.jsp").forward(request, response);
		}
	}
}
