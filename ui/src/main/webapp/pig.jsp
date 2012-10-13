<!-- 
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
 -->
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ page import="com.afspq.model.PigResults"%>
<% PigResults results = (PigResults) request.getAttribute("results"); %>
<!doctype html>
<html>
<head>
<meta charset="UTF-8">
<link href="images/search-icon.png" rel="icon" type="image/x-icon"/>
<link rel="stylesheet" type="text/css" href="css/style.css" />
<link rel="stylesheet" type="text/css" href="css/demo_table.css" />
<link rel="stylesheet" type="text/css" href="css/resultsPage.css"/>
<link rel="stylesheet" type="text/css" href="css/navbar.css"/>
<link rel="stylesheet" type="text/css" href="css/viz.css"/>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.7.2/jquery.min.js"></script>
<script src="scripts/jquery.dataTables.js"></script>
<title>Pig - Bahdit</title>
<script type="text/javascript" charset="utf-8">
	$(document).ready(function() {
		if ($("headers").html() === "") {
			alert('bob');
			$("#pigResults").css("background-image", "url('../images/bahdit-logo.png')");
		}
		$('#example').dataTable();
	});
</script>
</head>

<body id="resultsPage">
	<div id="headerWithSearchBar">
		<div id="searchBar"
			style="margin-left: 10px; margin-bottom: 10px; float: left; width: 60%">
			<h3>
				<a href="../Bahdit" style="text-decoration: none; color: black;">Bahdit</a>
			</h3>
		</div>
	</div>

	<div id="results">
		<div id="resultsFrame">
			<form id="queryField" action="PigQuery" method="get">
				<br> <br>
				<p style="font-size: 8pt; display: inline;">LOAD FROM</p>
				<input type="text" name="table" value="<% out.print(results.getTable()); %>" />
				<p style="font-size: 8pt; display: inline;">WITH QUERY</p>
				<input type="text" name="query" value="<% out.print(results.getQuery()); %>" /> <br> <br>
				<p style="font-size: 8pt; display: inline;">INTO VARIABLE</p>
				<input type="text" name="loadVar" value="<% out.print(results.getLoadVar()); %>" /> 
				<p style="font-size: 8pt; display: inline;">AS</p>
				<input type="text" name="schemaVar" value="<% out.print(results.getSchemaVar()); %>" /> <br> <br>
				<textarea rows="30" cols="90" name="script" style="resize: none;"><% out.print(results.getScript()); %></textarea> <br> <br>
				<p style="font-size: 8pt; display: inline;">SEE VARIABLE </p>
				<input type="text" name="storeVar" value="<% out.print(results.getStoreVar()); %>" />
				<p style="font-size: 8pt; display: inline;">STORE IN TABLE </p>
				<input type="text" name="tableVar" value="<% out.print(results.getTableVar()); %>" /> <br> <br>
				<input type="submit" name="submit" value="GO!" />
			</form>
		</div>
	</div>
	<div id="pigResults" style="overflow:scroll;">
		<div id="table" style="margin-top: 20px; overflow:scroll;">
			<table id="example" class="display">
			<thead id="headers">
				<% out.print(results.getHeaders()); %>
			</thead>
			<tbody>
				<% out.print(results.getResultRows()); %>
			</tbody>
			<tfoot>
				<% out.print(results.getHeaders()); %>
			</tfoot>
		</table>
		</div>
	</div>
</body>
</html>