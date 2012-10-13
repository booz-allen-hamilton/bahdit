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
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ page import="java.util.*"%>
<%@ page import="com.afspq.model.ImageResults"%>
<!doctype html>
<html>
<head>
<meta charset="UTF-8">
<link href="images/search-icon.png" rel="icon" type="image/x-icon"/>
<link rel="stylesheet" type="text/css" href="css/style.css" />
<link rel="stylesheet" type="text/css" href="css/resultsPage.css"/>
<link rel="stylesheet" type="text/css" href="css/navbar.css"/>
<link rel="stylesheet" type="text/css" href="css/viz.css"/>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.7.2/jquery.min.js"></script>
<script src="scripts/d3.js"></script>
<script src="scripts/layout.cloud.js"></script>
<script src="scripts/tagcanvas.js"></script>
<title>
<% ImageResults results = (ImageResults) request.getAttribute("results"); %>
</title>
<script>
$(document).ready(function() {
	searchTime = "<% out.print(Math.ceil(results.getTimeElapsed()) / 1000000000); %>";
	
	if (parseInt("<%out.print(results.getNumResults());%>") == 0) {
		document.getElementById("loadingtime").innerHTML = "";
	} else {
		time = parseInt("<%out.print(results.getNumResults());%>")
				+ " results in <b>"
				+ searchTime + " second(s)</b>";
		
		document.getElementById("loadingtime").innerHTML = time;
	}
});

function submitImgQuery () {
	if (document.getElementById("imgQuery").value != "") {
		document.forms["imgSearchForm"].submit();
	}
}
</script>
</head>

<body id="resultsPage">
	<div id="headerWithSearchBar">
		<div id="searchBar" style="margin-left: 10px; margin-bottom: 10px; float: left; width: 60%">
			<h3>
				<a href="../Bahdit" style="text-decoration: none; color: black;">Bahdit</a>
			</h3>

			<form id="imgSearchForm" class="resultsPageForm" id="queryField" action="ProcessQuery" method="get">
				<input type="text" id="imgQuery" class="text" name="imgQuery" value="<%out.print(results.getQuery());%>"/>
				<input type="hidden" name="page" value="1"/>
				<input type="hidden" name="searchType" value="image"/>
				<input type="hidden" name="similar" value="0"/>
				<input type="button" class="submit" value="Search" onclick="submitImgQuery();"/>
			</form>
		</div>
		
		<div id="loadingtime" style="position: relative; top: 70%; float: right; margin-left: 20px; width: 20%;"></div>
	</div>

	<div id="imageResults">
		<div id="resultsFrame">
			<% out.print(results.getResult()); %>
		</div>

		<div id="imgBottomToolbar">
			<div class="pageChanger"></div>
		</div>
	</div>
</body>
</html>