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
<%@ page import="com.afspq.model.Results"%>
<!doctype html>
<html>
<head>
<meta charset="UTF-8">
<link href="images/search-icon.png" rel="icon" type="image/x-icon"/>
<link rel="stylesheet" type="text/css" href="css/style.css"/>
<link rel="stylesheet" type="text/css" href="css/resultsPage.css"/>
<link rel="stylesheet" type="text/css" href="css/navbar.css"/>
<link rel="stylesheet" type="text/css" href="css/viz.css"/>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.7.2/jquery.min.js"></script>
<script src="scripts/jit.js"></script>
<script src="scripts/tagcanvas.js"></script>
<title>
<%
	Results results = (Results) request.getAttribute("results");
	String query = results.getQuery();
	out.print(query + " - Bahdit");
%>
</title>
<script>
pageRankGraphJson = <% out.print(results.nodesArr); %>;
keywordsTreeJson  = <% out.print(results.keywordsTreeJson); %>;

window.onresize = function () {
	windowHeight = window.innerHeight - 110;
	$("#tagCloud").css("height", windowHeight +"px");
	document.getElementById("tagCanvas").height = windowHeight;
	document.getElementById("tagCanvas").width  = $("#tagCloud").width();
};

$(document).ready(function() {
	$("#tagCanvas").hide();
	$("#keywordsTree").hide();
	$("#pageRankGraph").hide();
	
	windowHeight = window.innerHeight - 110;
	$("#tagCloud").css("height", windowHeight +"px");
	document.getElementById("tagCanvas").height = windowHeight;
	document.getElementById("tagCanvas").width  = $("#tagCloud").width();
	
	searchTime   = "<% out.print(Math.ceil(results.getTimeElapsed()) / 1000000000); %>";
	maxPages     = parseInt("<% out.print(results.getNumPages()); %>");
	pageNumber   = parseInt((document.URL).match(/page=[0-9]+/)[0].split("=")[1]);
	formInput    = "<input type='hidden' name='page' value='" + pageNumber + "'/>";
	numOfResults = parseInt("<%out.print(results.getNumResults());%>");
});
</script>
</head>

<body>
	<div id="headerWithSearchBar">
		<div id="searchBar" style="margin-left: 10px; margin-bottom: 10px; float: left; width: 60%">
			<h3>
				<a href="../Bahdit" style="text-decoration: none; color: black;">Bahdit</a>
			</h3>

			<form class="resultsPageForm" id="queryField" action="ProcessQuery" method="get">
				<input type="text" class="text" name="query" value="<%out.print(query);%>" />
				<input type="hidden" name="searchType" value="web"/>
				<input type="hidden" name="page" value="1" />
				<input type="button" class="submit" onclick="submitForm()" value="Search"/>
			</form>
		</div>
		
		<div id="loadingtime" style="position: relative; top: 70%; float: right; margin-left: 20px; width: 20%;"></div>
		
		<div id="vizTabOne" class='tab' style="top: 142px;">
			Keyword Cloud
		</div>
		
		<div id="vizTabTwo" class='tab' style="top: 190px;">
			Keyword Space Tree
		</div>
		
		<div id="vizTabThree" class='tab' style="top: 238px;">
			Page Rank Graph
		</div>
	</div>

	<div id="results">
		<div id="resultsFrame">
			<% out.print(results.getResult()); %>
		</div>

		<div id="bottomToolbar">
			<div class="pageChanger"></div>
		</div>
	</div>

	<div id="tagCloud" style="overflow: auto;">
		<div id="canvasDiv">
			<canvas id="tagCanvas" width=740px height=660px></canvas>
			<div id="keywordsTree" style="overflow: auto;">
				<div id="radioButtons" style="width: 100% ;text-align: center;">
					<b>Orientation: </b>&nbsp;&nbsp;
					<span class="radioButton">
						Top
						<input type="radio" id="r-top" name="orientation" value="top"/>
					</span>&nbsp;&nbsp;
					<span class="radioButton">
						Bottom
						<input type="radio" id="r-bottom" name="orientation" value="bottom"/>
					</span>&nbsp;&nbsp;
					<span class="radioButton">
						Left
						<input type="radio" id="r-left" name="orientation" checked="checked" value="left"/>
					</span>&nbsp;&nbsp;
					<span class="radioButton">
						Right
						<input type="radio" id="r-right" name="orientation" value="right"/>
					</span>
				</div>
			</div>
			<div id="pageRankGraph" style="width: 100%; height: 100%"></div>
		</div>
	</div>
	<div id="tags" style="display: none;">
		<% out.print(results.getTagCloudWords()); %>
	</div>
<script src="scripts/pageChanger.js"></script>
<script src="scripts/keywordsCloud.js"></script>
<script src="scripts/vizControl.js"></script>
<script src="scripts/keywordsSpaceTree.js"></script>
<script src="scripts/pageRankGraph.js"></script>
</body>
</html>