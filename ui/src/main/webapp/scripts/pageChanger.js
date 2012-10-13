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
/*---------------- Functions to change the search result page. ---------------*/
$(document).ready(function () {
	// Add the current page number to search query form.
	$("#queryField").append(formInput);

	if (numOfResults == 0) {
		document.getElementById("loadingtime").innerHTML = "";
	} else {
		time = parseInt(numOfResults)
		+ " results in <b>"
		+ searchTime + " second(s)</b>";

		document.getElementById("loadingtime").innerHTML = time;
	}

	space = "&nbsp;&nbsp;&nbsp;&nbsp;";
	prevButton = "<span id='previous' onclick='goBack()'>Previous</span>" + space;
	nextButton = space + "<span id='next' onclick='goNext()'>Next</span>";

	if (pageNumber != 1 && maxPages != 0) {
		$(".pageChanger").append(prevButton);
	}

	var i = 1;
	var max = maxPages;

	if (pageNumber <= 6) {
		i = 1;
		if (maxPages >= 10)
			max = 10;
		else
			max = maxPages;
	}

	else if (pageNumber >= 6 && pageNumber <= maxPages - 4) {
		i = pageNumber - 5;
		max = pageNumber + 4;
	} else {

		if (maxPages >= 10)
			i = maxPages - 10;
		else
			i = 1;
		max = maxPages;
	}

	while (i <= max && maxPages > 1) {
		link = (document.URL).replace(/&page=[0-9]+/, "&page=" + i);

		if (i == pageNumber) {
			$(".pageChanger").append(
					"<a style='text-align:center; padding: 1px; border:1px solid;"
					+ " text-decoration:none;' href='"
					+ link + "'>" + i + "</a>&nbsp;&nbsp;"
			);
		} else {
			$(".pageChanger").append(
					"<a href='" + link + "'>" + i + "</a>&nbsp;&nbsp;"
			);
		}
		i++;
	}

	if (pageNumber != maxPages && maxPages > 1) {
		$(".pageChanger").append(nextButton);
	}
});

function goBack() {
	query = (document.URL).match(/\?query=[\w\+]+/)[0].split("=")[1];

	if (pageNumber > 1) {
		pageNumber -= 1;
		window.location = (document.URL).replace(
				/&page=[0-9]+/, "&page=" + pageNumber
		);
	}
}

function goNext() {
	pageNumber = parseInt((document.URL).match(/page=[0-9]+/)[0].split("=")[1]);
	query = (document.URL).match(/\?query=[\w\+]+/)[0].split("=")[1];

	if (pageNumber < maxPages) {
		pageNumber += 1;
		window.location = (document.URL).replace(
				/&page=[0-9]+/, "&page=" + pageNumber
		);
	}
}

function submitForm() {
	if ($(".text").val() != "") {
		document.forms["queryField"].submit();
	}
}