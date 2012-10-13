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
/*------------------------ Visualization Control -------------------------*/
$('.tab').mouseover(function() {
	if ($(this).attr("id") == "vizTabOne") {
		extendTab("#vizTabOne");
	} else if ($(this).attr("id") == "vizTabTwo") {
		extendTab("#vizTabTwo");
	} else {
		extendTab("#vizTabThree");
	}
});

vizTabOneExpanded = false;
vizTabTwoExpanded = false;
vizTabThreeExpanded = false;

$('.tab').click(function() {
	$("#tagCloud").css("backgroundImage", "none");
	$("#tagCloud").css("opacity", "1");
	var div = "#canvasDiv";

	if ($(this).attr("id") == "vizTabOne") {
		var tab = "#vizTabOne";
		var canvas = "#tagCanvas";

		if (!vizTabOneExpanded) {
			vizTabOneExpanded = true;
			slideTab(tab);
			slideCanvas(div, canvas);
			$("#vizTabTwo").hide();
			$("#vizTabThree").hide();
		} else {
			vizTabOneExpanded = false;
			slideBackCanvas(div, canvas);
			slideBackTab(tab);
			$("#vizTabTwo").show();
			$("#vizTabThree").show();
		}

	} else if ($(this).attr("id") == "vizTabTwo") {
		var tab = "#vizTabTwo";
		var canvas = "#keywordsTree";

		if (!vizTabTwoExpanded) {
			vizTabTwoExpanded = true;
			slideTab(tab);
			slideCanvas(div, canvas);
			$("#vizTabOne").hide();
			$("#vizTabThree").hide();
			setTimeout("createKeywordTree()", 1000);
		} else {
			vizTabTwoExpanded = false;
			slideBackCanvas(div, canvas);
			slideBackTab(tab);
			$("#vizTabOne").show();
			$("#vizTabThree").show();
		}
	} else {
		var tab = "#vizTabThree";
		var canvas = "#pageRankGraph";

		if (!vizTabThreeExpanded) {
			vizTabThreeExpanded = true;
			slideTab(tab);
			slideCanvas(div, canvas);
			$("#vizTabOne").hide();
			$("#vizTabTwo").hide();
			setTimeout("createPageRankGraph()", 2000);
		} else {
			vizTabThreeExpanded = false;
			slideBackCanvas(div, canvas);
			slideBackTab(tab);
			$("#vizTabOne").show();
			$("#vizTabTwo").show();
		}
	}
});

function slideCanvas(div, canvas) {
	vizTabOneExpanded = true;
	$(div).animate({
		width: "100%"
	}, 200, function() {
		$(canvas).delay(100).fadeIn("slow");
	});
}

function slideBackCanvas(div, canvas) {
	vizTabOneExpanded = false;
	$(canvas).hide();
	$(div).animate({
		width: "0"
	}, 200);
	$("#tagCloud").css("backgroundImage", "url('images/scaled_bahdit_logo.png')");
	$("#tagCloud").css("opacity", "0.2");
}

function slideTab(tab) {
	$(tab).animate({
		left: "90.5%"
	}, 200);

	$(tab).css("border-right", "0");
	$(tab).css("border-bottom-right-radius", "0");
	$(tab).css("border-top-right-radius", "0");

	$(tab).css("border-left", "2px solid rgba(0,0,0, 0.4)");
	$(tab).css("border-bottom-left-radius", "4px");
	$(tab).css("border-top-left-radius", "4px");
	$(tab).css("background-position", "left center");
	$(tab).css("background-image", "url('images/arrow-left.png')");
}

function slideBackTab(tab) {
	$(tab).animate({
		left: "48%"
	}, 200);

	$(tab).css("border-left", "0");
	$(tab).css("border-bottom-left-radius", "0");
	$(tab).css("border-top-left-radius", "0");

	$(tab).css("border-right", "2px solid rgba(0,0,0, 0.4)");
	$(tab).css("border-bottom-right-radius", "4px");
	$(tab).css("border-top-right-radius", "4px");
	$(tab).css("background-position", "right center");
	$(tab).css("background-image", "url('images/arrow.png')");
}

function extendTab(tab) {
	$(tab).animate({
		width: "180px"
	}, 150, function() {
		$(tab).animate({
			width: "150px"
		});
	});
}