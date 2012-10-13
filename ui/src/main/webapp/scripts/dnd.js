/**
 * Copyright (c) 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Author: Eric Bidelman <e.bidelman@chromium.org>
**/

//Simple JavaScript Templating
//John Resig - http://ejohn.org/ - MIT Licensed
(function(){
	var cache = {};

	this.tmpl = function tmpl(str, data){
		// Figure out if we're getting a template, or if we need to
		// load the template - and be sure to cache the result.
		var fn = !/\W/.test(str) ?
				cache[str] = cache[str] ||
				tmpl(document.getElementById(str).innerHTML) :

					// Generate a reusable function that will serve as a template
					// generator (and which will be cached).
					new Function("obj",
							"var p=[],print=function(){p.push.apply(p,arguments);};" +

							// Introduce the data as local variables using with(){}
							"with(obj){p.push('" +

							// Convert the template into pure JavaScript
							str
							.replace(/[\r\t\n]/g, " ")
							.split("<%").join("\t")
							.replace(/((^|%>)[^\t]*)'/g, "$1\r")
							.replace(/\t=(.*?)%>/g, "',$1,'")
							.split("\t").join("');")
							.split("%>").join("p.push('")
							.split("\r").join("\\'")
							+ "');}return p.join('');");

				// Provide some basic currying to the user
				return data ? fn( data ) : fn;
	};
})();

Element.prototype.hasClassName = function(name) {
	return new RegExp("(?:^|\\s+)" + name + "(?:\\s+|$)").test(this.className);
};

Element.prototype.addClassName = function(name) {
	if (!this.hasClassName(name)) {
		var c = this.className;
		this.className = c ? [c, name].join(' ') : name;
	}
};

Element.prototype.removeClassName = function(name) {
	if (this.hasClassName(name)) {
		var c = this.className;
		this.className = c.replace(
				new RegExp("(?:^|\\s+)" + name + "(?:\\s+|$)", "g"), "");
	}
};


//insertAdjacentHTML(), insertAdjacentText() and insertAdjacentElement 
//for Netscape 6/Mozilla by Thor Larholm me@jscript.dk 
if (typeof HTMLElement != "undefined" && !HTMLElement.prototype.insertAdjacentElement) {
	HTMLElement.prototype.insertAdjacentElement = function (where, parsedNode) {
		switch (where) {
		case 'beforeBegin':
			this.parentNode.insertBefore(parsedNode, this)
			break;
		case 'afterBegin':
			this.insertBefore(parsedNode, this.firstChild);
			break;
		case 'beforeEnd':
			this.appendChild(parsedNode);
			break;
		case 'afterEnd':
			if (this.nextSibling) this.parentNode.insertBefore(parsedNode, this.nextSibling);
			else this.parentNode.appendChild(parsedNode);
			break;
		}
	}

	HTMLElement.prototype.insertAdjacentHTML = function (where, htmlStr) {
		var r = this.ownerDocument.createRange();
		r.setStartBefore(this);
		var parsedHTML = r.createContextualFragment(htmlStr);
		this.insertAdjacentElement(where, parsedHTML)
	}


	HTMLElement.prototype.insertAdjacentText = function (where, txtStr) {
		var parsedText = document.createTextNode(txtStr)
		this.insertAdjacentElement(where, parsedText)
	}
}


function onDragEnter(e) {
	e.stopPropagation();
	e.preventDefault();
}

function onDragOver(e) {
	e.stopPropagation();
	e.preventDefault();
	dropbox.addClassName('rounded');
}

function onDragLeave(e) {
	e.stopPropagation();
	e.preventDefault();
	dropbox.removeClassName('rounded');
}

function onDrop(e) {
	e.stopPropagation();
	e.preventDefault();
	dropbox.removeClassName('rounded');

	var readFileSize = 0;
	var files = e.dataTransfer.files;
	image = files[0];
	
	// Loop through list of files user dropped.
	for (var i = 0, file; file = files[i]; i++) {
		readFileSize += file.fileSize;

		// Only process image files.
		var imageType = /image.*/;
		if (!file.type.match(imageType)) {
			continue;
		}

		var reader = new FileReader();

		reader.onerror = function(e) {
			alert('Error code: ' + e.target.error.code);
		};

		// Create a closure to capture the file information.
		reader.onload = (function(aFile) {
			return function(evt) {
				// Generate angle between -30 and 30 degrees.
				var deg = Math.floor(Math.random() * 31);
				deg = Math.floor(Math.random() * 2) ? deg : -deg;

				var data = {
						'file': {
							'name': aFile.name,
							'src': evt.target.result,
							'fileSize': aFile.fileSize,
							'type': aFile.type,
							'rotate': deg
						}
				};
				
				addUploadButton();

				// Render thumbnail template with the file info (data object).
				document.getElementById('thumbnails').innerHTML = tmpl('thumbnail_template', data);
			};
		})(file);
		
		// Read in the image file as a data url.
		reader.readAsDataURL(file);
	}
	
	return false;
}

//Remember some global settings.
var totalFileSize = 0;
var dropbox = document.getElementById('dropZone');

//Setup drag and drop handlers.
dropbox.addEventListener('dragenter', onDragEnter, false);
dropbox.addEventListener('dragover', onDragOver, false);
dropbox.addEventListener('dragleave', onDragLeave, false);
dropbox.addEventListener('drop', onDrop, false);