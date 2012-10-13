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
/* This visuazlization was created by using The JavaScript InfoVis Toolkit.
 * For documentation go to: 
 * http://thejit.org/static/v20/Docs/files/Core/Core-js.html
 */

/*----------------------- Animated Keywords Space Tree -----------------------*/

// Only need to initialize once per page load.
treeInitialized = false;

function createKeywordTree() {
	if (!treeInitialized) {
		initTree();
		
		function initTree() {
			treeInitialized = true;
			
			$jit.ST.Plot.NodeTypes.implement({
				'stroke-rect': {  
					'render': function(node, canvas) {  
						var width = node.getData('width'),  
						height = node.getData('height'),  
						pos = this.getAlignedPos(node.pos.getc(true), width, height),  
						posX = pos.x + width/2,  
						posY = pos.y + height/2;  
						this.nodeHelper.rectangle.render('fill', {x: posX, y: posY}, width, height, canvas);  
						this.nodeHelper.rectangle.render('stroke', {x: posX, y: posY}, width, height, canvas);  
					}
				}
			});

			var st = new $jit.ST({
				injectInto: 'keywordsTree',				// ID of canvas container
				duration: 400,							// Animation speed
				transition: $jit.Trans.Quart.easeInOut,	// Set animation transition type
				levelDistance: 60,						// Distance between node and its children
				siblingOffset: 15,						// Top and bottom gap between nodes

				// Enable panning/zooming/moving
				Navigation: {
					enable:true,
					panning:true
				},

				Tips: {  
					enable: true,  
					type: 'HTML',
					offsetX: 10, 
					offsetY: 10,  
					onShow: function(tip, node) {
						if (/^k[0-9]+$/.test(node.id)) {
							tip.innerHTML = node.name;
						} else {
							tip.innerHTML = node.id;
							return
						};
					}
				},

				Node: {
					height: 30,
					width: 120,
					type: 'stroke-rect',
					overridable: true,
					CanvasStyles:{
						fillStyle: '#E6E6E6',
						strokeStyle: '#000000',
						lineWidth: 2
					}
				},

				Edge: {
					type: 'bezier',
					color: '#A9E2F3',
					lineWidth: 2,
					overridable: true
				}, 

				onCreateLabel: function(label, node){
					label.id = node.id;            
					label.innerHTML = node.name;
					label.onclick = function() {  
						st.onClick(node.id, {
							onComplete: function() {
								if (/^k[0-9]+$/.test(node.id)) {
									window.location = "ProcessQuery?query=" + node.name + "&searchType=web&page=1";
								}
							}
						});
					};

					// Set label styles
					var style 		 = label.style;
					style.width 	 = 120 + 'px';
					style.height 	 = 30 + 'px';            
					style.cursor 	 = 'pointer';
					style.color 	 = '#333';
					style.fontSize   = '0.7em';
					style.textAlign  = 'center';
					style.paddingTop = '3px';
				},

				onBeforePlotNode: function(node){
					if (node.selected) {
						node.data.$color = "#F5F6CE";
					}
				},

				onBeforePlotLine: function(adj){
					if (adj.nodeFrom.selected && adj.nodeTo.selected) {
						adj.data.$color 	= "#eed";
						adj.data.$lineWidth = 3;
					} else {
						//delete adj.data.$color;
						delete adj.data.$lineWidth;
					}
				}
			});

			st.loadJSON(keywordsTreeJson);	// Load json data
			st.compute();					// Compute node positions and layout

			// Optional: make a translation of the tree
			st.geom.translate(new $jit.Complex(-200, 0), "current");
			
			// Emulate a click on the root node.
			st.onClick(st.root);

			// Add event handlers to switch spacetree orientation.
			var top = $jit.id('r-top'), 
			left    = $jit.id('r-left'), 
			bottom  = $jit.id('r-bottom'), 
			right   = $jit.id('r-right');

			function changeHandler() {
				if(this.checked) {
					top.disabled = bottom.disabled = right.disabled = left.disabled = true;
					st.switchPosition(this.value, "animate", {
						onComplete: function(){
							top.disabled = bottom.disabled = right.disabled = left.disabled = false;
						}
					});
				}
			};

			top.onchange = left.onchange = bottom.onchange = right.onchange = changeHandler;
		}
	}
}