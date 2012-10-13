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

/*------------------------- Animated Page Rank Graph -------------------------*/
function createPageRankGraph() {
	initGraph();
	function initGraph(){
		var fd = new $jit.ForceDirected({

			injectInto : 'pageRankGraph',  // DIV for the visualization.
			Navigation : {				   // Allow zooming and moving.
				enable : true,
				type : 'Native',
				panning : 'avoid nodes',
				zooming : 10
			},

			Node : {
				overridable : true,
				dim : 7
			},
			
			Edge : {
				overridable : true,
				color : '#23A4FF',
				lineWidth : 0.4
			},

			// Tooltip that pops up when hovering over the nodes.
			Tips : {
				enable : true,
				type : 'Native',
				offsetX : 10,
				offsetY : 10,
				onShow : function(tip, node) {
					tip.innerHTML = node.name;
				}
			},

			Events : {
				enable : true,
				type : 'Native',
				onClick: function(node, eventInfo, e) {
					if (node.id != null){
						window.location = node.id;
					}
				},
				
				// Change cursor style and highlight the edges when hovering 
				// over node.
				onMouseEnter : function(node, eventInfo, e) {
					fd.canvas.getElement().style.cursor = 'move';
					
					//set final styles
					fd.graph.eachNode(function(n) {
						if(n.id != node.id) delete n.selected;
						n.eachAdjacency(function(adj) {
							adj.setDataset('end', {
								lineWidth: 0.4,
								color: '#23a4ff'
							});
						});
					});
					
					if(!node.selected) {
						node.selected = true;
						node.eachAdjacency(function(adj) {
							adj.setDataset('end', {
								lineWidth: 3,
								color: '#36acfb'
							});
						});
					} else {
						delete node.selected;
					}
					
					//trigger animation to final styles
					fd.fx.animate({
						modes: ['node-property:dim',
						        'edge-property:lineWidth:color'],
						        duration: 500
					});
					
					// Build the right column relations list.
					// This is done by traversing the clicked node connections.
					var html = "<h4>" + node.name + "</h4><b> connections:</b><ul><li>",
					list = [];
					node.eachAdjacency(function(adj){
						if(adj.getData('alpha')) list.push(adj.nodeTo.name);
					});					
				},
				
				onMouseLeave : function(node, eventInfo, e) {
					fd.canvas.getElement().style.cursor = '';
				},
				
				//Update node positions when dragged
				onDragMove : function(node, eventInfo, e) {
					var pos = eventInfo.getPos();
					node.pos.setc(pos.x, pos.y);
					fd.plot();
				},
				
				//Implement the same handler for touchscreens
				onTouchMove : function(node, eventInfo, e) {
					$jit.util.event.stop(e); //stop default touchmove event
					this.onDragMove(node, eventInfo, e);
				}
			},
			
			// Number of iterations for the FD algorithm
			iterations : 4,
			
			// Edge length
			levelDistance : 510,
			
			// Change node styles when DOM labels are placed
			// or moved.
			onPlaceLabel : function(domElement, node) {
				var style = domElement.style;
				var left = parseInt(style.left);
				var top = parseInt(style.top);
				var w = domElement.offsetWidth;
				style.left = (left - w / 2) + 'px';
				style.top = (top + 10) + 'px';
				style.display = '';
			}
		});
		
		// Load JSON data.
		fd.loadJSON(pageRankGraphJson);
		
		// Compute positions incrementally and animate.
		fd.computeIncremental({
			iter : 50,
			property : 'end',
			onComplete : function() {
				fd.animate({
					modes : [ 'linear' ],
					transition : $jit.Trans.Elastic.easeOut,
					duration : 1000
				});
			}
		});
	}
}