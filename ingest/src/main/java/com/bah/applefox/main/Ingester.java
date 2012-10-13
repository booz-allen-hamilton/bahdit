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
package com.bah.applefox.main;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * The ingester class provides the guidelines for any sources of URLs (Eg. Web
 * Crawlers). It is utilized in the Google Guice Dependency Injection
 * implemented in the IngesterModule. It also extends Configured which implies
 * that any extensions of this class must be MapReduce Jobs
 * 
 */
public abstract class Ingester extends Configured implements Tool {

}
