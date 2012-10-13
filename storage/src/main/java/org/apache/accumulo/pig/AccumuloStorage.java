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
package org.apache.accumulo.pig;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.bah.bahdit.main.plugins.fulltextindex.FullTextIndex;
import com.bah.bahdit.main.plugins.fulltextindex.utils.Utils;
import com.bah.bahdit.main.search.Search;



/**
 * 
 * A LoadFunc Class for Apache Pig to store and retrieve results from Accumulo
 * This class has been customized for Bahdit. No support for using querys.
 * 
 * In order to support queries, the pig calculator must mimic rank calculator used in bahdit
 * 
 */
@SuppressWarnings("rawtypes")
public class AccumuloStorage extends LoadFunc implements StoreFuncInterface {

	// Names of the properties in the property file
	public static final String INTABLE = "INTABLE";
	public static final String OUTTABLE = "OUTTABLE";

	private static final Log LOG = LogFactory.getLog(AccumuloStorage.class);

	private Configuration conf;
	private RecordReader<Key, Value> reader;
	private RecordWriter<Text, Mutation> writer;
	private HashMap<String, Double> pagerankTable;

	String inst;
	String zookeepers;
	String user;
	String password;
	String table;
	String query;
	String config;
	Text tableName;
	String auths;
	Authorizations authorizations;
	List<Pair<Text, Text>> columnFamilyColumnQualifierPairs = new LinkedList<Pair<Text,Text>>();

	String start = null;
	String end = null;

	int maxWriteThreads = 10;
	long maxMutationBufferSize = 10*1000*1000;
	int maxLatency = 10*1000;
	private int tupleSize;

	public AccumuloStorage(){}

	/* LoadFunc Methods */

	/**
	 * Gets the next key/value pair from accumulo and places them into a tuple
	 * 
	 * returns the created tuple
	 */
	@Override
	public Tuple getNext() throws IOException
	{
		try
		{        
			if (!reader.nextKeyValue())
				return null;

			Key key = (Key)reader.getCurrentKey();
			Value value = (Value)reader.getCurrentValue();
			assert key != null && value != null;
			return getTuple(key, value);
		}
		catch (InterruptedException e)
		{
			throw new IOException(e.getMessage());
		}
	}

	/**
	 * Takes a key value pair and turns it into a tuple that pig understands
	 */
	@SuppressWarnings("unchecked")
	protected Tuple getTuple(Key key, Value value) throws IOException {

		Tuple tuple = TupleFactory.getInstance().newTuple(4);
		if(query == null){
			ArrayList<String> cells = new ArrayList<String>();
			
			if(key.getRow() != null && !key.getRow().toString().equals(""))
				cells.add(key.getRow().toString());
			
			if(key.getColumnFamily() != null && !key.getColumnFamily().toString().equals(""))
				cells.add(key.getColumnFamily().toString());
			
			if(key.getColumnQualifier() != null && !key.getColumnQualifier().toString().equals(""))
				cells.add(key.getColumnQualifier().toString());
			
			if(key.getColumnVisibility() != null && !key.getColumnVisibility().toString().equals(""))
				cells.add(key.getColumnVisibility().toString());
			
			if(value != null &&  !new String(value.get()).equals(""))
				cells.add( new String(value.get()));
			
			for(int i = 0; i < tupleSize; i++){
				tuple.set(i, cells.get(i));
			}
		} else {
			try{
				tuple.set(0, key.getColumnFamily().toString());

				byte[] bT = key.getColumnQualifier().toString().getBytes(FullTextIndex.ENCODING);
				HashMap<String, Double> words = (HashMap<String, Double>)Utils.deserialize(bT);
				int size = words.size();
				Tuple wordsTuple = TupleFactory.getInstance().newTuple(size);
				Iterator it = words.entrySet().iterator();
				int i = 0;
				while (it.hasNext()) {
					Tuple word = TupleFactory.getInstance().newTuple(2);
					Map.Entry pairs = (Map.Entry)it.next();
					word.set(0, pairs.getKey());
					word.set(1, pairs.getValue());
					wordsTuple.set(i++, word);
					it.remove(); 
				}

				tuple.set(1, wordsTuple);
				tuple.set(2, ((Double)StorageUtils.deserialize(value.get())));
				tuple.set(3, pagerankTable.get(key.getColumnFamily().toString()));	

			} catch (ClassNotFoundException e) {
				LOG.warn(e.getMessage());
			}
		}

		return tuple; 
	}

	@Override
	public InputFormat getInputFormat()
	{
		return new AccumuloInputFormat();
	}

	/**
	 * Sets the reader object
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
	{
		this.reader = reader;
	}

	protected RecordWriter<Text, Mutation> getWriter() {
		return writer;
	}

	/**
	 * Takes input and from the pig statement, 
	 * parses it and then sets up the MapReduce job
	 * 
	 * Example inputs:
	 * 
	 * # The following input uses a table named table_name and uses the properties.conf 
	 * # to connect to zookeeper. The first 4 things that are not null or empty string
	 * # will be placed into the tuple.
	 * 
	 * table_name CONFIG properties.conf AS 4
	 * 
	 * # Another example (WITH QUERY not supported)
	 * 
	 * table_name CONFIG properties.conf WITH QUERY booz AS 4
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void setLocation(String location, Job job) throws IOException {
		
		String[] tmp = location.split(" CONFIG ");
		table = tmp[0].trim();
		String[] tmp2 = tmp[1].trim().split(" WITH QUERY ");
		if(tmp2.length > 1){
			config = tmp2[0].trim();
			String[] tmp3 = tmp2[1].split(" AS ");
			query = tmp3[0].trim();
			tupleSize = Integer.parseInt(tmp3[1].trim());
		} else {
			String[] tmp3 = tmp2[0].split(" AS ");
			config = tmp3[0].trim();
			tupleSize = Integer.parseInt(tmp3[1].trim());
		}
		

		Properties properties = new Properties();
		properties.load(new FileInputStream(new File(config)));

		job.setInputFormatClass(AccumuloInputFormat.class);	
		conf = job.getConfiguration();

		inst = properties.getProperty(Search.INSTANCE_NAME);
		user = properties.getProperty(Search.USERNAME);
		password = properties.getProperty(Search.PASSWORD);
		zookeepers = properties.getProperty(Search.ZK_SERVERS);
		authorizations = new Authorizations();


		if(!conf.getBoolean(AccumuloInputFormat.class.getSimpleName()+".configured", false))
		{
			AccumuloInputFormat.setInputInfo(conf, user, password.getBytes(), table, authorizations);
			AccumuloInputFormat.setZooKeeperInstance(conf, inst, zookeepers);

			if(query != null){
				start = query + " " + Long.MIN_VALUE;
				end = query + " " + Long.MAX_VALUE;

				InputStream sample = new FileInputStream(new File(properties.getProperty(FullTextIndex.FT_SAMPLE)));
				InputStream samplebuffer = new BufferedInputStream( sample );
				ObjectInput objectsample = new ObjectInputStream ( samplebuffer );

				InputStream pagerank = new FileInputStream(new File(properties.getProperty(FullTextIndex.PR_FILE)));
				InputStream pagerankbuffer = new BufferedInputStream( pagerank );
				ObjectInput objectpagerank = new ObjectInputStream ( pagerankbuffer );


				HashMap<String, Integer> sampleTable = null;
				try {
					sampleTable = (HashMap<String, Integer>)objectsample.readObject();
					pagerankTable = (HashMap<String, Double>)objectpagerank.readObject();
				} catch (ClassNotFoundException e1) {
					e1.printStackTrace();
				}

				Map<String,String> iteratorProperties = new HashMap<String,String>();
				iteratorProperties.put(Search.QUERY, query);
				iteratorProperties.put(Search.MAX_NGRAMS, properties.getProperty(Search.MAX_NGRAMS));

				// get pagerank table for pagerank calculations
				String pagerankTableString = null;
				try {
					pagerankTableString = new String(Utils.serialize(pagerankTable), FullTextIndex.ENCODING);
				} catch (Exception e) {
					LOG.error(e.getMessage());
				}

				iteratorProperties.put(Search.PAGERANK_TABLE, pagerankTableString);

				// put the sampling table into properties for tf-idf calculations
				String sampleTableString = null;
				try {
					sampleTableString = new String(StorageUtils.serialize(sampleTable), FullTextIndex.ENCODING);
				} catch (Exception e) {
					LOG.error(e.getMessage());
				}

				iteratorProperties.put(FullTextIndex.FT_SAMPLE, sampleTableString);

				// this iterator calculates the cosim of each document
				IteratorSetting cfg = new IteratorSetting(10, PigCalculator.class, iteratorProperties);
				AccumuloInputFormat.addIterator(conf, cfg);
				AccumuloInputFormat.setRanges(conf, Collections.singleton(new Range(start, end)));
			} 
		}
	}

	/* StoreFunc methods */

	public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException
	{
		return relativeToAbsolutePath(location, curDir);
	}

	/**
	 * Sets the store location by taking inputs
	 * 
	 * Example Inputs:
	 * 
	 * # this stores all the tuples into a table called table_name by 
	 * # connecting to zookeeper by using the config file named properties.conf
	 * 
	 * table_name CONFIG properties.conf
	 */
	public void setStoreLocation(String location, Job job) throws IOException
	{
		conf = job.getConfiguration();

		String[] tmp = location.split(" CONFIG ");
		table = tmp[0].trim();
		config = tmp[1].trim();

		Properties properties = new Properties();
		properties.load(new FileInputStream(new File(config)));

		inst = properties.getProperty(Search.INSTANCE_NAME);
		user = properties.getProperty(Search.USERNAME);
		password = properties.getProperty(Search.PASSWORD);
		zookeepers = properties.getProperty(Search.ZK_SERVERS);
		authorizations = new Authorizations();

		if(!conf.getBoolean(AccumuloOutputFormat.class.getSimpleName()+".configured", false))
		{
			AccumuloOutputFormat.setOutputInfo(conf, user, password.getBytes(), true, table);
			AccumuloOutputFormat.setZooKeeperInstance(conf, inst, zookeepers);
			AccumuloOutputFormat.setMaxLatency(conf, maxLatency);
			AccumuloOutputFormat.setMaxMutationBufferSize(conf, maxMutationBufferSize);
			AccumuloOutputFormat.setMaxWriteThreads(conf, maxWriteThreads);
		}
	}

	public OutputFormat getOutputFormat()
	{
		return new AccumuloOutputFormat();
	}

	@SuppressWarnings("unchecked")
	public void prepareToWrite(RecordWriter writer)
	{
		this.writer = writer;
	}

	/**
	 * Takes a tuple and turns it into a mutation to write out to a table
	 */
	public Collection<Mutation> getMutations(Tuple tuple) throws ExecException, IOException {
		Mutation mut = new Mutation(StorageUtils.objToText(tuple.get(0)));
		Text cf = StorageUtils.objToText(tuple.get(1));
		Text cq = StorageUtils.objToText(tuple.get(2));

		if(tuple.size() > 4)
		{
			Text cv = StorageUtils.objToText(tuple.get(3));
			Value val = new Value(StorageUtils.objToBytes(tuple.get(4)));
			if(cv.getLength() == 0)
			{
				mut.put(cf, cq, val);
			}
			else
			{
				mut.put(cf, cq, new ColumnVisibility(cv), val);
			}
		}
		else
		{
			Value val = new Value(StorageUtils.objToBytes(tuple.get(3)));
			mut.put(cf, cq, val);
		}

		return Collections.singleton(mut);
	}

	public void putNext(Tuple tuple) throws ExecException, IOException
	{
		Collection<Mutation> muts = getMutations(tuple);
		for(Mutation mut : muts)
		{
			try {
				getWriter().write(tableName, mut);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	@Override
	public String relativeToAbsolutePath(String location, Path curDir) throws IOException { return location; }
	
	// UNUSED METHODS
	public void cleanupOnFailure(String failure, Job job) { }

	public void setStoreFuncUDFContextSignature(String signature) { }

	public void checkSchema(ResourceSchema schema) throws IOException { }

}
