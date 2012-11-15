Overview
========

Bahdit is a search engine prototype developed at Booz Allen Hamilton
during the summer of 2012 by a team of interns. The goal of Bahdit
is to create a modular system of components to support online indexing
of data in a scalable way that is not currently realized in other
open source search solutions.

Bahdit uses Accumulo, a distributed key-value database, to store
the document indices. The initial version includes a map/reduce
based web crawling system that uses map/reduce primarily for parallel
job control and works partially outside of usual map/reduce paradigms.
This may eventually be retooled to better conform to map/reduce
paradigms and use something like Cascading for job control and coordination.

Eventually a storm/message queue based restful indexing api is planned
as well, which will mimic the Solr restful indexing api.

The actual index design is a hybrid term-document partitioned index,
which has become popular for many indexing systems based on Accumulo.
Essentially, a dictionary with term frequencies is used to find
the lowest frequency term in a user query. This is used to access
a term partitioned index, which itself contains a document-partitioned
index.

Details
=======

Bahdit has three main components: the map/reduce based web crawler and
indexing control system, the java search api, and the web search UI.

Build
-----

Build Bahdit by running the `mvn package` command. This will produce jars
in the various sub-project target directories that can be used to run
Bahdit.

Installation
------------

### Prerequisites

You must have Accumulo, Hadoop, and Zookeeper installed and functioning.

### Configuration

See properties.conf and further details below.

### Ingest

The Bahdit ingest application can, and should, be run as a user process. No
real installation steps are necessary.

### Web UI

The web UI can be run as a user process, as a system daemon, or in a web
application container.

To run as a user process, simply copy the war to your home directory
and include the properties.conf file in the classpath using the `-cp` option
for java. The war file is runnable, so you can use `java -jar` to run it.

To run as a system daemon, you will need to create an init script to to start
the application. Reference versions of how to do this may be found for
applications such as Gerrit and Hudson which use a similar deployment strategy.

Follow the usual war file deployment instructions for your application
container if you wish to deploy the application that way.

Running
-------

Currently there are no bootstrap shell scripts for Bahdit, as a result
you must construct your java command invocation by hand. Use java -jar.
Further details on constructing java classpaths, for instance, are outside
of the scope of this readme.

Bahdit requires a configuration file to be passed to the various utilities.
This can be passed to the web UI by including a file named properties.conf
in the root of the classpath. The ingest utilities accept the path to a
file as their first argument.

### Ingest Utilities

The ingest jar is an executable jar. Run it using `java -jar`. It includes
built in help documentation which can be accessed using the --help option.

### Web UI

The web UI is a runnable war file. You can run it using `java -jar` or
by uploading it to your favorite JavaEE application container.

Be sure to include a properties.conf file in the root of the application
classpath when running the web UI. This can be done from the command line
using the `-cp` option for java or in an application container by putting 
the file in WEB-INF/classes.

A reference properties.conf is included in the source code; the options are
fairly self-explainatory.
