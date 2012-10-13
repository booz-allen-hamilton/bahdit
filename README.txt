Version 1.0 - "Bahdit"

Requirements:
- Apache Tomcat
- Eclipse Web Tools Platform Plugin
- Hadoop
- Zookeeper
- Accumulo
- StartTheCloud.sh

Instructions:

Setup:
- navigate to where the shell script "StartTheCloud.sh" is stored
- type "./StartTheCloud.sh" in the command line
- if asked to re-format, type "Y"
- when asked for instance name, type "bah"
- when asked for password, type "bah"
- to change the number of iterations, edit "NUM_ITERATIONS" in "properties.conf"

Complete Setup:
- Move these files into the AppleFoxSearch/src/WebContent
  - hashSmp.sample
  - PageRank.pr
  - sampler.sample
  - tagSmp.sample
- Run AppleFoxSearch as a Tomcat server
