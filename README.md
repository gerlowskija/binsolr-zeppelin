# binsolr-zeppelin-tool

A Solr [plugin](https://lucene.apache.org/solr/guide/solr-plugins.html) that (in intent) provides an Apache Zeppelin integration into Solr's "bin/solr" CLI.

This integration doesn't currently work, because of two related classloading issues:

1. SolrCLI, the core class that runs Tool implementations, only looks at Solr's "root" classloader for Tool classes.
2. Plugin jars are loaded by CoreContainer, which implies a lot of machinery that the relatively minimal SolrCLI does not currently set up.

The packaging found here does work and allows the plugin to be installed- it just can't be invoked once installed.

## Installation

After checking out this repository, run the following steps to install the plugin
1. Run a python fileserver to serve the plugin repository: `cd repo && python3 -m http.server`
3. Start Solr with packages enabled: `bin/solr start -c -Denable.packages=true`
4. Add the repo to Solr's tracking: `bin/solr package add-repo zeppelin-tool "http://localhost:8000"`
5. Install the package in Solr: `bin/solr package install zeppelin-tool`
