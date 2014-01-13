maggiefs-hadoop
===============

Hadoop bindings for maggiefs


To build
==

If you have maven installed, building should be as simple as 

    mvn package

from the project root.  If you get errors involving "asm-3.1.jar is an invalid zip file", run ./getAsm.sh.



To install
==

Copy the jar from target/ to 

* $HADOOP_HOME/lib
* if applicable, $HBASE_HOME/lib

Then add the following lines to both core-site.xml and hbase-site.xml:

```xml
  <property>
    <name>fs.default.name</name>
    <value>mfs://localhost:1103/</value>
  </property>


  <property>
    <name>fs.mfs.impl</name>
    <value>org.maggiefs.hadoop.MaggieFileSystem</value>
    <description>The FileSystem for mfs: uris.</description>
  </property>
```


