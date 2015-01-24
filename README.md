# HBase In Action
Migrate examples used throughout "HBase In Action" to HBase 0.98.

Project based on [Nick Dimiduk's TwitBase code](https://github.com/hbaseinaction/twitbase)
##Compiling
 To build the jar:

    $ mvn package

##Using the example:
 To run in the fully distributed cluster just copy the `hbase-site.xml` to `src/main/resources`
 directory before build the project.

 To run example classes:

    $ bin/launcher
