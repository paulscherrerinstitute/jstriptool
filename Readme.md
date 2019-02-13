# Overview

__jstriptool__ is a reimplementation of StripTool in Java. 



# Requirements

 * Java 8. 



# Binaries

Releases can be downloaded from: https://github.com/paulscherrerinstitute/jstriptool/releases



# Building

Before being able to use gradle either comment the uploadArchives task in the build.gradle or create a ~/.gradle/gradle.properties file with following contents:
```
artifactoryUser=xxx
artifactoryPwd=xxx
artifactoryUrlRel=xxx
artifactoryUrlLibSnap=xxx
```

The JAR file jstriptool-<version>-fat.jar can be built executing:
 ```
 ./gradlew build
 ```  

After executing the build command, the file jstriptool-<version>-fat.jar is located in the folder  ./build/libs. 


## RPM
To build the RPM Java is required to be installed on your build machine (as the compilation of the Java code is not done inside the docker build container). 

To build the RPM, generate the fat jar first:
 ```
 ./gradlew clean build
 ```

Afterwards run the docker rpm build container as follows (for RHEL7):
```
docker run -it --rm -v ~/.ssh:/root/.ssh -v `pwd`:/data paulscherrerinstitute/centos_build_rpm:7 package jstriptool.spec
```

The resulting rpm will be placed in the `rpm` folder.

For SL6 use following command to build the RPM:

```
docker run -it --rm -v ~/.ssh:/root/.ssh -v `pwd`:/data paulscherrerinstitute/centos_build_rpm:6 package jstriptool.spec
```


# Launching

Launch the application typing:
 ```
 java -jar jstriptool-<version>-fat.jar <startup options...> <file name>
 ```  

 * If <file name> is provided, the plot window is opened.
 * If <file name> is not provided, the configuration window is opened.



# Startup Options

The most relevant options are:

 * `-config : Show configuration dialog too when filename is provided`
 * `-home=<dir> : Set home folder`
 * `-default=<dir> : Set default configuration file`
 * `-laf=<name> :  Set the look and feel: nimbus, metal, dark, system, or LAF class name`
 * `-aa=false : Disable anti-aliasing`

One can override epics configuration providing parameters in command line:

 * `-EPICS_CA_ADDR_LIST=<value>`
 * `-EPICS_CA_AUTO_ADDR_LIST=<value>`
 * `-EPICS_CA_CONN_TMO=<value>`
 * `-EPICS_CA_BEACON_PERIOD=<value>`
 * `-EPICS_CA_REPEATER_PORT=<value>`
 * `-EPICS_CA_SERVER_PORT=<value>`
 * `-EPICS_CA_MAX_ARRAY_BYTES=<value>`



# Environment Variables

The following environment variables are supported, as described in 
https://epics.anl.gov/EpicsDocumentation/ExtensionsManuals/StripTool/StripTool.html:
 
 * `STRIP_SITE_DEFAULTS`
 * `STRIP_FILE_SEARCH_PATH`

