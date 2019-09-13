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

The JAR file jstriptool-&#x3C;version&#x3E;-fat.jar can be built executing:
 ```
 ./gradlew build
 ```  

After executing the build command, the file jstriptool-&#x3C;version&#x3E;-fat.jar is located in the folder  ./build/libs. 


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
 java -jar jstriptool-&#x3C;version&#x3E;-fat.jar &#x3C;startup options...&#x3E; &#x3C;file name&#x3E;
 ```  

 * If &#x3C;file name&#x3E; is provided, the plot window is opened.
 * If &#x3C;file name&#x3E; is not provided, the configuration window is opened.



# Startup Options

The most relevant options are:

 * `-config : Show configuration dialog too when filename is provided`
 * `-home=&#x3C;dir&#x3E; : Set home folder`
 * `-default=&#x3C;dir&#x3E; : Set default configuration file`
 * `-laf=&#x3C;name&#x3E; :  Set the look and feel: nimbus, metal, dark, system, or LAF class name`
 * `-aa=false : Disable anti-aliasing`

One can override epics configuration providing parameters in command line:

 * `-EPICS_CA_ADDR_LIST=&#x3C;value&#x3E;`
 * `-EPICS_CA_AUTO_ADDR_LIST=&#x3C;value&#x3E;`
 * `-EPICS_CA_CONN_TMO=&#x3C;value&#x3E;`
 * `-EPICS_CA_BEACON_PERIOD=&#x3C;value&#x3E;`
 * `-EPICS_CA_REPEATER_PORT=&#x3C;value&#x3E;`
 * `-EPICS_CA_SERVER_PORT=&#x3C;value&#x3E;`
 * `-EPICS_CA_MAX_ARRAY_BYTES=&#x3C;value&#x3E;`



# Environment Variables

The following environment variables are supported, as described in 
https://epics.anl.gov/EpicsDocumentation/ExtensionsManuals/StripTool/StripTool.html:
 
 * `STRIP_SITE_DEFAULTS`
 * `STRIP_FILE_SEARCH_PATH`

