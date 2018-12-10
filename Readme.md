# Overview

__jstriptool__ is a reimplementation of StripTool in Java. 



# Requirements

 * Java 8. 



# Binaries

Releases can be downloaded from: https://github.com/paulscherrerinstitute/jstriptool/releases



# Building

The JAR file jstriptool-<version>-fat.jar can be built executing:
 ```
 ./gradlew build
 ```  

After executing the build command, the file jstriptool-<version>-fat.jar is located in the folder  ./build/libs. 



# Launching

Launch the application typing:
 ```
 java -jar jstriptool-<version>-fat.jar <startup options...> <file name>
 ```  

 * If <file name> is provided, the plot window is opened.
 * If <file name> is not provided, the configuration window is opened.


# Startup Options:

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



