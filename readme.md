# Camouflage 

Camouflage is a library which can be used to mask sensetive information in data. This library supports masking from both apache beam and spark.

## Major components

There are 3 components of this libraby
    
     camouflage-core
     camouflage-beam
     camouflage-spark

camouflage-core holds core logic of making while camouflage-beam as well as camouflage-spark comsumes the core library.

The idea was to facilitate same type of masking logic from scala as well as java code hence camouflage-core has all the implementation of various infotypes and mask types.

## Features

At present Camouflage supports following mask types.


REDACT_CONFIG: This mask type is used to mask all the characters in input data with specific replacemenet chracter. eg. salary of specific employee "1234k" becomes "*****".

HASH_CONFIG: This mask type uses SHA-256 tokenization algorithm to convert input string to hash value. It takes salt value as optional parameter which gets applied to generate HASH. eg. input string "RICHARD HENDRICS" becomes "5622058f6c0ca52c0bd3047c6d563c5622697ca9039b27c01e3c56efda2f91e6". SHA-256 gurantees consistent hash values of same input using same salt.


### To generate dependecy version use this shell script
```
present_dir=`pwd`
cd ../camouflage/
mvn clean install
cd $present_dir
spark_jar_path=`find . -name 'camouflage-spark-*-SNAPSHOT.jar'`
echo $spark_jar_path
spark_jar_name=`echo $spark_jar_path | rev | cut -d'/' -f1 | rev`
echo $spark_jar_name
spark_jar_version=`echo $spark_jar_name | rev | cut -d'-' -f2 | rev`
echo $spark_jar_version
mvn install:install-file -Dfile=$spark_jar_path -DgroupId=camouflage-spark -DartifactId=camouflage-spark -Dversion=$spark_jar_version -Dpackaging=jar -DgeneratePom=true -DlocalRepositoryPath=.  -DcreateChecksum=true

beam_jar_path=`find . -name 'camouflage-beam-*.jar'`
echo $beam_jar_path
beam_jar_name=`echo $beam_jar_path | rev | cut -d'/' -f1 | rev`
echo $beam_jar_name
beam_jar_version=`echo $beam_jar_name | rev | cut -d'-' -f2 | rev`
echo $beam_jar_version
mvn install:install-file -Dfile=camouflage-beam/target/camouflage-beam-2.1-SNAPSHOT.jar -DgroupId=camouflage-beam -DartifactId=camouflage-beam -Dversion=beam_jar_version -Dpackaging=jar -DgeneratePom=true -DlocalRepositoryPath=.  -DcreateChecksum=true
```