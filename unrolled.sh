#!/bin/bash

echo "Bash version ${BASH_VERSION}..."

for j in YA YB YC YD YE
do
echo $j



echo "spark"
date +%Y.%m.%d-%H:%M:%S 
time java -cp target/rheemstudy-1.0-SNAPSHOT.jar:/home/jonas.kemper/rheem/rheem-distro/target/rheem-distro-0.4.0-SNAPSHOT.jar:/home/jonas.kemper/rheem/rheem-distro/target/rheem-distro-0.4.0-SNAPSHOT-distro/rheem-distro-0.4.0-SNAPSHOT/*:/opt/spark/spark-1.6.2-bin-hadoop2.6/lib/spark-assembly-1.6.2-hadoop2.6.0.jar  -Drheem.configuration=file:/home/jonas.kemper/rheemstudy/app.properties  kmeansUnrolled spark  > ~/scripts/logs/spark_$j.txt 2>&1

echo "java"
date +%Y.%m.%d-%H:%M:%S 
time java -cp target/rheemstudy-1.0-SNAPSHOT.jar:/home/jonas.kemper/rheem/rheem-distro/target/rheem-distro-0.4.0-SNAPSHOT.jar:/home/jonas.kemper/rheem/rheem-distro/target/rheem-distro-0.4.0-SNAPSHOT-distro/rheem-distro-0.4.0-SNAPSHOT/*:/opt/spark/spark-1.6.2-bin-hadoop2.6/lib/spark-assembly-1.6.2-hadoop2.6.0.jar  -Drheem.configuration=file:/home/jonas.kemper/rheemstudy/app.properties  kmeansUnrolled java > ~/scripts/logs/java_$j.txt 2>&1


for i in 13 6 18 3 9 15 21 2 7 1 4 5 8 10 11 12 14 16 17 19 20 22 23 24 
do
	echo $i
	date +%Y.%m.%d-%H:%M:%S 
	time java -cp target/rheemstudy-1.0-SNAPSHOT.jar:/home/jonas.kemper/rheem/rheem-distro/target/rheem-distro-0.4.0-SNAPSHOT.jar:/home/jonas.kemper/rheem/rheem-distro/target/rheem-distro-0.4.0-SNAPSHOT-distro/rheem-distro-0.4.0-SNAPSHOT/*:/opt/spark/spark-1.6.2-bin-hadoop2.6/lib/spark-assembly-1.6.2-hadoop2.6.0.jar  -Drheem.configuration=file:/home/jonas.kemper/rheemstudy/app.properties  kmeansUnrolled mixed $i > ~/scripts/logs/mixed-$i-$j.txt 2>&1
done





done
