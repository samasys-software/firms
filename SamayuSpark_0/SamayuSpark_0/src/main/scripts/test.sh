#!/bin/bash
export SPARK_MAJOR_VERSION=2
export DATE=$(date +%m/%d/%Y)
echo "$DATE"
BASEDIR=$(dirname "$0")
BASEDIR=$(cd $BASEDIR/../lib; pwd)
echo "Tom"
spark-submit --class TomFile --deploy-mode client --master yarn $BASEDIR/samayuspark_0_2.11-0.1.jar $DATE /root/share_file_system/toms.csv /dev/kathirvel/src/toms
if [ "$?" -eq 0 ];
then
echo "Monaco"
spark-submit --class MonacoFile --deploy-mode client --master yarn $BASEDIR/samayuspark_0_2.11-0.1.jar $DATE /root/share_file_system/monaco.csv /dev/kathirvel/src/monaco
fi
if [ "$?" -eq 0 ];
then
echo "cdr" 
spark-submit --class CdrFile --deploy-mode client --master yarn $BASEDIR/samayuspark_0_2.11-0.1.jar $DATE /root/share_file_system/cdr.csv /dev/kathirvel/src/cdr
fi	        
if [ "$?" -eq 0 ]; 
then
echo "bdr" 
spark-submit --class BdrFile --deploy-mode client --master yarn $BASEDIR/samayuspark_0_2.11-0.1.jar $DATE /dev/kathirvel/src/bdr  /dev/kathirvel/std/bdr.parquet
fi		
if [ "$?" -eq 0 ];
then
echo "sdr"
spark-submit --class SdrFile --deploy-mode client --master yarn $BASEDIR/samayuspark_0_2.11-0.1.jar $DATE /dev/kathirvel/src/sdr  /dev/kathirvel/std/sdr.parquet
fi			
if [ "$?" -eq 0 ];
then
echo "tomsparquet"
spark-submit --class TomsFileToStd --deploy-mode client --master yarn $BASEDIR/samayuspark_0_2.11-0.1.jar $DATE /dev/kathirvel/src/toms  /dev/kathirvel/std/toms.parquet
fi	
if [ "$?" -eq 0 ];
then
echo "cdrparquet"
spark-submit --class CdrFileToStd --deploy-mode client --master yarn $BASEDIR/samayuspark_0_2.11-0.1.jar $DATE /dev/kathirvel/src/cdr  /dev/kathirvel/std/cdr.parquet
fi	
if [ "$?" -eq 0 ];
then
echo "monacoparquet"
spark-submit --class MonacoFileToStd --deploy-mode client --master yarn $BASEDIR/samayuspark_0_2.11-0.1.jar $DATE /dev/kathirvel/src/monaco  /dev/kathirvel/std/monaco.parquet
fi	
if [ "$?" -eq 0 ];
then
echo "passed"
else
echo "FAil"
fi
cd '/root/scalaproject/'
./scalaproject_run.sh
