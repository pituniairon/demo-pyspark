#!/bin/bash
## shell script to process file

## Variable Declaration
#THISMONTH=`date '+%Y-%M'`
#THISYEAR=`date '+%Y'`
THISMONTH=2016-07
THISYEAR=2016
pageURL="https://dumps.wikimedia.org/other/pagecounts-raw/$THISYEAR/$THISMONTH"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/home/haduser/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin

## to ingest monthly data
## wget -r -l1 -nH --cut-dirs=4 "https://dumps.wikimedia.org/other/pagecounts-raw/2016/2016-07" -A "pagecounts-20160701-[0-3][0-9]0000.gz" -P /home/haduser/demo_tmp_files/
wget -r -l1 -nH --cut-dirs=4 $pageURL -A "pagecounts-20160701-0[0-6]0000.gz" -P /home/haduser/demo_tmp_files/

## extract year-month from file
monthyr=`ls /home/haduser/demo_tmp_files/pagecounts* |tail -1 | cut -d'/' -f5 |cut -c12-17`

## extract gz files and upload them to hadoop
gunzip /home/haduser/demo_tmp_files/*.gz

$HADOOP_HOME/bin/hdfs dfs -mkdir /user/haduser/${monthyr}
$HADOOP_HOME/bin/hdfs dfs -put /home/haduser/demo_tmp_files/pagecounts-${monthyr}* /user/haduser/${monthyr}

## get complete path of hdfs file
## ./hadoop/bin/hdfs getconf -confKey fs.defaultFS

## clean up old files
rm /home/haduser/demo_tmp_files/pagecounts-${monthyr}*

## run spark application
$SPARK_HOME/bin/spark-submit ~/code_file/demoPickMonthlyData.py $monthyr \
--master yarn \
--deploy-mode cluster