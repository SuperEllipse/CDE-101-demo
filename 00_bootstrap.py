### Installing Requirements
!pip3 install -r requirements.txt

import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime
import yaml

#Extracting the correct URL from hive-site.xml
tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
root = tree.getroot()

for prop in root.findall('property'):
    if prop.find('name').text == "hive.metastore.warehouse.dir":
        storage = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]

print("The correct CLoud Storage URL is:{}".format(storage))

os.environ['STORAGE'] = storage

### Downloading the Lab Files

!wget https://www.cloudera.com/content/dam/www/marketing/tutorials/cdp-using-cli-api-to-automate-access-to-cloudera-data-engineering/tutorial-files.zip
!unzip tutorial-files.zip
  
!hdfs dfs -mkdir -p $STORAGE/datalake/cde-demo
!hdfs dfs -copyFromLocal /home/cdsw/PPP-Over-150k-ALL.csv $STORAGE/datalake/cde-demo/PPP-Over-150k-ALL.csv
!hdfs dfs -copyFromLocal /home/cdsw/PPP-Sub-150k-TX.csv $STORAGE/datalake/cde-demo/PPP-Sub-150k-TX.csv
!hdfs dfs -ls $STORAGE/datalake/cde-demo

!rm /home/cdsw/PPP-Over-150k-ALL.csv /home/cdsw/PPP-Sub-150k-TX.csv config.yaml

### CDE CLI Setup

!mkdir .cde

### Recreating Yaml file with your credentials:

dict_yaml = {"user" : os.environ["WORKLOAD_USER"], 
             "vcluster-endpoint": os.environ["CDE_VC_ENDPOINT"]}

with open(r'.cde/config.yaml', 'w') as file:
  documents = yaml.dump(dict_yaml, file)

### Downloading the CLI tool

#!wget $CDE_CLI_linux -P /home/cdsw/.local/bin
#mv cde /home/cdsw/.local/bin
#!chmod +x /home/cdsw/.local/bin/cde

### Setting up Project files  
!mv config.yaml /home/cdsw/.cde

#!export PATH=/home/cdsw/.cde:$PATH

