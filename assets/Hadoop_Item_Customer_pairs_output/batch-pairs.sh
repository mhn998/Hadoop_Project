docker cp ~/Big_Data/Project/Item_Customer/out/artifacts/Item_Customer_Pairs_jar/Item_Customer_Pairs.jar 86a5c39dd98f:/usr/local/hadoop
docker cp ~/Big_Data/Project/Item_Customer/assets/Input-TEAM-5.txt 86a5c39dd98f:/usr/local/hadoop
bin/hadoop fs -mkdir /user/cloudera/item_Customer_Pairs/input
/bin/hadoop fs -put Input-TEAM-5.txt /user/cloudera/Item_Customer_Pairs/input
bin/hadoop fs -rm -r /user/cloudera/Item_Customer_Pairs/output
bin/hadoop jar Item_Customer_Pairs.jar /user/cloudera/Item_Customer_Pairs/input /user/cloudera/Item_Customer_Pairs/output
bin/hadoop fs -cat /user/cloudera/Item_Customer_Pairs/output/*