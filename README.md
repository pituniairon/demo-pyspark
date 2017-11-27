## Details of the Infrastructure Setup
Pseudo cluster with one NameNode and one DataNode
Spark pseudo cluster running on yarn
MongoDB running on EC2 machine

## IP Address to EC2 instance
34.237.215.99

## User credentials for running apps
User: haduser

## MongoDB details
* Database Name:  demoDB
* Port:           27017 (default)
* Collection:     wikipedia_stats

## Script to run the application
~/code_file/getMonthlyFile.sh

## Underlying Python script called
~/code_file/demoPickMonthlyData.py

## Sample of MongoDB data post analysis
```json
{
	"_id" : ObjectId("5a13b95a471c37466786a43b"),
	"language" : "fa.voy",
	"total_hits" : NumberLong(3741),
	"average_pagesize" : NumberLong(5184507)
}
{
	"_id" : ObjectId("5a13b95a471c37466786a43c"),
	"language" : "csb.d",
	"total_hits" : NumberLong(208),
	"average_pagesize" : NumberLong(250561)
}
{
	"_id" : ObjectId("5a13b95a471c37466786a43d"),
	"language" : "gom",
	"total_hits" : NumberLong(2347),
	"average_pagesize" : NumberLong(4771632)
}
```
## Number of rows in MongoDB post analysis of 6 files
```javascript
db.wikipedia_stats.find().count();
1239
```

