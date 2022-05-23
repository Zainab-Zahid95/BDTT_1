# Databricks notebook source
dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

year = "2021"
fileroot = "clinicaltrial"
dbutils.fs.cp("/FileStore/tables/" + fileroot + "_" + year + "_csv.gz", "file:/tmp/")
import os
os.environ['fileroot'] = fileroot
os.environ['year'] = year

# COMMAND ----------

dbutils.fs.ls("file:/tmp/")

# COMMAND ----------

# MAGIC %sh
# MAGIC gunzip -d /tmp /tmp/"${fileroot}"_"${year}""_csv"

# COMMAND ----------

dbutils.fs.ls("file:/tmp/")

# COMMAND ----------

dbutils.fs.mv("file:/tmp/" + fileroot + "_" + year + "_csv", "/FileStore/tables/"+ fileroot + "_" + year + ".csv")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

clinicaltrials_2021 = spark.read.options(delimiter = "|", header = True, inferSchema = True).csv("/FileStore/tables/" + fileroot + "_" + year + ".csv")

# COMMAND ----------

clinicaltrials_2021.show()

# COMMAND ----------

display(clinicaltrials_2021)

# COMMAND ----------

clinicaltrials_2021.count()

# COMMAND ----------

clinicaltrials_2021.groupBy("Type").count().orderBy("count", ascending = False).show()

# COMMAND ----------

#Display, split and join the data with the conditions
myRDD1 = sc.textFile("dbfs:/FileStore/tables/clinicaltrial_2021.csv")

# COMMAND ----------

myRDD2 = myRDD1.map(lambda x: x.split('|'))

# COMMAND ----------

header = myRDD2.first()
myRDD3 = myRDD2.filter(lambda row: row != header)
NewRDD = myRDD3.flatMap(lambda x: x[7].split(","))


# COMMAND ----------

conditions_count = NewRDD.map(lambda x: (x,1))\
.reduceByKey(lambda v1, v2: v1+v2)\
.filter(lambda x: x[0] != "")\
.map(lambda x: (x[1],x[0]))\
.sortByKey(ascending = False)\
.map(lambda x: (x[1],x[0]))

# COMMAND ----------

conditions_count.take(5)

# COMMAND ----------

mesh = spark.read.options(delimiter = ",", header = True, inferSchema = True).csv("/FileStore/tables/mesh.csv")

# COMMAND ----------

myRDD4 = sc.textFile("dbfs:/FileStore/tables/mesh.csv")

# COMMAND ----------

mesh_joined = myRDD4.join(conditions_count)

# COMMAND ----------

mesh.take(10)

# COMMAND ----------

mesh.display()

# COMMAND ----------

filtered = mesh.rdd.map(lambda x: (x[0], x[1].split('.')[0]))

# COMMAND ----------

filtered.take(5)

# COMMAND ----------

new_join = filtered.join(conditions_count)

# COMMAND ----------

new_join.take(5)

# COMMAND ----------

swapped = new_join.map(lambda x: (x[1][0], x[1][1]));

# COMMAND ----------

swapped.take(5)

# COMMAND ----------

reduced = swapped.reduceByKey(lambda x, y: x+y);

# COMMAND ----------

reduced.take(5)

# COMMAND ----------

sorted = reduced.map(lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0]))

# COMMAND ----------

sorted.take(10)

# COMMAND ----------

pharma = spark.read.options(delimiter = ",", header = True, inferSchema = True).csv("/FileStore/tables/pharma.csv")

# COMMAND ----------

pharma.display()

# COMMAND ----------

pharma_sponsors = pharma.join(clinicaltrials_2021, pharma.Parent_Company ==  clinicaltrials_2021.Sponsor,"inner")

# COMMAND ----------

# COMMAND ----------
sponsor = clinicaltrials_2021.groupBy("Sponsor").count().orderBy("count", ascending = False)
pharma.groupBy("Parent_Company").count().orderBy("count", ascending = False).show(10)


# COMMAND ----------

sponsor.take(10)

# COMMAND ----------

clinicaltrials_2021.groupBy("Completion").count().orderBy("count", ascending = False).show(12)

# COMMAND ----------


