#Apache Spark - Java

A repository for apache spark various examples in java.


WordCount
* This class is the hello world program of apache spark.
* Its job is to count a specific word in a file.

RDDPersist
* RDDs are by default recomputed each time you run an action on them.
* If you wou;d like to reuse a particular state of RDD in multiple actions,
 you can persist them with spark RDD.persist() method.
* You can persist them on either memory or disk.
* The ability to always recompute an RDD is actually why RDDs are called “resilient.”
  When a machine holding RDD data fails, Spark uses this ability to recompute the missing partitions,
  transparent to the user.
