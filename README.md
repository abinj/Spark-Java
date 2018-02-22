**Apache Spark - Java**

A repository for apache spark various examples in java.


*WordCount*
* This class is the hello world program of apache spark.
* Its job is to count a specific word in a file.

*RDDPersist*
* RDDs are by default recomputed each time you run an action on them.
* If you wou;d like to reuse a particular state of RDD in multiple actions,
 you can persist them with spark RDD.persist() method.
* You can persist them on either memory or disk.
* The ability to always recompute an RDD is actually why RDDs are called “resilient.”
  When a machine holding RDD data fails, Spark uses this ability to recompute the missing partitions,
  transparent to the user.
  
*RDDPairAvg*
*combineByKey() is the most general of the per-key aggregation functions.
*Like aggregate() , combineByKey() allows the user to return values that are not the same type as our input data.
*As combineByKey() goes through the elements in a partition,If it’s a new element, combineByKey() uses a function
 we provide, called createCombiner() , to create the initial value for the accumulator on that key.
 
 It's important to note that this happens the first time a key is found in each partition, rather than
 only the first time the key is found in the RDD.
 
 If it is a value we have seen before while processing that partition, it will instead use
 the provided function, mergeValue() , with the current value for the accumulator for
 that key and the new value.
 
 Since each partition is processed independently, we can have multiple accumulators
 for the same key. When we are merging the results from each partition, if two or
 more partitions have an accumulator for the same key we merge the accumulators
 using the user-supplied mergeCombiners() function.
