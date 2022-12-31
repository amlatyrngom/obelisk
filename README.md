# Roadmap
1. Test Scaler (done).
2. Test Direct Invocation with scale ups and downs.
3. Wrapped dynamodb around consistent

* Suppose application knows a given key can only every be associated with unique value.
* Every key-value  
* This is true for (darq-lsn, )
* What optimizations become possible.
* I can store the key anywhere,

* Scenario 1: Speeding up any type of write.
    * Naive:
        * At low-scale, just read/write using dynamodb.
        * Then startup a set of replicas.
        * After starting grace period,
    * Keep on incrementing