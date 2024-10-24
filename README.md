# grid-scale
Big data time series


Writer:
1. One start up create a unique id for itself use UUID.
2. Have child processes that can write to different sqlite db.(assuming sqlite uses built in process memory or can be stateless with threads?)
3. Open connection to redis as the indexer
4. Read the bucket widths for time and tags.(Default 50K 1 year)
5. Read tolerance for system time and sample time in number of widths. Minimum 1
6. Start express server to accept connections.
7. Table structure per day TagName(255),Sample Time,Numeric Value,Other Value Json ̀


What needs to scale:
1. Space to save data. (storage)
2. Accommodate for multiple REST clients (compute)
3. Reduce time required for single query.(latency)

Approach:
1. Solve Scale issue.
2. Solve Latency issue.
3. Solve Cost issue.

Observations:
1. High IOPS is needed
2. Compute requirements are low
3. IOPS with high storage is costly

Assumption:
1. It is easy to share volume mounts across multiple pods in k8s in readonly mode and single write/read mode.(Need to validate)
2. Typically Data will come in with respect to wall clock time and occasionally be out of sync, There has to be a tolerance setting such that reads are not amplified.
    The way to solve this would be to have linked list of index in redis and bigger time buckets like years to limit too many DB's to be opened.

Design Pro:
1. Fragmentation od Data cause of H scaling and Hashing.(Writers)
2. When data come in with a lead of lag with respect to system time which grows the cache lookups on redis.(Can we have background-process to clear up mis-placements.)

Work items

3. Let sqlite chunk take table schema as input.(Table Creation,Index,Upsert & Select Statements)

5. Unit tests
6. Cache for table names which exists in a DB, instead of querying them everytime.
7. Perfect Hashing algo for converting strings to sequential int's Looks at Dynamo DB hashing also.
8. Test with 5 million tags on one day data :D
9. Perf & Memory testing.
10. Effect of lagging data with respect to wall clock(will this create lot of file io?)
11. Summarization.(Either with Indexes or one table per tag.)


F.A.Q

### What dimensions does this scale on?

1. **Space**: High resolution time-series data is every growing and needs cheap scale-able space, The idea is to save data on files and move them to a cheap storage when not used.
2. **Latency**: Idea is to he H-scalable when more and more clients connects in cost effective way, this happens by having N read replica's of the data.

### Why do we have one table per tag design?

Data is normally read in ranges for given tags eg Tag1 from 2010 -> 2024, if a table has mixed tags then a page read from disk will have filter out lot of records ie: efficiency per disk IO drops, to improve this and gain more efficiency of sequential page reads this model is adopted.(its similar to how we will try to place data in cpu Lx cache to increase compute).

### What is the maximum size of tag name and encoding?

Maximum size of tag name is 255 UTF-8 characters.

### How is data layed-out on disk?

Data is broken up into small chunks according to the grid-scale algo and pushed into individual files and directories.
All files(*.db) are sqlite files.
Following is the directory hierarchy/structure
1. **data** : [Folder] Logical folder to gather everything under one folder, helps with mounting and readers.
2. **scaled-set(n)** : [Folder] Logical folder, when a writer scales beyond capacity of attached disks under this folder.

    - For scenario's where IOPS and Latency both needs to be scaled.
    - To keep adding space to your cluster.
    - To introduce cheap storage for data which is rarely being used.

3. **disk(n)** : [Folder] Attached multiple disks, helps to spread IOPS, structure cannot be changed once config is active for entire deployment lifecycle.
4. **D(|*|*|)** : [Folder] Logical units aka cells of the grid system which helps in storing data in chunks.
5. **(*).db** : [File] The actual sqlite file individual writer will makes its own file for H-Scaling.


### Things than can be further improved?

1. **Hash Algo**: A function which takes in variable length string input capped at 255 characters and return an integer/big-integer output such that 
    - Every string generates a unique integer output
    - Should be repeatable for same input same output
    - Should be sequential i.e: spread factor of 1.
    - Should be stateless, cannot build a LUT or similar table memory.
    - Should be fast.
    - Collision's are fine.

2. **Data Fragmentation**: H-Scaling a data structure means to spread data across multiple chunks, but it brings data fragmentation problem which means to read data the reader may have to iterate through multiple chunks, thus adding time and complexity at the readers end and increasing read-latency.
