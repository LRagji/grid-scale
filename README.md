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


Work items
1. Hashing strings to numbers such that similar strings groups together.How does Redis does this what is the algo for key space hashing.
1. Converting table name to tag name. Done by using UNHEX and type=table on sqlite_schema table
1. Sharing volume mounts across pods(1 writer, inf Readers) to level load IO
2. Effect of lagging data with respect to wall clock(will this create lot of file io?)
3. Summarization.(Either with Indexes or one table per tag.)


F.A.Q

### Why do we have one table per tag design?

Data is normally read in ranges for given tags eg Tag1 from 2010 -> 2024, if a table has mixed tags then a page read from disk will have filter out lot of records ie: efficiency per disk IO drops, to improve this and gain more efficiency of sequential page reads this model is adopted.(its similar to how we will try to place data in cpu Lx cache to increase compute).

### What is the maximum size of tag name and encoding?

Maximum size of tag name is 255 UTF-8 characters.

## What is the data layout of data on disk?

Data is stored in *.db files which are sqlite files.
Following is the hierarchy
1. data : [Folder] Logical folder to gather everything under one folder, helps with mounting and readers.
2. scaled-set(n) : [Folder] Logical folder, when a writer scales beyond capacity of attached disks under this folder. Typical mapping could be tenancy.
3. disk(n) : [Folder] Attached multiple disks, helps to spread IOPS, structure cannot be changed once config is active.
4. D(*) : [Folder] Logical units aka cells of the grid system which helps in storing data in chunks.
5. W(*) : [File] The actual sqlite file individual writer will makes its own file for H-Scaling.

