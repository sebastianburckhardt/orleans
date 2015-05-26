Leaderboard simulation
------------------
Operations:
PostNow (requires synchronisation)
PostLater
GetApproximateTop10
GetExactTop10 (requires synchronisation)
------------------
Robots generate read/write requests in proportions specified
by the benchmark. Requests are generated in an open-loop
and arenot currently rate-controlled. All robots execute
the same load.
The staleness bound is set to int.maxValue and remains constant.

--------------------
Benchmark parameters

Format:
NumberRobots x NumRequests x PercentSyncRead x PercentAsyncRead x PercentSyncWrite x PercentAsyncWrite

For 1,50,100,500 robots:

1) Read-Only Benchmarks
 * No Replication
 * Sequenced Grain. All global reads
 * Sequenced Grain. All local reads
 * Sequenced Grain. 75 Global / 25 Local
 * Sequenced Grain. 25 Global / 75 Local

2) Write-Only Benchmarks
 * No Replication
 * Sequenced Grain. All global writes
 * Sequenced Grain. All local writes
 * Sequenced Grain. 75 Global / 25 Local
 * Sequenced Grain. 25 Global / 75 Local

3) Read-Write Benchmarks 
  a) Read mostly (read/write ratio: 90/10)
	* No replication
	* Sequenced Grain. All Global ops
	* Sequenced Grain. All Local ops
	* Sequenced Grain. 50/50 Global/Local
	* Sequenced Grain Local Reads, Global Writes
	* Sequenced Grain Global Reads, Local Writes
	* Sequenced Grain 50/50 Local/Global Reads, Local Writes
	* Sequenced Grain 50/50 Local/Global Reads, Global Writes
  b) Write-Heavy (read/write ratio: 70/30)
	* No replication
	* Sequenced Grain. All Global ops
	* Sequenced Grain. All Local ops
	* Sequenced Grain. 50/50 Global/Local
	* Sequenced Grain Local Reads, Global Writes
	* Sequenced Grain Global Reads, Local Writes
	* Sequenced Grain 50/50 Local/Global Reads, Local Writes
	* Sequenced Grain 50/50 Local/Global Reads, Global Writes
  b) Read/Write Equal (read/write ratio: 50/50)
	* No replication
	* Sequenced Grain. All Global ops
	* Sequenced Grain. All Local ops
	* Sequenced Grain. 50/50 Global/Local
	* Sequenced Grain Local Reads, Global Writes
	* Sequenced Grain Global Reads, Local Writes
	* Sequenced Grain 50/50 Local/Global Reads, Local Writes
	* Sequenced Grain 50/50 Local/Global Reads, Global Writes
