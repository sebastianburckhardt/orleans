using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Counters;
using System.Collections.Concurrent;
using System.Diagnostics;
using System;
using System.Threading;
using Orleans.Scheduler;
using Orleans.Runtime.Counters;
using Orleans.SystemTargetInterfaces;
using Orleans.Runtime.Scheduler;


/*
 * This file contains most of the logic for a distributed balanced graph partitioning algorithm in Orleans.
 * The goal is to put grains which communicate together on the same machine by moving them based on 
 * statistics about messaging between grains. We perform this in a distributed way to minimize overhead.
 * Also, stats are collected with memory constraints in mind only storing a subset of all communication
 * information.
 * 
 * Outside of this file, there are just a few places where graph partitioning code exists. There are places
 * that call this file to record messaging statistics when grains send or receive a message. The main silo
 * will start threads to run the swapping. There are several places to initialize the graph partitioning
 * director as well.
 */

namespace Orleans.Runtime.Coordination
{
    /// <summary>
    /// System target for graph partitioning
    /// </summary>
    internal class GraphPartitionSystemTarget : SystemTarget, IGraphPartitionSystemTarget
    {
        internal GraphPartitionSystemTarget(GrainId grain, SiloAddress silo)
            : base(grain, silo)
        {
        }

        /// <summary>
        /// Invoking this function translates to performing a swap request with the silo this grain is a target of
        /// </summary>
        /// <param name="remoteSilo">silo requesting the swap</param>
        /// <param name="proposal">generated candidate set</param>
        /// <param name="remoteGrainNum">number of grains at requesting silo</param>
        /// <returns></returns>
        public Task<List<ActivationAddress>> Swap(SiloAddress remoteSilo, Dictionary<ActivationAddress, double> proposal, int remoteGrainNum)
        {
            try
            {
                // If we swapped too recently, return a null list refusing the swap, otherwise proceed to swap
                if (!GraphPartitionDirector.CheckIfAllowedToSwap())
                {
                    GraphPartitionDirector.Log("Turning down swap request form " + remoteSilo + " as I swapped too recently");
                    return Task.FromResult<List<ActivationAddress>>(null);
                }

                GraphPartitionDirector.Log("Call to Swap with remote silo " + remoteSilo.ToLongString() + " number proposed grains " + proposal.Count + " remote activations " + remoteGrainNum);

                List<ActivationAddress> localToSend;
                List<ActivationAddress> remoteToSend;

                // obtain sets of grains to be swapped
                GraphPartitionDirector.LocalGreedySwapHeuristic(proposal, remoteSilo, remoteGrainNum, out localToSend, out remoteToSend);

                GraphPartitionDirector.Log("Proposed to send " + localToSend.Count);

                // send out my local grains which are to be swapped
                GraphPartitionDirector.SendGrains(remoteSilo, localToSend);

                // tell remote silo the grains the they should send back to our silo
                return Task.FromResult<List<ActivationAddress>>(remoteToSend);
            }
            catch (Exception e)
            {
                GraphPartitionDirector.Log("ERROR IN SWAP GRAIN " + e.StackTrace);
            }
            return null;
        }

        public OrleansContext GetContext()
        {
            return SchedulingContext;
        }
    }

    // Simple heavy-hitters implementation, almost any implementation from the literature could work
    // This algorithm is explained in a nice simple way here: http://boundary.com/blog/2013/05/14/approximate-heavy-hitters-the-spacesaving-algorithm/
    internal class HeavyHitter<T>
    {
        private Dictionary<T, int> counts;
        private Dictionary<T, int> errors;
        private int K;
        /// <summary>
        /// basic constructor, pass parameter for number of counts to keep
        /// </summary>
        public HeavyHitter(int K)
        {
            this.K = K;
            counts = new Dictionary<T, int>();
            errors = new Dictionary<T, int>();
        }
        /// <summary>
        /// constructor which updates the count of the first item
        /// </summary>
        public HeavyHitter(int K, T item)
            : this(K)
        {
            AddValue(item);
        }

        /// <summary>
        /// get all the keys for the counts
        /// </summary>
        public List<T> allKeys()
        {
            lock (this)
            {
                return new List<T>(counts.Keys);
            }
        }
        /// <summary>
        /// get count for particular item, a count is 0 if it does not exist
        /// </summary>
        public int GetCount(T item)
        {
            lock (this)
            {
                if (counts.ContainsKey(item))
                {
                    return counts[item];
                }
            }
            return 0;
        }
        /// <summary>
        /// get error for particular item, this is valid if the count is nonzero
        /// </summary>
        public int GetError(T item)
        {
            lock (this)
            {
                if (errors.ContainsKey(item))
                {
                    return counts[item];
                }
            }
            return 0;
        }
        /// <summary>
        /// incrememnt cound, main algorithm occurs in this method ensuring counts are removed it too many counts exist
        /// </summary>
        /// <returns>returns this object, useful for how its used with updates in concurrent dictionaries</returns>
        public HeavyHitter<T> AddValue(T item, int weight = 1)
        {
            lock (this)
            {
                if (counts.ContainsKey(item))
                {
                    counts[item] += weight;
                }
                else
                {
                    counts.Add(item, weight);
                    if (counts.Count > K)
                    {
                        KeyValuePair<T, int> minPair = new KeyValuePair<T, int>();
                        bool first = true;
                        foreach (var v in counts)
                        {
                            if (first || v.Value < minPair.Value)
                            {
                                minPair = v;
                                first = false;
                            }
                        }
                        errors.Add(item, minPair.Value);
                        counts[item] += minPair.Value;

                        counts.Remove(minPair.Key);
                        errors.Remove(minPair.Key);
                    }
                }
            }
            return this;
        }
    }

    /// <summary>
    /// Contains core logic for graph partitioning
    /// </summary>
    internal class GraphPartitionDirector : RandomPlacementDirector
    {
        // timer from last swap
        private static Stopwatch lastSwap;

        // raw counts
        private static ConcurrentDictionary<ActivationAddress, DateTime> lastHeardFrom = new ConcurrentDictionary<ActivationAddress, DateTime>();
        private static ConcurrentDictionary<ActivationAddress, ConcurrentDictionary<SiloAddress, int>> grainToSiloCounts = new ConcurrentDictionary<ActivationAddress, ConcurrentDictionary<SiloAddress, int>>();
        private static ConcurrentDictionary<ActivationAddress, HeavyHitter<ActivationAddress>> heavyHitters = new ConcurrentDictionary<ActivationAddress, HeavyHitter<ActivationAddress>>();

        // moving averages
        private static ConcurrentDictionary<ActivationAddress, ConcurrentDictionary<SiloAddress, double>> MAGrainToSiloCounts = new ConcurrentDictionary<ActivationAddress, ConcurrentDictionary<SiloAddress, double>>();
        private static ConcurrentDictionary<ActivationAddress, ConcurrentDictionary<ActivationAddress, double>> MAHeavyHitters = new ConcurrentDictionary<ActivationAddress, ConcurrentDictionary<ActivationAddress, double>>();

        // local system target
        internal static GraphPartitionSystemTarget LocalSystemTarget { get; private set; }

        // time between moving average calculations, affects units of moving average values which is important for the individual cutoff parameter
        private static TimeSpan statMACalculationTime = TimeSpan.FromSeconds(60);
        // time after a swap in which a silo cannot swap
        private static TimeSpan swapBreakTime = TimeSpan.FromSeconds(120);
        // time to wait before removing a grain from our statistics if we have not seen a message from or to that grain
        private static TimeSpan grainStatsTTL = TimeSpan.FromSeconds(60);
        // time between trying to initiate swaps
        private static TimeSpan swapInitiateTime = TimeSpan.FromSeconds(60);


        // moving average alpha: closer to 1 prefers new values while closer to 0 prefers history
        // alpha for grain to silo stats
        private static double movingAverageAlphaGrainToSilo = 0.8;
        // alpha for heavy hitter stats
        private static double movingAverageAlphaHeavyHitters = 0.2;

        // rate of message savings required to move an individual grain
        private static int individualCutoff = 2;
        // maximum number of grains to swap
        private static int maximumSwapAmount = 100;
        // maximum imbalance allowed between 2 silos
        private static int desiredMaximumImbalance = 50;

        // number of heavy hitters to store per local grain
        private static int numberHeavyHitters = 20;

        /// <summary>
        /// Update moving averages with raw count values
        /// </summary>
        public static void CalculateMovingAverages()
        {

            // get rid of expired grains
            DateTime now = DateTime.UtcNow;
            foreach (var grainToTime in lastHeardFrom)
            {
                if (now - grainToTime.Value > grainStatsTTL)
                {
                    ConcurrentDictionary<SiloAddress, double> temp1;
                    MAGrainToSiloCounts.TryRemove(grainToTime.Key, out temp1);

                    ConcurrentDictionary<ActivationAddress, double> temp2;
                    MAHeavyHitters.TryRemove(grainToTime.Key, out temp2);
                }
            }

            // lower existing moving average stats by a factor of (1 - alpha)
            foreach (var grainToCounts in MAGrainToSiloCounts)
            {
                foreach (var silo in new List<SiloAddress>(grainToCounts.Value.Keys))
                {
                    grainToCounts.Value.AddOrUpdate(silo, 0, (k, v) => v * (1 - movingAverageAlphaGrainToSilo));
                }
            }
            foreach (var grainToCounts in MAHeavyHitters)
            {
                foreach (var grain in new List<ActivationAddress>(grainToCounts.Value.Keys))
                {
                    grainToCounts.Value.AddOrUpdate(grain, 0, (k, v) => v * (1 - movingAverageAlphaHeavyHitters));
                }
            }

            // increase existing moving average stats by an alpha factor of the new raw count values
            foreach (var grainToCounts in grainToSiloCounts)
            {
                foreach (var siloToCount in grainToCounts.Value)
                {
                    MAGrainToSiloCounts.GetOrAdd(grainToCounts.Key, new ConcurrentDictionary<SiloAddress, double>()).AddOrUpdate(siloToCount.Key, siloToCount.Value, (k, v) => movingAverageAlphaGrainToSilo * siloToCount.Value);

                }
            }
            foreach (var grainToCounts in heavyHitters)
            {
                foreach (var grainToCount in grainToCounts.Value.allKeys())
                {
                    MAHeavyHitters.GetOrAdd(grainToCounts.Key, new ConcurrentDictionary<ActivationAddress, double>()).AddOrUpdate(grainToCount, grainToCounts.Value.GetCount(grainToCount), (k, v) => movingAverageAlphaHeavyHitters * grainToCounts.Value.GetCount(grainToCount));
                }
            }

            grainToSiloCounts.Clear();
            heavyHitters.Clear();
        }

        /// <summary>
        /// update raw counts for an observed message, possibly pass message size in future and increment raw counts based on size
        /// </summary>
        public static void MessageBetweenGrainAndSilo(ActivationAddress localGrain, SiloAddress remoteSilo, ActivationAddress remoteGrain)
        {
            try
            {
                // only track normal grain messages
                if (localGrain.Grain.IsGrain)
                {

                    // update last heard from structure
                    lastHeardFrom.AddOrUpdate(localGrain, DateTime.UtcNow, (k, v) => DateTime.UtcNow);

                    // update raw grain-to-silo count
                    grainToSiloCounts.GetOrAdd(localGrain, new ConcurrentDictionary<SiloAddress, int>()).AddOrUpdate(remoteSilo, 1, (k, v) => v + 1);

                    // update raw grain-to-grain count
                    heavyHitters.AddOrUpdate(localGrain, new HeavyHitter<ActivationAddress>(numberHeavyHitters), (k, v) => v.AddValue(remoteGrain));
                }
            }
            catch (Exception e)
            {
                Log("ERROR IN STAT COLLECTION " + e.StackTrace);
            }
        }

        /// <summary>
        /// retrieves set of grains that are the best candidates with a remote silo, each returned grain has the value of remote minus local messages
        /// </summary>
        public static Dictionary<ActivationAddress, double> GetBestCandidates(SiloAddress remoteSilo)
        {
            Dictionary<ActivationAddress,double> candidates = new Dictionary<ActivationAddress, double>();
            // GKTODO: need to change to get the best candiaites, not just first ones. Need to sort.
            foreach (var grainToCounts in MAGrainToSiloCounts)
            {
                // don't need more than the maximum swap amount of grains
                if (candidates.Count >= maximumSwapAmount)
                {
                    break;
                }
                double local;
                if (!grainToCounts.Value.TryGetValue(Silo.CurrentSilo.SiloAddress, out local))
                {
                    local = 0;
                }
                double remote;
                if (grainToCounts.Value.TryGetValue(remoteSilo, out remote))
                {
                    // don't add a grain if its savings is worse than the individual cutoff
                    if (remote - local > individualCutoff)
                    {
                        candidates.Add(grainToCounts.Key, remote - local);
                    }
                }
            }
            return candidates;
        }

        /// <summary>
        /// get grain-to-grain communication, check both directions, if both are non-zero take their average
        /// </summary>
        public static double GrainToGrainMessage(ActivationAddress x, ActivationAddress y)
        {
            // check both directions so that the local grain could be passed as either x or y
            double a = OneWayGrainToGrainMessage(x, y);
            double b = OneWayGrainToGrainMessage(y, x);
            if (a != 0 && b != 0)
            {
                return (a + b) / 2;
            }
            if (a != 0)
            {
                return a;
            }
            return b;
        }

        /// <summary>
        /// used only by the GrainToGrainMessage function
        /// </summary>
        public static double OneWayGrainToGrainMessage(ActivationAddress local, ActivationAddress remote)
        {
            ConcurrentDictionary<ActivationAddress, double> counts;
            if (MAHeavyHitters.TryGetValue(local, out counts))
            {
                double val;
                if (counts.TryGetValue(remote, out val))
                {
                    return val;
                }
            }
            return 0;
        }

        /// <summary>
        /// for swap function, check whether balance is alright to move a grain
        /// </summary>
        public static bool BalanceCheck(int removingFrom, int addingTo)
        {
            if (addingTo < removingFrom)
            {
                return true;
            }
            if (addingTo + 1 - removingFrom <= desiredMaximumImbalance)
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// heuristic at responding silo in coordinated distributed swapping, greedily builds up sets of grains to swap
        /// </summary>
        /// <param name="remoteCandidates">requesting silo (remote silo) candidate grains</param>
        /// <param name="remoteSilo">requesting silo (remote silo) identifier</param>
        /// <param name="remoteGrainNum">requesting silo (remote silo) number of total grains</param>
        /// <param name="localToSend">set of grains local silo should send to remote silo</param>
        /// <param name="remoteToSend">set of grains remote silo should send to this local silo</param>
        public static void LocalGreedySwapHeuristic(Dictionary<ActivationAddress, double> remoteCandidates, SiloAddress remoteSilo, int remoteGrainNum, out List<ActivationAddress> localToSend, out List<ActivationAddress> remoteToSend)
        {
            int localGrainNum = MAGrainToSiloCounts.Count;
            int toleratedImbalance = desiredMaximumImbalance;
            toleratedImbalance = (int)((remoteGrainNum + localGrainNum) * 0.1);
            remoteToSend = new List<ActivationAddress>();
            localToSend = new List<ActivationAddress>();

            Log("Tolerated imbalance " + toleratedImbalance);

            Dictionary<ActivationAddress, double> localCandidates = GetBestCandidates(remoteSilo);
 
            while (localToSend.Count + remoteToSend.Count < maximumSwapAmount)
            {
                KeyValuePair<ActivationAddress, double> bestLocal = new KeyValuePair<ActivationAddress, double>(null, 0);
                KeyValuePair<ActivationAddress, double> bestRemote = new KeyValuePair<ActivationAddress, double>(null, 0);

                if (BalanceCheck(localGrainNum, remoteGrainNum))
                {
                    // this is correct, but its completely inefficient implementation, priority queue would be far better
                    bestLocal = localCandidates.OrderBy((kv) => kv.Value).Last();
                }

                if (BalanceCheck(remoteGrainNum, localGrainNum))
                {
                    bestRemote = remoteCandidates.OrderBy((kv) => kv.Value).Last();
                }

                if (bestLocal.Key == null && bestRemote.Key == null)
                {
                    break;
                }

                if (bestLocal.Value > bestRemote.Value)
                {
                    localToSend.Add(bestLocal.Key);
                    UpdateCandidates(remoteCandidates, bestLocal.Key);
                    localGrainNum--;
                    remoteGrainNum++;
                }
                else
                {
                    remoteToSend.Add(bestRemote.Key);
                    UpdateCandidates(localCandidates, bestRemote.Key);
                    localGrainNum++;
                    remoteGrainNum--;
                }
            }
        }

        /// <summary>
        /// utility function just used in GetSwapChoice method
        /// </summary>
        private static void UpdateCandidates(Dictionary<ActivationAddress, double> candidates, ActivationAddress movedGrain)
        {

            foreach (var candidate in new List<ActivationAddress>(candidates.Keys))
            {
                candidates[candidate] -= 2 * GrainToGrainMessage(movedGrain, candidate);
                if (candidates[candidate] < individualCutoff)
                {
                    candidates.Remove(candidate);
                }
            }
        }

        /// <summary>
        /// actual sending of grains, shuts down grains, updates data structures
        /// </summary>
        public static void SendGrains(SiloAddress remoteSilo, List<ActivationAddress> grains)
        {
            List<ActivationData> toChangeData = new List<ActivationData>();
            foreach (ActivationAddress address in grains)
            {
                ActivationData data = null;
                if (catalogReferenceForPlacement.TryGetActivationData(address.Activation, out data))
                {
                    toChangeData.Add(data);
                    ConcurrentDictionary<SiloAddress, int> counts;
                    grainToSiloCounts.TryRemove(address, out counts);
                    HeavyHitter<ActivationAddress> temp;
                    heavyHitters.TryRemove(address, out temp);
                    newPlacements.AddOrUpdate(address.GrainReference.GrainId, remoteSilo, (k, v) => remoteSilo);
                }
            }

            Log("Wiping " + toChangeData.Count + " out of " + grains.Count + " grains");
            catalogReferenceForPlacement.QueueShutdownActivations(toChangeData);
        }

        // datastructure holding new placement decisions, so when a grain is shutdown the local graph partition director can move the grain to the newly decided silo
        public static ConcurrentDictionary<GrainId, SiloAddress> newPlacements = new ConcurrentDictionary<GrainId, SiloAddress>();

        // counter for how many times this silo has attempted a swap request
        private static int iter;

        // the following 4 public parameters should be refactored, this was a quick and easy way to get access to them
        // stores name of this silo
        public static string nameReference;
        // catalog reference, useful to shutdown grains
        public static Catalog catalogReferenceForPlacement;
        // reference to silo stats, useful for total number of grains over all silos, currently may not be used
        public static SiloStatisticsManager statsReferenceForPlacement;
        // scheduler reference, useful for sending a message to a remote system target
        public static OrleansTaskScheduler scheduler;


        public static void Init(string siloName, SiloStatisticsManager stats, Catalog catalog, SiloAddress address, OrleansTaskScheduler sched)
        {
            GraphPartitionDirector.nameReference = siloName;
            GraphPartitionDirector.statsReferenceForPlacement = stats;
            GraphPartitionDirector.catalogReferenceForPlacement = catalog;
            GraphPartitionDirector.LocalSystemTarget = new GraphPartitionSystemTarget(Constants.GraphPartitionSystemTargetId, address);
            GraphPartitionDirector.scheduler = sched;
            Thread swapperThread = new Thread(GraphPartitionDirector.SwapStrategy);
            swapperThread.Start();
        }

        public static bool CheckIfAllowedToSwap()
        {
            lock (lastSwap)
            {
                if (lastSwap.IsRunning && lastSwap.ElapsedMilliseconds < swapBreakTime.TotalMilliseconds)
                {
                    return false;
                }
                lastSwap.Restart();
                newPlacements.Clear();
                return true;
            }
        }

        public static void SwapDidNotHappen()
        {
            lock (lastSwap)
            {
                lastSwap.Stop();
            }
        }

        public static void StatsCalculation()
        {
            while (true)
            {
                Thread.Sleep(statMACalculationTime);
                CalculateMovingAverages();
            }

        }

        public async static void SwapStrategy()
        {
            try
            {
                Thread stats = new Thread(StatsCalculation);
                stats.Start();
                    
                lastSwap = new Stopwatch();
                iter = 0;

                // stagger when each silo starts, helps prevent strange race conditions when swapping
                Random rand = new Random(Silo.CurrentSilo.SiloAddress.GetHashCode());
                Thread.Sleep(rand.Next(30000));
                while (true)
                {
                    Thread.Sleep(swapInitiateTime);
                    iter++;

                    // some stat logging to see that things are going alright
                    int sample = 10;
                    double cross = 0;
                    double total = 0;
                    foreach (var grainToCounts in MAGrainToSiloCounts)
                    {
                        foreach (var siloToCount in grainToCounts.Value)
                        {
                            if (!siloToCount.Key.Matches(Silo.CurrentSilo.SiloAddress))
                            {
                                cross += siloToCount.Value;
                            }
                            total += siloToCount.Value;
                            if (sample-- > 0)
                            {
                                Logger.GetLogger("MESSAGES SPAM").Info(siloToCount.Value + " avg msgs/minute between grain " + grainToCounts.Key + " and silo " + siloToCount.Key);
                            }
                        }
                    }

                    sample = 10;
                    foreach (var grainToCounts in MAHeavyHitters)
                    {
                        foreach (var grainToCount in grainToCounts.Value)
                        {
                            if (sample-- > 0)
                            {
                                Logger.GetLogger("MESSAGES SPAM").Info(grainToCount.Value + " avg msgs/minute between grain " + grainToCounts.Key + " and grain " + grainToCounts.Key);
                            }
                        }
                    }
                    if (total == 0)
                    {
                        total = 1;
                    }
                    Log("iter " + iter + " grains " + MAGrainToSiloCounts.Count + " " + (int)(100 * cross / total) + "% remote messages (" + cross + "/" + total + ")");
                    Logger.GetLogger("STATREADING MEASUREMENTS").Info(MAGrainToSiloCounts.Count + " " + cross + " " + total);

                    // don't initiate a swap too early
                    if (iter <= 4)
                    {
                        continue;
                    }

                    // if we swapped too recently, don't try to swap
                    if (!CheckIfAllowedToSwap())
                    {
                        Log("Swapped too recently, skipping for now");
                        continue;
                    }

                    // get all the silos we have stats with
                    HashSet<SiloAddress> silos = new HashSet<SiloAddress>();
                    foreach (var v in grainToSiloCounts)
                    {
                        foreach (var w in v.Value)
                        {
                            silos.Add(w.Key);
                        }
                    }

                    Log("I think there are " + silos.Count + " silos");

                    silos.Remove(Silo.CurrentSilo.SiloAddress);

                    // get best candidates with each other silo
                    Dictionary<SiloAddress, Dictionary<ActivationAddress, double>> swapInfo = new Dictionary<SiloAddress, Dictionary<ActivationAddress, double>>();
                    double overallTotal = 0;
                    foreach (SiloAddress silo in silos)
                    {
                        Dictionary<ActivationAddress, double> candidates = GetBestCandidates(silo);

                        foreach (var v in candidates)
                        {
                            overallTotal += v.Value;
                        }
                        if (candidates.Count > 0)
                        {
                            swapInfo.Add(silo, candidates);
                        }
                    }

                    // try to swap with each one in order of potential savings with each remote silo
                    bool swapped = false;
                    foreach (var v in swapInfo.OrderBy((kv) => kv.Value.Sum((x) => -1 * x.Value)))
                    {
                        Log("Trying with " + v.Key + " that has value " + v.Value.Sum((x) => x.Value) + " with " + v.Value.Count + " candidates");
                        if (v.Value.Sum((x) => x.Value) < overallTotal * 0.5 / swapInfo.Count)
                        {
                            Log("Skipping as its not enough savings to try");
                            continue;
                        }
                        IGraphPartitionSystemTarget grain = GraphPartitionSystemTargetFactory.GetSystemTarget(Constants.GraphPartitionSystemTargetId, v.Key);
                        List<ActivationAddress> canSwap = await scheduler.RunOrQueueTask(() => grain.Swap(Silo.CurrentSilo.SiloAddress, v.Value, MAGrainToSiloCounts.Count), LocalSystemTarget.GetContext()).AsTask();
                        if (canSwap != null)
                        {
                            Log("Received swap response with " + canSwap.Count + " grains");
                            SendGrains(v.Key, canSwap);
                            swapped = true;

                            // if we were able to swap, we are done swapping
                            break;
                        }
                        else
                        {
                            Log("Swap request rejected");
                        }
                    }
                    if (!swapped)
                    {
                        Log("No swap happened");
                        SwapDidNotHappen();
                    }
                }
            }
            catch (Exception e)
            {
                Log("ERROR IN SWAP STRATEGY " + e.StackTrace);
            }
        }

        public static void Log(string x)
        {
            Logger.GetLogger("Grain Swap Analysis").Info(x);
        }


        /*
        class Move
        {
            public Move(ActivationAddress grain, SiloAddress silo)
            {
                this.grain = grain;
                this.silo = silo;
            }
            public ActivationAddress grain;
            public SiloAddress silo;
        }

        public static void PeriodicGrainWipe()
        {

            int siloID = Convert.ToInt32(nameReference.Substring(4));
            Random rand = new Random(siloID);
            Logger.GetLogger("Grain Placement Analysis").Info("I think my ID is " + siloID);
            
            int numSilos = 10;
            iter = 0;
            while (true)
            {
                lock (GrainToSiloCounts)
                {
                    //GrainToSiloCounts.Clear();
                }
                Logger.GetLogger("Grain Placement Analysis").Info("Starting sleep");
                Thread.Sleep(60 * 1000);

                int totalGrains = 0;
                numSilos = 0;
                foreach (var v in DeploymentLoadCollector.PeriodicStatistics)
                {
                    numSilos++;
                    Logger.GetLogger("Deployment Load Collector").Info(v.Key + ": " + v.Value.ActivationCount);
                    totalGrains += v.Value.ActivationCount;
                }
                if (numSilos == 0)
                {
                    numSilos = 1;
                }

                Logger.GetLogger("Grain Placement Analysis").Info("I think there are " + numSilos + " silos");

                if (totalGrains == 0)
                {
                    totalGrains = 1;
                }
              
                lock (newPlacements)
                {
                    newPlacements.Clear();
                }
                iter++;

                Logger.GetLogger("Grain Placement Analysis").Info("Wake-up at iter " + iter);
                int total = 0;
                int cross = 0;
                lock (GrainToSiloCounts)
                {
                    foreach (var v in GrainToSiloCounts)
                    {
                        foreach (var w in v.Value)
                        {
                            if (!w.Key.Matches(Silo.CurrentSilo.SiloAddress))
                            {
                                cross += w.Value;
                            }
                            total += w.Value;
                            //Logger.GetLogger("PLACEMENT MESSAGES SPAM").Info(v.Key.ToString() + ": " + w.Key.ToLongString() + " "+ w.Value);
                        }
                    }
                    if (total == 0)
                    {
                        total = 1;
                    }
                    Logger.GetLogger("Grain Placement Analysis").Info((int)(100 * GrainToSiloCounts.Count / totalGrains) + "% grains (" + GrainToSiloCounts.Count + "/" + totalGrains + ")");
                    Logger.GetLogger("Grain Placement Analysis").Info((int)(100 * cross / total) + "% remote messages (" + cross + "/" + total + ")");
                    Logger.GetLogger("STATREADING MEASUREMENTS").Info(GrainToSiloCounts.Count + " " + cross + " " + total);
                }
                
                if (iter <= 7 || iter % 2 == 1)
                {
                    continue;
                }

                if (rand.Next(2) == 0)
                {
                    continue;
                }

                if (!GraphPartitionUsed)
                {
                    continue;
                }

                Logger.GetLogger("Grain Placement Analysis").Info("Performing grain analysis");
                List<ActivationAddress> toChange = new List<ActivationAddress>();
                lock (GrainToSiloCounts)
                {
                    SortedDictionary<int, List<Move>> candidateMoves = new SortedDictionary<int, List<Move>>();
                    int c = 0;
                    foreach (var grainToCounts in GrainToSiloCounts)
                    {
                        int localMessages = 0;
                        SiloAddress bestCandidateSilo = null;
                        foreach (var siloToMessages in grainToCounts.Value)
                        {
                            if (siloToMessages.Key.Matches(Silo.CurrentSilo.SiloAddress))
                            {
                                localMessages = siloToMessages.Value;
                            }
                            else
                            {
                                if (bestCandidateSilo == null || grainToCounts.Value[bestCandidateSilo] < siloToMessages.Value)
                                {
                                    bestCandidateSilo = siloToMessages.Key;
                                }
                            }
                        }
                        if (bestCandidateSilo != null)
                        {
                            int messageSavings = grainToCounts.Value[bestCandidateSilo] - localMessages;

                            Move move = new Move(grainToCounts.Key, bestCandidateSilo);
                            if (!candidateMoves.ContainsKey(messageSavings))
                            {
                                candidateMoves.Add(messageSavings, new List<Move>());
                            }
                            c++;
                            candidateMoves[messageSavings].Add(move);
                            if (grainToCounts.Value[bestCandidateSilo] > 0)
                            {
                                //Logger.GetLogger("PLACEMENT MESSAGES SPAM").Info(grainToCounts.Key + ": " + bestCandidateSilo + " " + grainToCounts.Value[bestCandidateSilo] + " " + localMessages);
                            }
                        }
                    }

                    int myBalancedShare = totalGrains / numSilos;
                    double alpha = 0.1;

                    Logger.GetLogger("Grain Placement Analysis").Info("Balanced share " + myBalancedShare);

                    int maxToMove = (int)(GrainToSiloCounts.Count - (myBalancedShare * (1 - alpha)));
                    int minToMove = (int)(GrainToSiloCounts.Count - (myBalancedShare * (1 + alpha)));

                    //minToMove = 0;
                    //maxToMove = (int)(GrainToSiloCounts.Count * 0.02);

                    Logger.GetLogger("Grain Placement Analysis").Info(maxToMove + " maximum grains to move");
                    Logger.GetLogger("Grain Placement Analysis").Info(maxToMove + " minimum grains to move");
                    Logger.GetLogger("Grain Placement Analysis").Info(c + " candidate grains");

                    int num = 1;
                    int toPrint = 10;
                    int lastValue = -1;

                    foreach (var candidateValueToMoveList in candidateMoves.Reverse())
                    {
                        if (maxToMove <= 0 || candidateValueToMoveList.Key <= 3)
                        {
                            if (minToMove <= 0)
                            {
                                break;
                            }
                        }
                        lastValue = candidateValueToMoveList.Key;
                        foreach (var move in candidateValueToMoveList.Value)
                        {
                            if (num <= toPrint)
                            {
                                Logger.GetLogger("Grain Placement Analysis").Info(num + " grain " + move.grain + " with candidate value " + candidateValueToMoveList.Key + " moving to " + move.silo);
                            }
                            num++;
                            toChange.Add(move.grain);
                            lock (newPlacements)
                            {
                                newPlacements[move.grain.GrainReference.GrainId] = move.silo;
                            }
                            minToMove--;
                            maxToMove--;
                            if (maxToMove <= 0 || candidateValueToMoveList.Key <= 0)
                            {
                                if (minToMove <= 0)
                                {
                                    break;
                                }
                            }
                        }
                    }
                    Logger.GetLogger("Grain Placement Analysis").Info("worst candidate value moved "+lastValue);
                }
                Logger.GetLogger("STATREADING MOVEMENTS").Info(""+toChange.Count);
                    
                List<ActivationData> toChangeData = new List<ActivationData>();
                foreach (ActivationAddress address in toChange)
                {
                    ActivationData data = null;
                    if (catalogReferenceForPlacement.TryGetActivationData(address.Activation, out data))
                    {
                        toChangeData.Add(data);
                    }
                }

                foreach (var x in toChange)
                {
                    ConcurrentDictionary<SiloAddress, int> counts;
                    GrainToSiloCounts.TryRemove(x, out counts);
                }

                foreach (var v in GrainToSiloCounts)
                {
                    ConcurrentDictionary<SiloAddress, int> counts = v.Value;
                    foreach(var key in new List<SiloAddress>(counts.Keys)) {
                        counts[key] = (int)(counts[key] * 0.8);
                    }
                }

                Logger.GetLogger("Grain Placement Analysis").Info("Wiping  " + toChangeData.Count + " grains to be moved");
                catalogReferenceForPlacement.QueueShutdownActivations(toChangeData);
            }
        }
*/
        //private static bool GraphPartitionUsed = false;

        protected override Task<PlacementResult>
            OnAddTarget(PlacementStrategy strategy, GrainId grain, IPlacementContext context)
        {
            try
            {
                //GraphPartitionUsed = true;
                var allSilos = context.AllSilos;

                // if our local datastrcture was set for this grain, we need to put it on that desired silo
                SiloAddress desiredAddress;
                if (!newPlacements.TryGetValue(grain, out desiredAddress))
                {
                    desiredAddress = null;
                }
                if (desiredAddress != null)
                {
                    foreach (var siloAddress in allSilos)
                    {
                        //Logger.GetLogger("GraphPartitionPlacement").Info("Grain "+grain+" found its new silo "+desiredAddress);
                        if (siloAddress.Matches(desiredAddress))
                        {
                            return
                               Task.FromResult(
                                   PlacementResult.SpecifyCreation(
                                       siloAddress,
                                       strategy,
                                       context.GetGrainTypeName(grain)));
                        }
                    }
                    //Logger.GetLogger("GraphPartitionPlacement").Info("Grain "+grain+" was in new placements set, but its desired silo "+desiredAddress+" was not found");
                }
                // we reach here if our local datastrcture from graph partitioning has no information on this grain
                // early in the run, use random placement
                if (iter < 4)
                {
                    return
                        Task.FromResult(
                            PlacementResult.SpecifyCreation(
                                allSilos[_rng.Next(allSilos.Count)],
                                strategy,
                                context.GetGrainTypeName(grain)));
                }
                // after a while, use local placement
                return Task.FromResult(
                    PlacementResult.SpecifyCreation(
                        context.LocalSilo,
                        strategy,
                        context.GetGrainTypeName(grain)));
            }
            catch (Exception e)
            {
                Log("ERROR IN PLACEMENT " + e.StackTrace);
            }
            return null;
        }
    }
}
