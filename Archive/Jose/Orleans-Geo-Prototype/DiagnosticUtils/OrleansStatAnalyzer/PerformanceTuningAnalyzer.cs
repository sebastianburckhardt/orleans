using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    internal class Stage
    {
        // A queue stage includes counters whose name starts with these strings.
        public static List<string> preficsQueueStage = new List<string>() { "Queues.QueueSize.Average.", "Queues.EnQueued.", "Queues.AverageArrivalRate.RequestsPerSecond." };
        // A qthread stage includes counters whose name starts with these strings.
        public static List<string> preficsThreadStage = new List<string>() { 
            "Thread.Utilization.",
            "Thread.NumProcessedRequests.",
            "Thread.ProcessingTime.Average.CPUCycles.Milliseconds.",
            "Thread.ProcessingTime.Average.WallClock.Milliseconds.",
            "Thread.ProcessingTime.Total.CPUCycles.Milliseconds.",
            "Thread.ProcessingTime.Total.WallClock.Milliseconds.",
            "Thread.ExecutionTime.Total.CPUCycles.Milliseconds.",
            "Thread.ExecutionTime.Total.WallClock.Milliseconds.",
            "Thread.Processing.Utilization.CPUCycles.",
            "Thread.Processing.Utilization.WallClock."
        };

        // A queue-thread stage includes counters whose name starts with these strings. 
        public static List<string> preficsQueueThreadStage
        {
            get
            {
                List<string> a = new List<string>();
                a.AddRange(preficsQueueStage);
                a.AddRange(preficsThreadStage);
                return a;
            }
        }

        public string name;
        public string type;
        public PerfCounterData counterData;

        // Actual counter names included in this stage, like "Thread.Utilization.WorkerPoolThread.1"
        public List<string> countersThisStage;
        public Dictionary<string, double> globalValues;
        public List<string> prefics;
       
        private double getOverallValue(string counter, Func<List<double>, double> f)
        {
            List<Double> counterVals = new List<double>();
            foreach (var pair in counterData.SiloData)
            {
                try
                {
                    foreach (var counterVal in pair.Value.SiloStatistics[counter].TimeVals)
                    {
                        counterVals.Add(counterVal.Value);
                    }
                }
                catch (KeyNotFoundException)
                {
                    Console.WriteLine("The key {0} is not found.", counter);
                }
            }

            return f(counterVals);
        }

        private double getMedianValue(string counter)
        {
            return getOverallValue(counter, NumericalAnalyzer.Median);
        }

        private double getLastValue(string counter)
        {
            return getOverallValue(counter, list => list.Last());
        }

        public Stage(PerfCounterData _counterData, string _name, string _type, string exception = null)
            : this(_counterData, _name, _type, new List<string>() { _name }, exception)
        {
        }

        public Stage(PerfCounterData _counterData, string _name, string _type, List<string> _counterNames, string exception = null)
        {
            counterData = _counterData;
            name = _name;
            type = _type;
            countersThisStage = new List<string>();
            globalValues = new Dictionary<string, double>();

            if (type == "queue-thread")
                prefics = preficsQueueThreadStage;
            else if (type == "queue")
                prefics = preficsQueueStage;
            else if (type == "thread")
                prefics = preficsThreadStage;
            else
                prefics = new List<string>() { "" };

            // _counterNames: Unique part of the counter names included in this stage, like "WorkerPoolThread"
            // prefics: Prefix of the counter names included in this stage, like "Thread.Utilization."
            // countersThisStage: Actual counter names included in this stage, like "Thread.Utilization.WorkerPoolThread.1"
            foreach (string counterName in _counterNames)
            {
                foreach (string prefix in prefics)
                {
                    if(exception != null)
                        countersThisStage.AddRange(counterData.AllCounters.Where(counter => counter.StartsWith(prefix + counterName) && !counter.Contains(exception) && !counter.EndsWith("Delta")));
                    else
                        countersThisStage.AddRange(counterData.AllCounters.Where(counter => counter.StartsWith(prefix + counterName) && !counter.EndsWith("Delta")));
                }
            }

            foreach(string counter in countersThisStage)
            {
                // For "Runtime.*" counters, take the madian from all values
                if (counter.StartsWith("Runtime"))
                {
                    globalValues.Add(counter, getMedianValue(counter));
                }
                // For other counters, take the last value across time
                else
                {
                    globalValues.Add(counter, getLastValue(counter));
                }
            }
        }

        public Dictionary<string,double> getScores()
        {
            
            Dictionary<string,double> ans = new Dictionary<string,double>();

            foreach (string prefix in prefics)
            {
                try
                {
                    if (prefix.Contains("Utilization"))
                    {
                        // ThreadUtilizations should be averaged across counters (e.g. Thread.Utilization.WorkerPoolThread.* -> average value)
                        ans.Add(prefix,globalValues.Where(pair => pair.Key.StartsWith(prefix)).Average(pair => pair.Value));
                    }

                    else
                    {
                        ans.Add(prefix, globalValues.Where(pair => pair.Key.StartsWith(prefix)).Sum(pair => pair.Value));
                    }
                    /*if (name.Contains("WorkerPoolThread"))
                    {
                        Console.Error.WriteLine(prefix);
                        foreach (var x in globalValues.Where(pair => pair.Key.StartsWith(prefix)))
                        {
                            Console.Error.WriteLine(x.Key + " => " + x.Value);
                        }
                    }*/
                }
                catch (Exception)
                {
                    ans.Add(prefix, 0);
                }
            }
            return ans;
        }
    }

    class PerformanceTuningAnalyzer : PerformanceCounterAnalyzer
    {
        private List<Stage> stages;

        // Set this to true when you want to analyze silo logs, and to false when you analyze client logs.
        // Also do not forget to change AnalysisConfiguration.xml to use appropriate logs.
        private const bool siloSide = true;

        private void defineStages(PerfCounterData counterData)
        {
            stages = new List<Stage>();
            
            if (siloSide)
            {
                // Server Side
                stages.Add(new Stage(counterData, "GatewaySiloSender", "thread"));
                stages.Add(new Stage(counterData, "SiloMessageSender", "thread"));
                stages.Add(new Stage(counterData, "SiloMessageSender.PingSender", "thread"));
                stages.Add(new Stage(counterData, "SiloMessageSender.SystemSender", "thread"));
                stages.Add(new Stage(counterData, "IncomingMessageAgent.Application", "thread"));
                stages.Add(new Stage(counterData, "IncomingMessageAgent.Ping", "thread"));
                stages.Add(new Stage(counterData, "IncomingMessageAgent.System", "thread"));
               /* stages.Add(new Stage(counterData, "Scheduler.Catalog", "queue"));
                stages.Add(new Stage(counterData, "AllSystemTargets", "queue",
                    new List<string>() {
                    "Scheduler.ClientObserverRegistrar", 
                    "Scheduler.DirectoryCacheValidator",
                    "Scheduler.DirectoryService",
                    "Scheduler.MembershipOracle",
                    "Scheduler.SiloControl",
                    "Scheduler.TestSystemTarget",
                    "Scheduler.TypeManagerId"
                }));*/
                stages.Add(new Stage(counterData, "WorkerPoolThread", "thread", "thread"));
                stages.Add(new Stage(counterData, "WorkerPoolThread.System", "thread"));
                //stages.Add(new Stage(counterData, "Scheduler.LevelOne.MainQueue", "queue"));
                //stages.Add(new Stage(counterData, "Scheduler.LevelOne.SystemQueue", "queue"));
                //stages.Add(new Stage(counterData, "Scheduler.LevelTwo.Sum", "queue"));
                //stages.Add(new Stage(counterData, "Scheduler.LevelTwo.Average", "queue"));
                /*stages.Add(new Stage(counterData, "Runtime.CpuUsage", "other"));
                stages.Add(new Stage(counterData, "Runtime.DOT.NET.ThreadPool.InUse.WorkerThreads", "other"));
                stages.Add(new Stage(counterData, "Runtime.DOT.NET.ThreadPool.InUse.CompletionPortThreads", "other"));
                stages.Add(new Stage(counterData, "Runtime.GC.PercentOfTimeInGC", "other"));
                stages.Add(new Stage(counterData, "Messaging.Sent.BatchSize.PerSocketDirection.GWToClient", "other"));
                stages.Add(new Stage(counterData, "Messaging.Sent.BatchSize.PerSocketDirection.SiloToSilo", "other"));
                stages.Add(new Stage(counterData, "Messaging.Received.BatchSize.PerSocketDirection.GWToClient", "other"));
                stages.Add(new Stage(counterData, "Messaging.Received.BatchSize.PerSocketDirection.SiloToSilo", "other"));*/
            }
            else
            {
                // Client Side
                stages.Add(new Stage(counterData, "ClientReceiver", "queue-thread"));
                stages.Add(new Stage(counterData, "GatewayClientSender", "queue-thread"));
                stages.Add(new Stage(counterData, "Runtime.CpuUsage", "other"));
                stages.Add(new Stage(counterData, "Runtime.DOT.NET.ThreadPool.InUse.WorkerThreads", "other"));
                stages.Add(new Stage(counterData, "Runtime.DOT.NET.ThreadPool.InUse.CompletionPortThreads", "other"));
                stages.Add(new Stage(counterData, "Runtime.GC.PercentOfTimeInGC", "other"));
                stages.Add(new Stage(counterData, "Messaging.Sent.BatchSize.PerSocketDirection.ClientToGW", "other"));
                stages.Add(new Stage(counterData, "Messaging.Received.BatchSize.PerSocketDirection.ClientToGW", "other"));
            }
        }

        // this function generates comma-seperated-values, so you can display them with Excel after executing this command line:
        //   > OrleanzStatAnalyze.exe | FindStr "," > result.csv
        public override void Analyze(PerfCounterData counterData, HashSet<string> counters)
        {
            Console.WriteLine("This is a PerformanceTuningAnalyzer written by Soramichi");

            defineStages(counterData);

            // print the stage names only at once (suppose there are multiple datasets, then we want to aggregate them into one CSV)
            //if (counterData.Name == "1")

            Console.Write("Stage,");
            foreach (string prefix in Stage.preficsThreadStage)
            {
                Console.Write(prefix.Substring(0,prefix.Length - 1).Substring(7) + ",");
            }
            Console.WriteLine();

            IEnumerable<Stage> sortedStages = stages.OrderBy(x => (-1)*x.getScores()["Thread.NumProcessedRequests."]);

            foreach (Stage stage in sortedStages)
            {
                Console.Write(stage.name+",");
                foreach (string prefix in Stage.preficsThreadStage)
                {
                    if (stage.getScores().ContainsKey(prefix))
                    {
                        Console.Write(stage.getScores()[prefix]);
                    }
                    else
                    {
                        Console.Write("0");
                    }
                    Console.Write(",");
                }

                Console.WriteLine();
            }

           
        }
    }
}
