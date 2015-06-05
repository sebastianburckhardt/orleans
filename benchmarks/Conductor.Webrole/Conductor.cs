using Common;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Azure.Storage;

namespace Conductor.Webrole
{
    public class Conductor : IConductorContext
    {

        public static Conductor Instance { get { return _instance ?? (_instance = new Conductor()); } }

        private static Conductor _instance;

        private Conductor() { }


        public Benchmarks.Console console;

        private string testname;

        public string TestName { get { return testname; } }

        public Dictionary<string, WebSocket> LoadGenerators = new Dictionary<string, WebSocket>();

        IBenchmark benchmark;
        IEnumerable<IScenario> scenarios;
        public List<RobotInfo> robots;

        private string STAT_TABLE = "results";


        public class RobotInfo
        {
            public string instance;
            public WebSocket ws;
            public TaskCompletionSource<string> promise;
            public Dictionary<string, LatencyDistribution> stats;
        }


        public CommandHub Hub;

        public void Broadcast(string a, string b)
        {
            if (Hub != null)
                Hub.Clients.All.addNewMessageToPage(a, b);
        }

        private void Run()
        {
            console = new Benchmarks.Console(WriteLine, ReadLine);

            console.Welcome();

            while (true)
            {
                var kvp = console.SelectScenario();

                if (!kvp.HasValue)
                    break;

                this.benchmark = kvp.Value.Key;
                this.scenarios = kvp.Value.Value;

                if (LoadGenerators.Count == 0)
                {
                    Broadcast("Failed", "cannot run scenario: no load generators");
                    continue;
                }

                CloudTableClient tableClient = AzureUtils.getTableClient("DataConnectionString");
                AzureUtils.createTableCheck(tableClient, STAT_TABLE);

                foreach (var scenario in scenarios)
                {

                    this.testname = string.Format("{0:o}.{1}.{2}", DateTime.UtcNow, benchmark.Name, scenario.Name);
                    this.robots = new List<RobotInfo>();


                    // assign robots to load generators round-robin
                    var numrobots = scenario.NumRobots;
                    while (numrobots > 0)
                        foreach (var gen in LoadGenerators)
                        {
                            if (numrobots-- == 0)
                                break;
                            robots.Add(new RobotInfo() { instance = gen.Key, ws = gen.Value });
                        }

                    var result = RunScenario(scenario).Result;


                    // collect stats from all robots
                    var overallstats = new Dictionary<string, LatencyDistribution>();
                    foreach (var robot in robots)
                        if (robot.stats != null)
                            foreach (var kkvp in robot.stats)
                            {
                                if (!overallstats.ContainsKey(kkvp.Key))
                                    overallstats.Add(kkvp.Key, new LatencyDistribution());
                                overallstats[kkvp.Key].MergeDistribution(kkvp.Value);
                            }

                    Broadcast("Result", result + " " + Util.PrintStats(overallstats));

                    LatencyDistribution stats = null;
                    if (overallstats.Any()) 
                    {
                        stats = overallstats.First().Value;
                    }
                    Azure.Storage.StatEntity statEntity = new Azure.Storage.StatEntity(benchmark.Name, scenario.Name, DateTime.Now, result, stats);
                 //   Azure.Storage.StatEntity statEntity = new Azure.Storage.StatEntity(benchmark.Name, scenario.Name, DateTime.Now, result);

                    try
                    {
                        TableResult logResult = AzureUtils.updateEntity<Azure.Storage.StatEntity>(tableClient, STAT_TABLE, statEntity).Result;
                        if (logResult.HttpStatusCode != 204)
                        {
                            Console.WriteLine("Failed to write results to storage {0}", logResult.HttpStatusCode);

                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Failed to write results to storage {0}", e.ToString());
                    }

                    if (overallstats.Count > 0)
                        Console.WriteLine("Stats", Util.PrintStats(overallstats));
                }
            }

            Broadcast(">", "Finished");

            console = null;
        }
        public int NumRobots
        {
            get;
            set;
        }

        public async Task<string> RunRobot(int robotnumber, string parameters)
        {
            var robot = robots[robotnumber];

            Util.Assert(robot.promise == null);
            robot.promise = new TaskCompletionSource<string>();

            //var message = "START " + testname + " " + robotnumber + " " + parameters;
            JObject message = JObject.FromObject(new
            {
                type = "START",
                testname = testname,
                robotnr = robotnumber,
                args = parameters
            });
            var buffer = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message.ToString()));
            await LoadGenerators[robot.instance].SendAsync(buffer, WebSocketMessageType.Text, true, CancellationToken.None);

            return await robot.promise.Task;
        }

        public void OnRobotMessage(int robotnumber, string message, Dictionary<string, LatencyDistribution> stats)
        {
            var robot = robots[robotnumber];
            var promise = robot.promise;
            robot.promise = null;
            robot.stats = stats;

            promise.SetResult(message);
        }

        public void OnDisconnect(string instance, string message)
        {
            LoadGenerators.Remove(instance);
            Broadcast("Disconnected", instance + ": " + message);
            foreach (RobotInfo r in robots)
                if (r.instance == instance && r.promise != null)
                    r.promise.TrySetResult("ERROR: Lost Connection");
            ShowGenerators();
        }

        public void OnConnect(string instance, WebSocket ws)
        {
            LoadGenerators[instance] = ws;
            ShowGenerators();
        }

        public void ShowGenerators()
        {
            if (LoadGenerators.Count == 0)
                Broadcast("Connected Generators", "None");
            else
                Broadcast("Connected Generators (" + LoadGenerators.Count.ToString() + ")", string.Join(" ", LoadGenerators.Keys));
        }

        private async Task<string> RunScenario(IScenario scenario)
        {
            try
            {
                Broadcast("Start Scenario", benchmark.Name + "." + scenario.Name);

                return await scenario.ConductorScript(this);
            }
            catch (Exception e)
            {
                return "ERROR: exception " + e.ToString();
            }
            finally
            {
                Broadcast("End Scenario", benchmark.Name + "." + scenario.Name);
            }
        }

        public void WriteLine(string what)
        {
            Broadcast("", what);
        }

        public string ReadLine()
        {
            string command = null;

            lock (commands)
            {
                if (commands.Count > 0)
                    command = commands.Dequeue();
            }

            if (command == null)
            {
                //Broadcast("#>>>>", "Please enter a command");

                lock (commands)
                {
                    while (commands.Count == 0)
                        System.Threading.Monitor.Wait(commands);

                    command = commands.Dequeue();
                }
            }

            Broadcast("Entered", command);

            return command;
        }

        private Queue<string> commands = new Queue<string>();

        public void Typed(string command)
        {

            if (console == null)
            {
                Thread thread = new Thread(new ThreadStart(Run));
                thread.Start();

                return;
            }

            if (!string.IsNullOrEmpty(command))
                lock (commands)
                {
                    commands.Enqueue(command);
                    if (commands.Count == 1)
                        System.Threading.Monitor.PulseAll(commands);
                }


        }



        public async Task Trace(string info)
        {
            Broadcast("", info);
        }
    }
}