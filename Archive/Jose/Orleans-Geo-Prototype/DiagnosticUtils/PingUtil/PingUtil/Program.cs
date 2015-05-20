using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace PingUtil
{
    class Program
    {
        public static void Main(string[] args)
        {
            int sleep = 60;
            string connectionString = null;
            string deploymentId = null;
            string eventsource = "Windows Error Reporting";
            Dictionary<string, IPEndPoint> addresses = new Dictionary<string, IPEndPoint>();
            string logFileName = string.Format(@"PingUtilLog-{0}-{1}-{2}.log", Environment.UserName, Environment.MachineName, DateTime.UtcNow.ToString("d-MMM-yyyy-HH-mm-ss"));

            if (ParseFromCommandLine(args, addresses,
                ref sleep,
                ref logFileName,
                ref connectionString,
                ref deploymentId,
                ref eventsource))
            {
                string hostname = Dns.GetHostName();
                hostname = hostname.ToUpper();
                //Console.WriteLine("Running on machine {0} with hostname {1}", Environment.MachineName, hostname);
                IPEndPoint self = FindEndPoint(Agent.PING_PORT, hostname);

                Log.Init(Log.Severity.Info, hostname, logFileName, eventsource);
                Log.Write(Log.Severity.Info, "PingUtil running on machine {0} with hostname {1} with commandline = {2}", Environment.MachineName, hostname, string.Join(" ", args));
                Log.Write(Log.Severity.Info, "PingUtil now running with following options:\nHost = {0}\nMy IP Address = {1}\nDeploymentId = {2}\nSleep= {3}\n" +
                                    "Port = {4}\nConnectionString = {5}\nlogFile = {6}\n",
                    hostname,
                    self,
                    deploymentId,
                    sleep,
                    Agent.PING_PORT,
                    connectionString,
                    logFileName);

                Log.Write(Log.Severity.Info, "Starting the agent");
                Agent agent = new Agent(hostname, self, addresses, TimeSpan.FromSeconds(sleep), connectionString, deploymentId);
                agent.Listen();
                agent.PingOthers();
            }
        }

        public static void PrintHelp(string logfile)
        {
            Console.WriteLine("PingUtil <-servers | -azure | -auto> [options]");
            Console.WriteLine("     -servers:<host1, host2, ..., hostn>  [List of machines to ping]");
            Console.WriteLine("     -auto                   [Use when running in Azure. Will detect Azure deployment id and data connection string automatically]");
            Console.WriteLine("     -azure:<deployment-id>  [Specify Azure deployment id manually. Specify Data Connection string in the PingUtil.exe.config file by adding “DataConnectionString” under appSettings]");
            Console.WriteLine("     -port:<n>               [optional. Port to use for ping communication. Default is " + Agent.PING_PORT + "]");
            Console.WriteLine("     -sleep:<n>              [optional. Number of seconds to wait before next ping. Default is 60]");
            Console.WriteLine("     -logfile:<file_name>    [optional. If not specified, will use: " + logfile + "]");
            Console.WriteLine("     -eventsource:<name>     [optional]");
        }

        public static bool ParseFromCommandLine(string[] args, Dictionary<string, IPEndPoint> addresses , 
            ref int sleep, 
            ref string logFileName,
            ref string connectionString,
            ref string deploymentId,
            ref string eventsource)
        {
            if (args == null || args.Length == 0 || args[0].Equals("-?") || args[0].Equals("-help") || args[0].Equals("/?") || args[0].Equals("/help"))
            {
                PrintHelp(logFileName);
                return false;
            }

            bool configuredCorrectly = false;
            foreach (string arg in args)
            {
                if (arg.StartsWith("-"))
                {
                    if (arg.StartsWith("-servers:", StringComparison.CurrentCultureIgnoreCase))
                    {
                        string server = CutPreembale(arg, "-servers:").ToUpper();
                        var endpoint = FindEndPoint(Agent.PING_PORT, server);
                        if (null != endpoint && server != Environment.MachineName.ToUpper())
                        {
                            Console.WriteLine("My peer is : {0} on {1}", endpoint, server.ToUpper());
                            addresses.Add(server, endpoint);
                        }
                        configuredCorrectly = true;
                    }
                    if (arg.StartsWith("-azure:", StringComparison.CurrentCultureIgnoreCase))
                    {
                        connectionString = GetConfigFlag("DataConnectionString", string.Empty);
                        deploymentId = CutPreembale(arg, "-azure:");
                        configuredCorrectly = true;
                    }
                    if (arg.StartsWith("-auto", StringComparison.CurrentCultureIgnoreCase))
                    {
                        Console.WriteLine("Attempting to automatically detect azure settings.");

                        if (RoleEnvironment.IsAvailable)
                        {
                            if (RoleEnvironment.IsEmulated)
                            {
                                // Disable this utility in dev fabric.
                                Console.WriteLine("Dev fabric detected. Exiting.");
                                return false;
                            }
                            connectionString = RoleEnvironment.GetConfigurationSettingValue("dataConnectionString");
                            deploymentId = RoleEnvironment.DeploymentId;
                            configuredCorrectly = true;
                        }
                        else
                        {
                            Console.WriteLine("-auto switch used, but appears to not be running in Azure.");
                            return false;
                        }
                    }
                    if (arg.StartsWith("-sleep:", StringComparison.CurrentCultureIgnoreCase))
                    {
                        sleep = int.Parse(CutPreembale(arg, "-sleep:"));
                    }
                    if (arg.StartsWith("-logfile:", StringComparison.CurrentCultureIgnoreCase))
                    {
                        logFileName = CutPreembale(arg, "-logfile:");
                    }
                    if (arg.StartsWith("-eventsource:", StringComparison.CurrentCultureIgnoreCase))
                    {
                        eventsource = CutPreembale(arg, "-eventsource:");
                    }
                    if (arg.StartsWith("-port:", StringComparison.CurrentCultureIgnoreCase))
                    {
                        Agent.PING_PORT = int.Parse(CutPreembale(arg, "-port:"));
                    }
                }
            }
            if (!configuredCorrectly)
            {
                PrintHelp(logFileName);
                return false;
            }
            return true;
        }

        private static string CutPreembale(string input, string preemable)
        {
            string output = input.Substring(input.IndexOf(preemable) + preemable.Length);
            if (string.IsNullOrEmpty(output))
            {
                throw new ArgumentException(String.Format("Wrong argument format. Argument {0} is not followed immediately by a value. There should be no spaces after {0}", preemable));
            }
            return output;
        }


        private static string GetConfigFlag(string name, string defaultValue)
        {
            string result = ConfigurationManager.AppSettings[name];
            if (!string.IsNullOrWhiteSpace(result))
                return result;
            else
                return defaultValue;
        }

        private static IPEndPoint FindEndPoint(int port, string server)
        {
            // Obtain the IP address from the list of IP addresses associated with the server.
            IPAddress[] nodeIps = Dns.GetHostAddresses(server);
            foreach (IPAddress address in nodeIps)
            {
                //Console.WriteLine("Considering address : {0} on {1}", address, server);
                // only IP addresses
                if (address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.Loopback.Equals(address))
                {
                    return new IPEndPoint(address, port);
                }
            }
            return null;
        }

        //private static void Simulate()
        //{
        //    Log.Init(Log.Severity.Info, Environment.MachineName.ToUpper(), "Foo.txt");
        //    List<Task> tasks = new List<Task>();
        //    Dictionary<string, IPEndPoint> addresses = new Dictionary<string, IPEndPoint>();
        //    Dictionary<string, Agent> agents = new Dictionary<string, Agent>();
        //    for (int i = 0; i < 5; i++)
        //    {
        //        string name = string.Format("Instance{0}", i);
        //        IPEndPoint endPoint = new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 3000 + i);
        //        addresses.Add(name, endPoint);
        //        Agent agent = new Agent(name, endPoint, addresses, TimeSpan.FromSeconds(60), null, null);
        //        agents.Add(name, agent);
        //    }
        //    foreach (string name in agents.Keys)
        //    {
        //        Agent agent = agents[name];
        //        agent.Listen();
        //    }
        //    foreach (string name in agents.Keys)
        //    {
        //        Agent agent = agents[name];
        //        tasks.Add(Task.Factory.StartNew(() =>
        //        {
        //            agent.PingOthers();
        //        }));
        //    }
        //    Task.WaitAll(tasks.ToArray());
        //}
    }
}
