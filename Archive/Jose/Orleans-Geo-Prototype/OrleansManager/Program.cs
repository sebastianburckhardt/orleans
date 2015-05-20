using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using Orleans;
using Orleans.Counters;
using Orleans.Management;
using Orleans.Runtime;

namespace OrleansManager
{
    class Program
    {
        private static IOrleansManagementGrain SystemManagement;

        static void Main(string[] args)
        {
            Console.WriteLine("Invoked OrleansManager.exe with arguments {0}", Utils.IEnumerableToString(args));

            var command = args.Length > 0 ? args[0].ToLowerInvariant() : "";

            if (String.IsNullOrEmpty(command) || command.Equals("/?") || command.Equals("-?"))
            {
                PrintUsage();
                Environment.Exit(-1);
            }

            try
            {
                RunCommand(command, args);
                Environment.Exit(0);
            }
            catch (Exception exc)
            {
                Console.WriteLine("Terminating due to exception:");
                Console.WriteLine(exc.ToString());
                //Environment.Exit(1);
            }
        }

        private static void RunCommand(string command, string[] args)
        {
            OrleansClient.Initialize();

            SystemManagement = OrleansManagementGrainFactory.GetGrain(RuntimeInterfaceConstants.SystemManagementId);
            Dictionary<string, string> options = args.Skip(1)
                .Where(s => s.StartsWith("-"))
                .Select(s => s.Substring(1).Split('='))
                .ToDictionary(a => a[0].ToLowerInvariant(), a => a.Length > 1 ? a[1] : "");

            var restWithoutOptions = args.Skip(1).Where(s => !s.StartsWith("-")).ToArray();

            switch (command)
            {
                case "grainstats":
                    PrintSimpleGrainStatistics(restWithoutOptions);
                    break;

                case "fullgrainstats":
                    PrintGrainStatistics(restWithoutOptions);
                    break;

                case "collect":
                    CollectActivations(options, restWithoutOptions);
                    break;

                case "unregister":
                    var unregisterArgs = args.Skip(1).ToArray();
                    UnregisterGrain(unregisterArgs);
                    break;

                case "lookup":
                    var lookupArgs = args.Skip(1).ToArray();
                    LookupGrain(lookupArgs);
                    break;

                case "grainreport":
                    var grainReportArgs = args.Skip(1).ToArray();
                    GrainReport(grainReportArgs);
                    break;

                default:
                    PrintUsage();
                    break;
            }
        }

        private static void PrintUsage()
        {
            Console.WriteLine(@"Usage:
    OrleansManager grainstats [silo1 silo2 ...]
    OrleansManager fullgrainstats [silo1 silo2 ...]
    OrleansManager collect [-memory=nnn] [-age=nnn] [silo1 silo2 ...]
    OrleansManager unregister <grain interface type code (int)|grain implementation class name (string)> <grain id long|grain id Guid>
    OrleansManager lookup <grain interface type code (int)|grain implementation class name (string)> <grain id long|grain id Guid>
    OrleansManager grainReport <grain interface type code (int)|grain implementation class name (string)> <grain id long|grain id Guid>");
        }

        private static void CollectActivations(Dictionary<string, string> options, string[] args)
        {
            var silos = args.Select(ParseSilo).ToArray();
            int ageLimitSeconds = 0;
            string s;

            //if (options.TryGetValue("count", out s))
            //    Int32.TryParse(s, out activationLimit);
            if (options.TryGetValue("age", out s))
                Int32.TryParse(s, out ageLimitSeconds);

            TimeSpan ageLimit = TimeSpan.FromSeconds(ageLimitSeconds);
            if (ageLimit > TimeSpan.Zero)
                SystemManagement.ForceActivationCollection(silos, ageLimit);
            else
                SystemManagement.ForceGarbageCollection(silos);
        }

        private static void PrintSimpleGrainStatistics(string[] args)
        {
            var silos = args.Select(ParseSilo).ToArray();
            var stats = SystemManagement.GetSimpleGrainStatistics(silos).Result;
            Console.WriteLine("Silo                   Activations  Type");
            Console.WriteLine("---------------------  -----------  ------------");
            foreach (var s in stats.OrderBy(s => s.SiloAddress + s.GrainType))
            {
                Console.WriteLine("{0}  {1}  {2}", s.SiloAddress.ToString().PadRight(21), Pad(s.ActivationCount, 11), s.GrainType);
            }
        }
        
        private static void PrintGrainStatistics(string[] args)
        {
            var silos = args.Select(ParseSilo).ToArray();
            var stats = SystemManagement.GetSimpleGrainStatistics(silos).Result;
            Console.WriteLine("Act  Type");
            Console.WriteLine("--------  -----  ------  ------------");
            foreach (var s in stats.OrderBy(s => Tuple.Create(s.GrainType, s.ActivationCount)))
            {
                Console.WriteLine("{0}  {1}", Pad(s.ActivationCount, 8), s.GrainType);
            }
        }

        private static void GrainReport(string[] args)
        {
            GrainId grainId = ConstructGrainId(args, "GrainReport");

            List<SiloAddress> silos = GetSiloAddresses();
            if (silos == null || silos.Count == 0) return;

            List<DetailedGrainReport> reports = new List<DetailedGrainReport>();
            foreach (var silo in silos)
            {
                WriteStatus(string.Format("**Calling GetDetailedGrainReport({0}, {1})", silo, grainId));
                try
                {
                    ISiloControl siloControl = SiloControlFactory.GetSystemTarget(Constants.SiloControlId, silo);
                    DetailedGrainReport grainReport = siloControl.GetDetailedGrainReport(grainId).Result;
                    reports.Add(grainReport);
                }
                catch (Exception exc)
                {
                    WriteStatus(string.Format("**Failed to get grain report from silo {0}. Exc: {1}", silo, exc.ToString()));
                }
            }
            foreach (var grainReport in reports)
            {
                WriteStatus(grainReport.ToString());
            }

            LookupGrain(args);
        }

        private static void UnregisterGrain(string[] args)
        {
            GrainId grainId = ConstructGrainId(args, "unregister");

            SiloAddress silo = GetSiloAddress();
            if (silo == null)
            {
                return;
            }

            IRemoteGrainDirectory directory = RemoteGrainDirectoryFactory.GetSystemTarget(Constants.DirectoryServiceId, silo);
            int retries = 3;

            WriteStatus(string.Format("**Calling DeleteGrain({0}, {1}, {2})", silo, grainId, retries));
            directory.DeleteGrain(grainId, retries).Wait();
            WriteStatus(string.Format("**DeleteGrain finished OK."));
        }

        private static async void LookupGrain(string[] args)
        {
            GrainId grainId = ConstructGrainId(args, "lookup");

            SiloAddress silo = GetSiloAddress();
            if (silo == null)
            {
                return;
            }

            IRemoteGrainDirectory directory = RemoteGrainDirectoryFactory.GetSystemTarget(Constants.DirectoryServiceId, silo);
            int retries = 3;

            WriteStatus(string.Format("**Calling LookupGrain({0}, {1}, {2})", silo, grainId, retries));
            Tuple<List<Tuple<SiloAddress, ActivationId>>, int> lookupResult = await directory.LookUp(grainId, retries);
            WriteStatus(string.Format("**LookupGrain finished OK. Lookup result is:"));
            List<Tuple<SiloAddress, ActivationId>> list = lookupResult.Item1;
            if (list == null)
            {
                WriteStatus(string.Format("**The returned activation list is null."));
                return;
            }
            if (list.Count == 0)
            {
                WriteStatus(string.Format("**The returned activation list is empty."));
                return;
            }
            Console.WriteLine("**There {0} {1} activations registered in the directory for this grain. The activations are:", (list.Count > 1) ? "are" : "is", list.Count);
            foreach (Tuple<SiloAddress, ActivationId> tuple in list)
            {
                WriteStatus(string.Format("**Activation {0} on silo {1}", tuple.Item2, tuple.Item1));
            }
        }

        private static GrainId ConstructGrainId(string[] args, string operation)
        {
            if (args == null || args.Length < 2)
            {
                PrintUsage();
                return null;
            }
            string interfaceTypeCodeOrImplClassName = args[0];
            int interfaceTypeCodeDataLong = 0;
            long implementationTypeCode = 0;

            if (Int32.TryParse(interfaceTypeCodeOrImplClassName, out interfaceTypeCodeDataLong))
            {
                // parsed it as int, so it is an interface type code.
                implementationTypeCode = TypeCodeMapper.GetImplementationTypeCode(interfaceTypeCodeDataLong);
            }
            else
            {
                // interfaceTypeCodeOrImplClassName is the implementation class name
                implementationTypeCode = TypeCodeMapper.GetImplementationTypeCode(interfaceTypeCodeOrImplClassName);
            }

            string grainIdStr = args[1];
            GrainId grainId = null;
            long grainIdLong;
            Guid grainIdGuid;
            if (Int64.TryParse(grainIdStr, out grainIdLong))
            {
                grainId = GrainId.GetGrainId(implementationTypeCode, grainIdLong);
            }
            else if (Guid.TryParse(grainIdStr, out grainIdGuid))
            {
                grainId = GrainId.GetGrainId(implementationTypeCode, grainIdGuid);
            }

            // [mlr][todo] the team agreed that OrleansManager won't be extended to support UniqueKey key extensions right now.
            WriteStatus(string.Format("**Full Grain Id to {0} is: GrainId = {1}",
               operation,
               grainId.ToFullString()));

            return grainId;
        }

        private static SiloAddress GetSiloAddress()
        {
            List<SiloAddress> silos = GetSiloAddresses();
            if (silos == null || silos.Count==0) return null;
            return silos.FirstOrDefault();
        }

        private static List<SiloAddress> GetSiloAddresses()
        {
            List<IPEndPoint> gws = OrleansClient.Gateways;
            if (gws.Count < 1)
            {
                WriteStatus(string.Format("**Retrieved only zero gateways from OrleansClient.Gateways"));
                return null;
            }
            return gws.Select(ip => SiloAddress.New(ip, 0)).ToList();
        }

        //private static List<SiloAddress> GetSiloAddresses_2()
        //{
        //    Dictionary<SiloAddress, SiloStatus> silos = SystemManagement.GetHosts(true).GetValue();
        //    if (silos.Count < 1)
        //    {
        //        Console.WriteLine("**Retrieved only zero silos from SystemManagement.GetHosts() call");
        //        return null;
        //    }
        //    return silos.Keys.ToList();
        //}

        private static string Pad(int value, int width)
        {
            return value.ToString("d").PadRight(width);
        }

        private static SiloAddress ParseSilo(string s)
        {
            return SiloAddress.FromParsableString(s);
        }

        public static void WriteStatus(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(msg);
            Console.ResetColor();
        }
    }
}


