using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml;
using Orleans.Counters;
using Orleans.Management;
using Orleans.Runtime.MembershipService;



namespace Orleans.Runtime.Management
{
    /// <summary>
    /// Implementation class for the Orleans management grain.
    /// </summary>
    internal class OrleansManagementGrain : GrainBase, IOrleansManagementGrain
    {
        // NOTE: If ever this class / namespace is renamed, then a corresponding change needs to be made 
        //       in the Silo.CreateSystemGrains method in OrleansRuntime which uses classname string to
        //       precreate the runtime instances of this system grain in the silo.

        private OrleansLogger logger;
        private IMembershipTable membershipTable;

        public override Task ActivateAsync()
        {
            logger = Logger.GetLogger("OrleansManagementGrain", Logger.LoggerType.Runtime);
            return TaskDone.Done;
        }

        private async Task<IMembershipTable> GetMembershipTable()
        {
            if (membershipTable != null)
            {
                return membershipTable;
            }
            var config = await GetSiloControlReference(Silo.CurrentSilo.SiloAddress).GetGlobalConfig();

            if (config.LivenessType.Equals(GlobalConfiguration.LivenessProviderType.MembershipTableGrain))
            {
                membershipTable = MembershipTableFactory.Cast(GrainReference.FromGrainId(Constants.SystemMembershipTableId));
            }
            //else if (config.LivenessType.Equals(GlobalConfiguration.LivenessProviderType.File))
            //{
            //    membershipTable = new FileBasedMembershipTable(config.LivenessFileDirectory);
            //}
            else if (config.LivenessType.Equals(GlobalConfiguration.LivenessProviderType.AzureTable))
            {
                membershipTable = await AzureBasedMembershipTable.GetAzureBasedMembershipTable(config.DeploymentId, config.DataConnectionString);
            }
            return membershipTable;
        }

        public async Task<Dictionary<SiloAddress, SiloStatus>> GetHosts(bool onlyActive = false)
        {
            var mbrTable = await GetMembershipTable();
            var table = await mbrTable.ReadAll();
            
            var t = onlyActive ? 
                table.Members.Where(item => item.Item1.Status.Equals(SiloStatus.Active)).ToDictionary(item => item.Item1.SiloAddress, item => item.Item1.Status) :
                table.Members.ToDictionary(item => item.Item1.SiloAddress, item => item.Item1.Status);
            return t;
        }

        public Task SetSystemLogLevel(SiloAddress[] siloAddresses, int traceLevel)
        {
            var silos = GetSiloAddresses(siloAddresses);
            logger.Info("SetSystemTraceLevel={1} {0}", Utils.IEnumerableToString(silos), traceLevel);

            List<Task> actionPromises = PerformPerSiloAction(silos,
                s => GetSiloControlReference(s).SetSystemLogLevel(traceLevel));

            return Task.WhenAll(actionPromises);
        }

        public Task SetAppLogLevel(SiloAddress[] siloAddresses, int traceLevel)
        {
            var silos = GetSiloAddresses(siloAddresses);
            logger.Info("SetAppTraceLevel={1} {0}", Utils.IEnumerableToString(silos), traceLevel);

            List<Task> actionPromises = PerformPerSiloAction(silos,
                s => GetSiloControlReference(s).SetAppLogLevel(traceLevel));

            return Task.WhenAll(actionPromises);
        }

        public Task SetLogLevel(SiloAddress[] siloAddresses, string logName, int traceLevel)
        {
            var silos = GetSiloAddresses(siloAddresses);
            logger.Info("SetLogLevel[{1}]={2} {0}", Utils.IEnumerableToString(silos), logName, traceLevel);

            List<Task> actionPromises = PerformPerSiloAction(silos,
                s => GetSiloControlReference(s).SetLogLevel(logName, traceLevel));

            return Task.WhenAll(actionPromises);
        }

        public Task ForceGarbageCollection(SiloAddress[] siloAddresses)
        {
            var silos = GetSiloAddresses(siloAddresses);
            logger.Info("Forcing garbage collection on {0}", Utils.IEnumerableToString(silos));
            List<Task> actionPromises = PerformPerSiloAction(silos,
                s => GetSiloControlReference(s).ForceGarbageCollection());
            return Task.WhenAll(actionPromises);
        }

        public Task ForceActivationCollection(SiloAddress[] siloAddresses, TimeSpan ageLimit)
        {
            var silos = GetSiloAddresses(siloAddresses);
            return Task.WhenAll(GetSiloAddresses(silos).Select(s =>
                GetSiloControlReference(s).ForceActivationCollection(ageLimit)));
        }

        public async Task ForceActivationCollection(TimeSpan ageLimit)
        {
            Dictionary<SiloAddress, SiloStatus> hosts = await GetHosts(true);
            SiloAddress[] silos = hosts.Keys.ToArray();
            await ForceActivationCollection(silos, ageLimit);
        }

        public Task ForceRuntimeStatisticsCollection(SiloAddress[] siloAddresses)
        {
            var silos = GetSiloAddresses(siloAddresses);
            logger.Info("Forcing runtime statistics collection on {0}", Utils.IEnumerableToString(silos));
            List<Task> actionPromises = 
                PerformPerSiloAction(
                    silos,
                    s => 
                        GetSiloControlReference(s).ForceRuntimeStatisticsCollection());
            return Task.WhenAll(actionPromises);
        }


        public Task<SiloRuntimeStatistics[]> GetRuntimeStatistics(SiloAddress[] siloAddresses)
        {
            var silos = GetSiloAddresses(siloAddresses);
            if (logger.IsVerbose) logger.Verbose("GetRuntimeStatistics on {0}", Utils.IEnumerableToString(silos));
            List<Task<SiloRuntimeStatistics>> promises = new List<Task<SiloRuntimeStatistics>>();
            foreach (SiloAddress siloAddress in silos)
            {
                promises.Add(GetSiloControlReference(siloAddress).GetRuntimeStatistics());
            }
            return Task.WhenAll(promises);
        }

        private async Task<GrainStatistic[]> GetGrainStatistics(SiloAddress[] hostsIds)
        {
            var list = GetSiloAddresses(hostsIds)
                .Select(s => GetSiloControlReference(s).GetGrainStatistics()).ToList();

            await Task.WhenAll(list);

            return list.Select(a => a.Result).ToList()
                .SelectMany(a => a)
                .GroupBy(s => s)
                .Select(g => new GrainStatistic
                {
                    ActivationCount = g.Key.Item3,
                    GrainType = g.Key.Item2,
                    GrainCount = 1,
                    SiloCount = g.Count()
                })
                .GroupBy(s => new { s.ActivationCount, s.SiloCount, s.GrainType })
                .Select(g => new GrainStatistic
                {
                    ActivationCount = g.Key.ActivationCount,
                    GrainType = g.Key.GrainType,
                    GrainCount = g.Count(),
                    SiloCount = g.Key.SiloCount
                })
                .ToArray();
        }

        public async Task<SimpleGrainStatistic[]> GetSimpleGrainStatistics(SiloAddress[] hostsIds)
        {
            var all = GetSiloAddresses(hostsIds).Select(s =>
                GetSiloControlReference(s).GetSimpleGrainStatistics()).ToList();
            await Task.WhenAll(all);
            return all.SelectMany(s => s.Result).ToArray();
        }
        public async Task<SimpleGrainStatistic[]> GetSimpleGrainStatistics()
        {
            Dictionary<SiloAddress, SiloStatus> hosts = await GetHosts(true);
            SiloAddress[] silos = hosts.Keys.ToArray();
            return await GetSimpleGrainStatistics(silos);
        }

        public async Task<int> GetGrainActivationCount(GrainReference grainReference)
        {
            Dictionary<SiloAddress, SiloStatus> hosts = await GetHosts(true);
            List<SiloAddress> hostsIds = hosts.Keys.ToList();
            List<Task<DetailedGrainReport>> tasks = new List<Task<DetailedGrainReport>>();
            foreach (var silo in hostsIds)
            {
                tasks.Add(GetSiloControlReference(silo).GetDetailedGrainReport(grainReference.GrainId));
            }
            await Task.WhenAll(tasks);
            return tasks.Select(s => s.Result).Select(r => r.LocalActivations.Count).Sum();
        }

        public async Task UpdateConfiguration(SiloAddress[] hostIds, Dictionary<string,string> configuration, Dictionary<string,string> tracing)
        {
            var global = new[] { "Globals/", "/Globals/", "OrleansConfiguration/Globals/", "/OrleansConfiguration/Globals/" };
            if (hostIds != null && configuration.Keys.Any(k => global.Any(g => k.StartsWith(g))))
                throw new ArgumentException("Must update global configuration settings on all silos");
            var silos = GetSiloAddresses(hostIds);
            if (silos.Length == 0)
                return;

            var document = XPathValuesToXml(configuration);
            if (tracing != null)
            {
                AddXPathValue(document, new [] { "OrleansConfiguration", "Defaults", "Tracing"}, null);
                var parent = document["OrleansConfiguration"]["Defaults"]["Tracing"];
                foreach (var trace in tracing)
                {
                    var child = document.CreateElement("TraceLevelOverride");
                    child.SetAttribute("LogPrefix", trace.Key);
                    child.SetAttribute("TraceLevel", trace.Value);
                    parent.AppendChild(child);
                }
            }
            var sw = new StringWriter();
            document.WriteTo(new XmlTextWriter(sw));
            var xml = sw.ToString();
            // do first one, then all the rest to avoid spamming all the silos in case of a parameter error
            await GetSiloControlReference(silos[0]).UpdateConfiguration(xml);
            await Task.WhenAll(silos.Skip(1).Select(s =>
                    GetSiloControlReference(s).UpdateConfiguration(xml)));
        }

        public Task<GlobalConfiguration[]> GetGlobalConfiguration(SiloAddress[] hostsIds)
        {
            var all = GetSiloAddresses(hostsIds).Select(s =>
                GetSiloControlReference(s).GetGlobalConfig());
            return Task.WhenAll(all);
        }

        public Task<NodeConfiguration[]> GetNodeConfiguration(SiloAddress[] hostsIds)
        {
            IEnumerable<Task<NodeConfiguration>> all = GetSiloAddresses(hostsIds).Select(s =>
                GetSiloControlReference(s).GetLocalConfig());
            return Task.WhenAll(all);
        }

        private static SiloAddress[] GetSiloAddresses(SiloAddress[] silos)
        {
            if (silos != null && silos.Length > 0)
                return silos;
            return InsideGrainClient.Current.Catalog.SiloStatusOracle
                .GetApproximateSiloStatuses(true).Select(s => s.Key).ToArray();
        }

        /// <summary>
        /// Perform an action for each silo.
        /// </summary>
        /// <remarks>
        /// Because SiloControl contains a reference to a system target, each method call using that reference 
        /// will get routed either locally or remotely to the appropriate silo instance auto-magically.
        /// </remarks>
        /// <param name="siloAddresses">List of silos to perform the action for</param>
        /// <param name="perSiloAction">The action functiona to be performed for each silo</param>
        /// <returns>Array containing one AsyncCompletions for each silo the action was performed for</returns>
        private List<Task> PerformPerSiloAction(SiloAddress[] siloAddresses, Func<SiloAddress, Task> perSiloAction)
        {
            List<Task> requestsToSilos = new List<Task>();
            foreach (SiloAddress siloAddress in siloAddresses)
            {
                requestsToSilos.Add( perSiloAction(siloAddress) );
            }
            return requestsToSilos;
        }

        private static XmlDocument XPathValuesToXml(Dictionary<string,string> values)
        {
            var doc = new XmlDocument();
            if (values != null)
            {
                foreach (var p in values)
                {
                    var path = p.Key.Split('/').ToList();
                    if (path[0] == "")
                        path.RemoveAt(0);
                    if (path[0] != "OrleansConfiguration")
                        path.Insert(0, "OrleansConfiguration");
                    if (!path[path.Count - 1].StartsWith("@"))
                        throw new ArgumentException("XPath " + p.Key + " must end with @attribute");
                    AddXPathValue(doc, path, p.Value);
                }
            }
            return doc;
        }

        private static void AddXPathValue(XmlNode xml, IEnumerable<string> path, string value)
        {
            var first = path.FirstOrDefault();
            if (first == null)
                return;
            if (first.StartsWith("@"))
            {
                first = first.Substring(1);
                if (path.Count() != 1)
                    throw new ArgumentException("Attribute " + first + " must be last in path");
                var e = xml as XmlElement;
                if (e == null)
                    throw new ArgumentException("Attribute " + first + " must be on XML element");
                e.SetAttribute(first, value);
                return;
            }
            foreach (var child in xml.ChildNodes)
            {
                var e = child as XmlElement;
                if (e != null && e.LocalName == first)
                {
                    AddXPathValue(e, path.Skip(1), value);
                    return;
                }
            }
            var empty = (xml as XmlDocument ?? xml.OwnerDocument).CreateElement(first);
            xml.AppendChild(empty);
            AddXPathValue(empty, path.Skip(1), value);
        }

        private ISiloControl GetSiloControlReference(SiloAddress silo)
        {
            return SiloControlFactory.GetSystemTarget(Constants.SiloControlId, silo);
        }

        public async Task<int> GetTotalActivationCount()
        {
            Dictionary<SiloAddress, SiloStatus> hosts = await GetHosts(true);
            List<SiloAddress> silos = hosts.Keys.ToList();
            List<Task<int>> tasks = new List<Task<int>>();
            foreach (var silo in silos)
                tasks.Add(GetSiloControlReference(silo).GetActivationCount());
            await Task.WhenAll(tasks);
            int sum = 0;
            foreach (Task<int> task in tasks)
                sum += task.Result;
            return sum;
        }
    }
}
