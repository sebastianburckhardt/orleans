using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GeoOrleans.Runtime.ClusterProtocol.Interfaces;
using GeoOrleans.Runtime.Common;
using Newtonsoft.Json;
using System.Net;

namespace GeoOrleans.Runtime.ClusterProtocol.Grains
{
    public class ClusterRep : Orleans.Grain, IClusterRep
    {

        public async Task<Dictionary<string,DeploymentInfo>> GetGlobalInfo()
        {
            return GlobalInfo;
        }


        public Dictionary<string, DeploymentInfo> GlobalInfo { get; set; }


        private void Update()
        {

            // create string rep of availability of each resource
            var resources = new HashSet<string>();
            foreach (var b in buckets)
                foreach (var k in b.Value.Keys)
                    resources.Add(k);

            var d = new Dictionary<string, string>();

            var sortedresources = resources.ToList();
            sortedresources.Sort();
            foreach (var r in sortedresources)
            {
                var sb = new StringBuilder();
                foreach (var b in buckets)
                {
                    ActivityCounts c;
                    b.Value.TryGetValue(r, out c);
                    if (c.Fails > 0)
                    {
                        sb.Append(c.Uses - c.Fails);
                        sb.Append("/");
                    }
                    sb.Append(c.Uses);
                    sb.Append(" ");
                }
                d[r] = sb.ToString();
            }

            DeploymentInfo info;

            if (!GlobalInfo.TryGetValue(Util.MyDeploymentId, out info))
                info = GlobalInfo[Util.MyDeploymentId] = new DeploymentInfo()
                {
                    Deployment = Util.MyDeploymentId,
                    Instances = new Dictionary<string,InstanceInfo>()
                };

            info.ResourceAvailability = d;
            info.Timestamp = DateTime.UtcNow;
        }


        public LinkedList<KeyValuePair<DateTime, Dictionary<string, ActivityCounts>>> buckets
            = new LinkedList<KeyValuePair<DateTime, Dictionary<string, ActivityCounts>>>();


   
        private static DateTime RoundDown(DateTime dt, TimeSpan d)
        {
            var delta = dt.Ticks % d.Ticks;
            return new DateTime(dt.Ticks - delta, dt.Kind);
        }


        private void RecordResourceActivityCounts(Dictionary<string, ActivityCounts> counts)
        {
            // round down minute
            var currentbucket = RoundDown(DateTime.UtcNow, new TimeSpan(0, 1, 0));

            if (buckets.Count == 0 || buckets.Last.Value.Key != currentbucket)
            {
                buckets.AddLast(new KeyValuePair<DateTime, Dictionary<string, ActivityCounts>>(currentbucket, counts));

                if ((DateTime.UtcNow - buckets.First.Value.Key).TotalMinutes > 5)
                    buckets.RemoveFirst();
            }
            else
            {
                var d = buckets.Last.Value.Value;
                foreach (var s in counts)
                {
                    ActivityCounts c;
                    d.TryGetValue(s.Key, out c);
                    c.Uses += s.Value.Uses;
                    c.Fails += s.Value.Fails;
                    d[s.Key] = c;
                }
            }
        }

        public async Task ReportActivity(string instance, InstanceInfo instanceinfo, Dictionary<string, ActivityCounts> counts)
        {
            GlobalInfo[Util.MyDeploymentId].Instances[instance] = instanceinfo;

            RecordResourceActivityCounts(counts);
        }


        // post JSON gossip message containing configuration information
        public async Task<Dictionary<string, DeploymentInfo>> PostInfo(Dictionary<string, DeploymentInfo> info)
        {

            var returnedentries = new Dictionary<string, DeploymentInfo>();

            foreach (var kvp in info)
            {
                DeploymentInfo existing;

                // insert if not exist
                if (!GlobalInfo.TryGetValue(kvp.Key, out existing))
                    GlobalInfo[kvp.Key] = kvp.Value;

                else
                {
                    var comparison = existing.Timestamp.CompareTo(kvp.Value.Timestamp);

                    if (comparison < 0)
                    {
                        // received entry is newer
                        GlobalInfo[kvp.Key] = kvp.Value;
                    }
                    else if (comparison > 0)
                    {
                        // received entry is stale - send back existing
                        returnedentries[kvp.Key] = existing;
                    }
                }
            }

            foreach (var kvp in GlobalInfo)
                if (!info.ContainsKey(kvp.Key))
                    returnedentries[kvp.Key] = kvp.Value;

            return returnedentries;
        }


        // initialize with given JSON data
        public override async Task OnActivateAsync()
        {
            // don't deactivate this grain
            DelayDeactivation(new TimeSpan(long.MaxValue));

            GlobalInfo = new Dictionary<string, DeploymentInfo>();

            Update();

            RegisterTimer(async (o) =>
            {
                // record timer tick
                RecordActivity("timer [cluster rep]", false);

                Update();

                await Broadcast();

            }, null, new TimeSpan(0, 0, 0), new TimeSpan(0, 0, 1));
        }

        private void RecordActivity(string resource, bool fail)
        {
            var acd = new Dictionary<string, ActivityCounts>();
            acd[resource] = new ActivityCounts() { Fails = fail ? 1 : 0, Uses = 1 };
            RecordResourceActivityCounts(acd);
        }


        private Dictionary<string, Task> pendingsendtasks = new Dictionary<string, Task>();

        public async Task Broadcast()
        {

        //    if (Util.MyDeploymentId == "localdeployment")
                // protocol is no-op in the single-node local simulation
                return;

            var info = JsonConvert.SerializeObject(GlobalInfo).ToString();
            foreach (var e in AzureEndpoints.AllPublicEndpoints())
            {
                Task t;      
                if (!pendingsendtasks.TryGetValue(e, out t))
                    pendingsendtasks[e] = Send(e, info);
                else if (t.IsCompleted)
                    await t;
            }
        }


        public async Task Send(string endpoint, string info)
        {
            var resource = "send mgt info [" + endpoint + "]";
            try
            {
                var req = (HttpWebRequest)WebRequest.Create("http://" + endpoint + ":12121/mgt/info");

                req.Method = "POST";

                req.ContentType = "application/text";
                byte[] bytes = System.Text.Encoding.ASCII.GetBytes(info);
                req.ContentLength = bytes.Length;
                System.IO.Stream os = await req.GetRequestStreamAsync();
                await os.WriteAsync(bytes, 0, bytes.Length);
                os.Close();

                var resp = (HttpWebResponse)await req.GetResponseAsync();

                if (resp != null)
                {
                    System.IO.StreamReader sr = new System.IO.StreamReader(resp.GetResponseStream());
                    var responsetext = await sr.ReadToEndAsync();
                    var updates = JsonConvert.DeserializeObject<Dictionary<string, DeploymentInfo>>(responsetext);
                    foreach (var kvp in updates)
                    {
                        DeploymentInfo existing;
                        // insert if not exist
                        if (!GlobalInfo.TryGetValue(kvp.Key, out existing))
                            GlobalInfo[kvp.Key] = kvp.Value;
                        else if (existing.Timestamp.CompareTo(kvp.Value.Timestamp) < 0)
                            // received entry is newer
                            GlobalInfo[kvp.Key] = kvp.Value;
                    }
                }

                RecordActivity(resource, false);
            }
            catch (Exception e)
            {

                RecordActivity(resource, true);
            }

            pendingsendtasks.Remove(endpoint);
        }

    }
     
}