using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.ServiceRuntime;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.Threading.Tasks;
using System.IO;
using Orleans;
using Orleans.Runtime.Host;

namespace Orleans.Frontend
{
    public class WorkerRole : RoleEntryPoint
    {
        public override void Run()
        {
            deploymentid = GetAlphaHash(deployment.GetHashCode(), 5);
            instanceid = deploymentid + instance.Substring(instance.LastIndexOf('_') + 1);

            diag("Portal: Starting Server");

            StartServices();

            diag("Portal: Running");

            while (true)
            {
                Thread.Sleep(10000);

                //CheckHealth();

            }
        }

        public static string GetAlphaHash(int seed, int length)
        {
            System.Text.StringBuilder b = new System.Text.StringBuilder();
            var random = new Random(seed);
            for (int i = 0; i < length; i++)
                b.Append((char)((int)'a' + random.Next(26)));
            return b.ToString();
        }

             /*
       public void CheckHealth()
        {
            if (currentserver == null)
                return;

            try
            {
                var problems = currentserver.IsDown();

                if (problems != null)
                {
                    try
                    {
                        var server = currentserver;
                        currentserver = null;
                        diag("##### Frontend: Problems Detected on Server " + server.GetIdentity() + ": " + problems);
                        diag("Frontend: Restarting " + server.GetIdentity());
                        server.Stop("Frontend: Restarting Server", true);
                        diag("Portal: Restarting " + server.GetIdentity() + " in 10 seconds");
                        Thread.Sleep(10000);
                        this.StartNewServer();
                    }
                    catch (Exception e)
                    {
                        diag("Portal: Failed to restart (Exception: " + e.Message + "). Requesting role recycle.");
                        RoleEnvironment.RequestRecycle();
                    }
                }
            }
            catch (Exception e)
            {
                diag("Portal: exception in health check: " + e);
            }

        }
             * */

        public override void OnStop()
        {
            diag("Portal: Stopping " + (currentserver != null ? currentserver.GetIdentity() : ""));

            if (currentserver != null)
            {
                var server = currentserver;
                currentserver = null;
                server.Stop();
            }

            diag("Portal: Stopping " + (currentcmserver != null ? currentcmserver.GetIdentity() : ""));

            if (currentcmserver != null)
            {
                var cmserver = currentcmserver;
                currentcmserver = null;
                cmserver.Stop();
            }


            diag("Portal: Stopped");


            base.OnStop();
        }

        /// <summary>
        /// low-level trace stuff
        /// </summary>
        /// <param name="s"></param>
        public void tracer(string s)
        {
            if (!runningincloud)
                Trace.WriteLine(s);
        }

        /// <summary>
        /// diagnostic information
        /// </summary>
        /// <param name="s"></param>
        public void diag(string s)
        {
            Trace.WriteLine(s);

            // may want to add this to some more visible log
        }


        public override bool OnStart()
        {
            diag("Portal: WorkerRole.OnStart Called");

            // Set the maximum number of concurrent connections 
            ServicePointManager.DefaultConnectionLimit = 4000; // DOES THIS MAKE SENSE?

            // get info
            deployment = RoleEnvironment.DeploymentId;
            instance = RoleEnvironment.CurrentRoleInstance.Id;

            // check if we are running in cloud or in simulator
            runningincloud = RoleEnvironment.GetConfigurationSettingValue("InCloud") == "true";

            return base.OnStart();
        }

        internal string deployment;
        internal string deploymentid;
        internal string instance;
        internal string instanceid;
        internal bool runningincloud;
 

        internal Benchmarks.Server currentserver;
        internal ClusterProtocolServer currentcmserver;

        public void StartServices()
        {
            try
            {
                // start orleans client
                diag("Portal: Starting Orleans Client");
                if (!AzureClient.IsInitialized)
                {
                    FileInfo clientConfigFile = AzureConfigUtils.ClientConfigFileLocation;
                    if (!clientConfigFile.Exists)
                    {
                        throw new FileNotFoundException(string.Format("Cannot find Orleans client config file for initialization at {0}", clientConfigFile.FullName), clientConfigFile.FullName);
                    }
                    AzureClient.Initialize(clientConfigFile);
                }
                diag("Portal: Orleans Client Started.");


                // start service endpoint
                var endpointdescriptor = RoleEnvironment.CurrentRoleInstance.InstanceEndpoints["frontend"];
                var securehttp = (endpointdescriptor.Protocol == "https");
                var port = endpointdescriptor.IPEndpoint.Port.ToString();
                var endpoint = endpointdescriptor.Protocol + "://+:" + port + "/";
                diag("Portal: Launching " + (runningincloud ? "deployed " : "local") + " service at " + endpoint);
                currentserver = new Benchmarks.Server(
                              RoleEnvironment.DeploymentId,
                              instanceid,
                              runningincloud,
                              securehttp,
                              this.tracer,
                              this.diag
                              );
                currentserver.Start(endpoint);
                diag("Portal: Service started at endpoint: " + endpoint);

                // start cluster mgt endpoint
                var cmendpointdescriptor = RoleEnvironment.CurrentRoleInstance.InstanceEndpoints["clustermgt"];
                var cmsecurehttp = (cmendpointdescriptor.Protocol == "https");
                var cmport = cmendpointdescriptor.IPEndpoint.Port.ToString();
                var cmendpoint = cmendpointdescriptor.Protocol + "://+:" + cmport + "/";
                diag("Portal: Launching cluster mgt service at " + cmendpoint);
                currentcmserver = new ClusterProtocolServer(
                      RoleEnvironment.DeploymentId,
                      instanceid,
                      runningincloud,
                      cmsecurehttp,
                      this.tracer,
                      this.diag
                      );
                currentcmserver.Start(cmendpoint);
                diag("Portal: CM Service started at endpoint: " + cmendpoint);

                diag("Portal: Startup complete");
            }
            catch (Exception e)
            {
                diag("Portal: failed to start: " + e.ToString());
            }
        }
    }
}

