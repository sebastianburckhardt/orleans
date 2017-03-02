/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

 using System;
using System.IO;
using System.Net;
using System.Runtime;
using System.Threading;
using System.Globalization;

using Orleans.Runtime.Configuration;
using Orleans.Runtime;


namespace Orleans.Transactions.Host
{
    /// <summary>
    /// Allows programmatically hosting an Orleans TM in the curent app domain.
    /// </summary>
    public class TransactionManagerHost : MarshalByRefObject, IDisposable
    {
        /// <summary> Name of this TM. </summary>
        public string Name { get; set; }

        /// <summary>
        /// Configuration file used for this TM.
        /// Changing this after the TM has started (when <c>ConfigLoaded == true</c>) will have no effect.
        /// </summary>
        public string ConfigFileName { get; set; }

        /// <summary>
        /// Directory to use for the trace log file written by this silo.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The values of <c>null</c> or <c>"None"</c> mean no log file will be written by Orleans Logger manager.
        /// </para>
        /// <para>
        /// When deciding The values of <c>null</c> or <c>"None"</c> mean no log file will be written by Orleans Logger manager.
        /// </para>
        /// </remarks>
        public string TraceFilePath { get; set; }

        /// <summary> Configuration data for the Orleans system. </summary>
        public ClusterConfiguration Config { get; set; }

        /// <summary>
        /// Whether the config has been loaded and initializing it's runtime config.
        /// </summary>
        /// <remarks>
        /// Changes to config properties will be ignored after <c>ConfigLoaded == true</c>.
        /// </remarks>
        public bool ConfigLoaded { get; private set; }

        /// <summary> Deployment Id (if any) for the cluster this silo is running in. </summary>
        public string DeploymentId { get; set; }

        /// <summary> Whether this TM started successfully and is currently running. </summary>
        public bool IsStarted { get; private set; }

        private OrleansClientTransactionService orleans;
        private EventWaitHandle startupEvent;
        private bool disposed;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tmName">Name of this TM.</param>
        public TransactionManagerHost(string tmName)
        {
            Name = tmName;
            IsStarted = false;
        }

        /// <summary> Constructor </summary>
        /// <param name="tmName">Name of this TM.</param>
        /// <param name="configFile">Config file that will be used to initialize this TM.</param>
        public TransactionManagerHost(string tmName, FileInfo configFile)
            : this(tmName)
        {
            ConfigFileName = configFile.FullName;
            var config = new ClusterConfiguration();
            config.LoadFromFile(ConfigFileName);
            SetTMConfig(config);
        }

        /// <summary>
        /// Initialize this TM.
        /// </summary>
        public void InitializeOrleansTM()
        {
            try
            {
                if (!ConfigLoaded) LoadOrleansConfig();
                
                orleans = new OrleansClientTransactionService(Config.Globals.Transactions);
            }
            catch (Exception exc)
            {
                ReportStartupError(exc);
                orleans = null;
            }
        }

        /// <summary>
        /// Start this TM.
        /// </summary>
        /// <returns></returns>
        public bool StartOrleansTM()
        {
            try
            {
                if (string.IsNullOrEmpty(Thread.CurrentThread.Name))
                    Thread.CurrentThread.Name = this.GetType().Name;
                
                if (orleans != null)
                {
                    orleans.Start();
                    
                    var startupEventName = Name;

                    bool createdNew;
                    startupEvent = new EventWaitHandle(true, EventResetMode.ManualReset, startupEventName, out createdNew);
                    if (!createdNew)
                    {
                        startupEvent.Set();
                    }

                    IsStarted = true;
                }
                else
                {
                    throw new InvalidOperationException("Cannot start TM " + this.Name + " due to prior initialization error");
                }
            }
            catch (Exception exc)
            {
                ReportStartupError(exc);
                orleans = null;
                IsStarted = false;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Stop this silo.
        /// </summary>
        public void StopOrleansTM()
        {
            IsStarted = false;
            if (orleans != null) orleans.Stop();
        }

        /// <summary>
        /// Wait for this TM to shutdown.
        /// </summary>
        /// <remarks>
        /// Note: This method call will block execution of current thread, 
        /// and will not return control back to the caller until the TM is shutdown.
        /// </remarks>
        public void WaitForOrleansTMShutdown()
        {
            if (!IsStarted)
                throw new InvalidOperationException("Cannot wait for TM " + this.Name + " since it was not started successfully previously.");
            
            if (startupEvent != null)
                startupEvent.Reset();
            else
                throw new InvalidOperationException("Cannot wait for TM " + this.Name + " due to prior initialization error");
            
            if (orleans != null)
                orleans.ServiceTerminatedEvent.WaitOne();
            else
                throw new InvalidOperationException("Cannot wait for silo " + this.Name + " due to prior initialization error");
        }

        /// <summary>
        /// Set the DeploymentId for this silo, 
        /// as well as the Azure connection string to use the silo system data, 
        /// such as the cluster membership table..
        /// </summary>
        /// <param name="deploymentId">DeploymentId this silo is part of.</param>
        /// <param name="connectionString">Azure connection string to use the silo system data.</param>
        public void SetDeploymentId(string deploymentId, string connectionString)
        {
            Config.Globals.DeploymentId = deploymentId;
            Config.Globals.Transactions.DataConnectionString = connectionString;
        }

        /// <summary>
        /// Report an error during TM startup.
        /// </summary>
        /// <remarks>
        /// Information on the TM startup issue will be logged to any attached Loggers,
        /// then a timestamped StartupError text file will be written to 
        /// the current working directory (if possible).
        /// </remarks>
        /// <param name="exc">Exception which caused the TM startup issue.</param>
        public void ReportStartupError(Exception exc)
        {
            // TODO: fill this in
        }

        /// <summary>
        /// Search for and load the config file for this silo.
        /// </summary>
        public void LoadOrleansConfig()
        {
            if (ConfigLoaded) return;

            var config = Config ?? new ClusterConfiguration();

            try
            {
                if (ConfigFileName == null)
                    config.StandardLoad();
                else
                    config.LoadFromFile(ConfigFileName);
            }
            catch (Exception ex)
            {
                throw new AggregateException("Error loading Config file: " + ex.Message, ex);
            }

            SetTMConfig(config);
        }

        /// <summary>
        /// Allows TM config to be programmatically set.
        /// </summary>
        /// <param name="config">Configuration data.</param>
        private void SetTMConfig(ClusterConfiguration config)
        {
            Config = config;
            
            if (!String.IsNullOrEmpty(DeploymentId))
                Config.Globals.DeploymentId = DeploymentId;
            
            if (string.IsNullOrWhiteSpace(Name))
                throw new ArgumentException("TM Name not defined - cannot initialize config");

            ConfigLoaded = true;
        }

        /// <summary>
        /// Called when this silo is being Disposed by .NET runtime.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    if (startupEvent != null)
                    {
                        startupEvent.Dispose();
                        startupEvent = null;
                    }
                    this.IsStarted = false;
                }
            }
            disposed = true;
        }
    }
}
