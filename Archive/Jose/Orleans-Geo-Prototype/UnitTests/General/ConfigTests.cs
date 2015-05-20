using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Providers;

using UnitTests.StorageTests;

// ReSharper disable RedundantTypeArgumentsOfMethod
// ReSharper disable CheckNamespace
// ReSharper disable ConvertToConstant.Local

namespace UnitTests
{
    [TestClass]
    [DeploymentItem("Config_LogConsumers-OrleansConfiguration.xml")]
    [DeploymentItem("Config_LogConsumers-ClientConfiguration.xml")]
    [DeploymentItem("Config_NonTimestampedLogFileNames.xml")]
    [DeploymentItem("Config_TestSiloConfig.xml")]
    [DeploymentItem("Config_StorageProvider1.xml")]
    [DeploymentItem("Config_StorageProvider2.xml")]
    [DeploymentItem("Config_StorageProvider_Default.xml")]
    [DeploymentItem("Config_StorageProvider_NoConfig.xml")]
    [DeploymentItem("Config_StorageProvider_SomeConfig.xml")]
    public class ConfigTests
    {
        [TestInitialize]
        public void TestInitialize()
        {
            Logger.UnInitialize();
            LimitManager.UnInitialize();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            Logger.UnInitialize();
            LimitManager.UnInitialize();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Config_NewConfigTest()
        {
            TextReader input = File.OpenText("Config_TestSiloConfig.xml");
            OrleansConfiguration config = new OrleansConfiguration();
            config.Load(input);
            input.Close();

            Assert.AreEqual<int>(2, config.Globals.SeedNodes.Count, "Seed node count is incorrect");
            Assert.AreEqual<IPEndPoint>(new IPEndPoint(IPAddress.Loopback, 11111), config.Globals.SeedNodes[0], "First seed node is set incorrectly");
            Assert.AreEqual<IPEndPoint>(new IPEndPoint(IPAddress.IPv6Loopback, 22222), config.Globals.SeedNodes[1], "Second seed node is set incorrectly");

            Assert.AreEqual<int>(12345, config.Defaults.Port, "Default port is set incorrectly");

            NodeConfiguration nc = config.GetConfigurationForNode("Node1");
            Assert.AreEqual<int>(11111, nc.Port, "Port is set incorrectly for node Node1");
            Assert.IsTrue(nc.IsPrimaryNode, "Node1 should be primary node");
            Assert.IsTrue(nc.IsSeedNode, "Node1 should be seed node");
            Assert.IsFalse(nc.IsGatewayNode, "Node1 should not be gateway node");

            nc = config.GetConfigurationForNode("Node2");
            Assert.AreEqual<int>(22222, nc.Port, "Port is set incorrectly for node Node2");
            Assert.IsFalse(nc.IsPrimaryNode, "Node2 should not be primary node");
            Assert.IsTrue(nc.IsSeedNode, "Node2 should be seed node");
            Assert.IsTrue(nc.IsGatewayNode, "Node2 should be gateway node");

            nc = config.GetConfigurationForNode("Store");
            Assert.AreEqual<int>(12345, nc.Port, "IP port is set incorrectly for node Store");
            Assert.IsFalse(nc.IsPrimaryNode, "Store should not be primary node");
            Assert.IsFalse(nc.IsSeedNode, "Store should not be seed node");
            Assert.IsFalse(nc.IsGatewayNode, "Store should not be gateway node");

            //IPAddress[] ips = Dns.GetHostAddresses("");
            //IPEndPoint ep = new IPEndPoint(IPAddress.Loopback, 12345);
            //for (int i = 0; i < ips.Length; i++)
            //{
            //    if ((ips[i].AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork) && !IPAddress.Loopback.Equals(ips[i]))
            //    {
            //        ep = new IPEndPoint(ips[i], 12345);
            //        break;
            //    }
            //}

            //Assert.AreEqual<IPEndPoint>(ep, nc.Endpoint, "IP endpoint is set incorrectly for node Store");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void LogFileName()
        {
            var oc = new OrleansConfiguration();
            oc.StandardLoad();
            var n = oc.GetConfigurationForNode("Node1");
            string fname = n.TraceFileName;
            Console.WriteLine("LogFileName = " + fname);
            Assert.IsNotNull(fname);
            Assert.IsFalse(fname.Contains(":"), "Log file name should not contain colons.");

            // Check that .NET is happy with the file name
            var f = new FileInfo(fname);
            Assert.IsNotNull(f.Name);
            Assert.AreEqual(fname, f.Name);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void NodeLogFileName()
        {
            string siloName = "MyNode1";
            string baseLogFileName = siloName + ".log";
            string baseLogFileNamePlusOne = siloName + "-1.log";
            string expectedLogFileName = baseLogFileName;
            string configFileName = "Config_NonTimestampedLogFileNames.xml";

            if (File.Exists(baseLogFileName)) File.Delete(baseLogFileName);
            if (File.Exists(expectedLogFileName)) File.Delete(expectedLogFileName);

            var config = new OrleansConfiguration();
            config.LoadFromFile(configFileName);
            var n = config.GetConfigurationForNode(siloName);
            string fname = n.TraceFileName;
            Console.WriteLine("LogFileName = " + fname);
            
            Assert.AreEqual(baseLogFileName, fname);

            Logger.Initialize(n);

            Assert.IsTrue(File.Exists(baseLogFileName), "Base name log file exists: " + baseLogFileName);
            Assert.IsTrue(File.Exists(expectedLogFileName), "Expected name log file exists: " + expectedLogFileName);
            Assert.IsFalse(File.Exists(baseLogFileNamePlusOne), "Munged log file exists: " + baseLogFileNamePlusOne);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void NodeLogFileNameAlreadyExists()
        {
            string siloName = "MyNode2";
            string baseLogFileName = siloName + ".log";
            string baseLogFileNamePlusOne = siloName + "-1.log";
            string expectedLogFileName = baseLogFileName;
            string configFileName = "Config_NonTimestampedLogFileNames.xml";

            if (File.Exists(baseLogFileName)) File.Delete(baseLogFileName);
            if (File.Exists(expectedLogFileName)) File.Delete(expectedLogFileName);

            if (!File.Exists(baseLogFileName)) File.Create(baseLogFileName);

            var config = new OrleansConfiguration();
            config.LoadFromFile(configFileName);
            var n = config.GetConfigurationForNode(siloName);
            string fname = n.TraceFileName;
            Console.WriteLine("LogFileName = " + fname);

            Assert.AreEqual(baseLogFileName, fname);

            Logger.Initialize(n);

            Assert.IsTrue(File.Exists(baseLogFileName), "Base name log file exists: " + baseLogFileName);
            Assert.IsTrue(File.Exists(expectedLogFileName), "Expected name log file exists: " + expectedLogFileName);
            Assert.IsFalse(File.Exists(baseLogFileNamePlusOne), "Munged log file exists: " + baseLogFileNamePlusOne);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void LogFile_Write_AlreadyExists()
        {
            const string siloName = "MyNode3";
            const string configFileName = "Config_NonTimestampedLogFileNames.xml";

            string logFileName = siloName + ".log";
            FileInfo fileInfo = new FileInfo(logFileName);

            CreateIfNotExists(fileInfo);
            Assert.IsTrue(fileInfo.Exists, "Log file should exist: " + fileInfo.FullName);

            long initialSize = fileInfo.Length;

            var config = new OrleansConfiguration();
            config.LoadFromFile(configFileName);
            var n = config.GetConfigurationForNode(siloName);
            string fname = n.TraceFileName;
            Console.WriteLine("LogFileName = " + fname);

            Assert.AreEqual(logFileName, fname);

            Logger.Initialize(n);

            Assert.IsTrue(File.Exists(fileInfo.FullName), "Log file exists - before write: " + fileInfo.FullName);

            Logger myLogger = Logger.GetLogger("MyLogger", Logger.LoggerType.Application);

            myLogger.Info("Write something");

            fileInfo.Refresh(); // Need to refresh cached view of FileInfo

            Assert.IsTrue(fileInfo.Exists, "Log file exists - after write: " + fileInfo.FullName);

            long currentSize = fileInfo.Length;

            Assert.IsTrue(currentSize > initialSize, "Log file {0} should have been written to: Initial size = {1} Current size = {2}", logFileName, initialSize, currentSize);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void LogFile_Write_NotExists()
        {
            const string siloName = "MyNode4";
            const string configFileName = "Config_NonTimestampedLogFileNames.xml";

            string logFileName = siloName + ".log";
            FileInfo fileInfo = new FileInfo(logFileName);

            DeleteIfExists(fileInfo);

            Assert.IsFalse(File.Exists(fileInfo.FullName), "Log file should not exist: " + fileInfo.FullName);

            long initialSize = 0;

            var config = new OrleansConfiguration();
            config.LoadFromFile(configFileName);
            var n = config.GetConfigurationForNode(siloName);
            string fname = n.TraceFileName;
            Console.WriteLine("LogFileName = " + fname);

            Assert.AreEqual(logFileName, fname);

            Logger.Initialize(n);

            Assert.IsTrue(File.Exists(fileInfo.FullName), "Log file exists - before write: " + fileInfo.FullName);

            Logger myLogger = Logger.GetLogger("MyLogger", Logger.LoggerType.Application);

            myLogger.Info("Write something");

            fileInfo.Refresh(); // Need to refresh cached view of FileInfo

            Assert.IsTrue(fileInfo.Exists, "Log file exists - after write: " + fileInfo.FullName);

            long currentSize = fileInfo.Length;

            Assert.IsTrue(currentSize > initialSize, "Log file {0} should have been written to: Initial size = {1} Current size = {2}", logFileName, initialSize, currentSize);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void LogFile_Create()
        {
            const string siloName = "MyNode5";

            string logFileName = siloName + ".log";
            FileInfo fileInfo = new FileInfo(logFileName);

            DeleteIfExists(fileInfo);

            bool fileExists = fileInfo.Exists;
            Assert.IsFalse(fileExists, "Log file should not exist: " + fileInfo.FullName);

            CreateIfNotExists(fileInfo);

            fileExists = fileInfo.Exists;
            Assert.IsTrue(fileExists, "Log file should exist: " + fileInfo.FullName);

            long initialSize = fileInfo.Length;

            Assert.AreEqual(0, initialSize, "Log file {0} should be empty. Current size = {1}", logFileName, initialSize);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void ClientConfig_Default_ToString()
        {
            var cfg = new ClientConfiguration();
            var str = cfg.ToString();
            Assert.IsNotNull(str, "ClientConfiguration.ToString");
            Console.WriteLine(str);
            Assert.IsNull(cfg.SourceFile, "SourceFile");
            //Assert.IsNull(cfg.TraceFileName, "TraceFileName");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void ClientConfig_TraceFileName_Blank()
        {
            var cfg = new ClientConfiguration();
            cfg.TraceFileName = string.Empty;
            Console.WriteLine(cfg.ToString());

            cfg.TraceFileName = null;
            Console.WriteLine(cfg.ToString());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void ClientConfig_TraceFilePattern_Blank()
        {
            var cfg = new ClientConfiguration();
            cfg.TraceFilePattern = string.Empty;
            Console.WriteLine(cfg.ToString());
            Assert.IsNull(cfg.TraceFileName, "TraceFileName should be null");

            cfg.TraceFilePattern = null;
            Console.WriteLine(cfg.ToString());
            Assert.IsNull(cfg.TraceFileName, "TraceFileName should be null");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void ServerConfig_TraceFileName_Blank()
        {
            var cfg = new NodeConfiguration();
            cfg.TraceFileName = string.Empty;
            Console.WriteLine(cfg.ToString());

            cfg.TraceFileName = null;
            Console.WriteLine(cfg.ToString());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void ServerConfig_TraceFilePattern_Blank()
        {
            var cfg = new NodeConfiguration();
            cfg.TraceFilePattern = string.Empty;
            Console.WriteLine(cfg.ToString());
            Assert.IsNull(cfg.TraceFileName, "TraceFileName should be null");

            cfg.TraceFilePattern = null;
            Console.WriteLine(cfg.ToString());
            Assert.IsNull(cfg.TraceFileName, "TraceFileName should be null");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void ClientConfig_LoadFrom()
        {
            Logger.UnInitialize();

            string filename = "ClientConfiguration.xml";
            var cfg = ClientConfiguration.LoadFromFile(filename);
            var str = cfg.ToString();
            Assert.IsNotNull(str, "ClientConfiguration.ToString");
            Assert.AreEqual(filename, cfg.SourceFile);

            Logger.Initialize(cfg);
            Assert.AreEqual(3, Logger.LogConsumers.Count, "Number of log consumers: " + string.Join(",", Logger.LogConsumers));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void ClientConfig_LogConsumers()
        {
            Logger.UnInitialize();

            string filename = "Config_LogConsumers-ClientConfiguration.xml";
            var cfg = ClientConfiguration.LoadFromFile(filename);
            Assert.AreEqual(filename, cfg.SourceFile);

            Logger.Initialize(cfg);
            Assert.AreEqual(3, Logger.LogConsumers.Count, "Number of log consumers: " + string.Join(",",Logger.LogConsumers));
            Assert.AreEqual("UnitTests.DummyLogConsumer", Logger.LogConsumers.Last().GetType().FullName, "Log consumer type");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void ServerConfig_LogConsumers()
        {
            Logger.UnInitialize();

            string filename = "Config_LogConsumers-OrleansConfiguration.xml";
            var cfg = new OrleansConfiguration();
            cfg.LoadFromFile(filename);
            Assert.AreEqual(filename, cfg.SourceFile);

            Logger.Initialize(cfg.GetConfigurationForNode("Primary"));
            Assert.AreEqual(3, Logger.LogConsumers.Count, "Number of log consumers: " + string.Join(",", Logger.LogConsumers));
            Assert.AreEqual("UnitTests.DummyLogConsumer", Logger.LogConsumers.Last().GetType().FullName, "Log consumer type");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Limits_ClientConfig()
        {
            const string filename = "Config_LogConsumers-ClientConfiguration.xml";
            var config = ClientConfiguration.LoadFromFile(filename);

            string limitName;
            LimitValue limit;
            Assert.IsTrue(config.LimitValues.Count >= 3, "Number of LimitValues: " + string.Join(",", config.LimitValues));
            for (int i = 1; i <= 3; i++)
            {
                limitName = "Limit" + i;
                limit = config.GetLimit(limitName);
                Assert.IsNotNull(limit);
                Assert.AreEqual(limitName, limit.Name, "Limit name " + i);
                Assert.AreEqual(i, limit.SoftLimitThreshold, "Soft limit " + i);
                Assert.AreEqual(2 * i, limit.HardLimitThreshold, "Hard limit " + i);
            }

            limitName = "NoHardLimit";
            limit = config.GetLimit(limitName);
            Assert.IsNotNull(limit);
            Assert.AreEqual(limitName, limit.Name, "Limit name " + limitName);
            Assert.AreEqual(4, limit.SoftLimitThreshold, "Soft limit " + limitName);
            Assert.AreEqual(0, limit.HardLimitThreshold, "Hard limit " + limitName);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Limits_ServerConfig()
        {
            const string filename = "Config_LogConsumers-OrleansConfiguration.xml";
            var orleansConfig = new OrleansConfiguration();
            orleansConfig.LoadFromFile(filename);
            NodeConfiguration config = orleansConfig.GetConfigurationForNode("Primary");

            string limitName;
            LimitValue limit;
            Assert.IsTrue(config.LimitValues.Count >= 3, "Number of LimitValues: " + string.Join(",", config.LimitValues));
            for (int i = 1; i <= 3; i++)
            {
                limitName = "Limit" + i;
                limit = config.GetLimit(limitName);
                Assert.IsNotNull(limit);
                Assert.AreEqual(limitName, limit.Name, "Limit name " + i);
                Assert.AreEqual(i, limit.SoftLimitThreshold, "Soft limit " + i);
                Assert.AreEqual(2 * i, limit.HardLimitThreshold, "Hard limit " + i);
            }

            limitName = "NoHardLimit";
            limit = config.GetLimit(limitName);
            Assert.IsNotNull(limit);
            Assert.AreEqual(limitName, limit.Name, "Limit name " + limitName);
            Assert.AreEqual(4, limit.SoftLimitThreshold, "Soft limit " + limitName);
            Assert.AreEqual(0, limit.HardLimitThreshold, "Hard limit " + limitName);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Limits_ClientConfig_NotSpecified()
        {
            const string filename = "Config_LogConsumers-ClientConfiguration.xml";
            var config = ClientConfiguration.LoadFromFile(filename);

            string limitName = "NotPresent";
            LimitValue limit = config.GetLimit(limitName);
            Assert.IsNull(limit);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void Limits_ServerConfig_NotSpecified()
        {
            const string filename = "Config_LogConsumers-OrleansConfiguration.xml";
            var orleansConfig = new OrleansConfiguration();
            orleansConfig.LoadFromFile(filename);
            NodeConfiguration config = orleansConfig.GetConfigurationForNode("Primary");

            string limitName = "NotPresent";
            LimitValue limit = config.GetLimit(limitName);
            Assert.IsNull(limit);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void Limits_LimitsManager_NotInitialized()
        {
            string limitName = "NotInitialized";
            LimitValue limit = LimitManager.GetLimit(limitName);
            Assert.Fail("Exception should have been thrown");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Limits_LimitsManager_ServerConfig()
        {
            const string filename = "Config_LogConsumers-OrleansConfiguration.xml";
            var orleansConfig = new OrleansConfiguration();
            orleansConfig.LoadFromFile(filename);
            NodeConfiguration config = orleansConfig.GetConfigurationForNode("Primary");

            LimitManager.Initialize(config);

            string limitName;
            LimitValue limit;
            for (int i = 1; i <= 3; i++)
            {
                limitName = "Limit" + i;
                limit = LimitManager.GetLimit(limitName);
                Assert.IsNotNull(limit);
                Assert.AreEqual(limitName, limit.Name, "Limit name " + i);
                Assert.AreEqual(i, limit.SoftLimitThreshold, "Soft limit " + i);
                Assert.AreEqual(2 * i, limit.HardLimitThreshold, "Hard limit " + i);
            }

            limitName = "NoHardLimit";
            limit = LimitManager.GetLimit(limitName);
            Assert.IsNotNull(limit);
            Assert.AreEqual(limitName, limit.Name, "Limit name " + limitName);
            Assert.AreEqual(4, limit.SoftLimitThreshold, "Soft limit " + limitName);
            Assert.AreEqual(0, limit.HardLimitThreshold, "No Hard limit " + limitName);

            limitName = "NotPresent";
            limit = LimitManager.GetLimit(limitName);
            Assert.IsNotNull(limit);
            Assert.AreEqual(limitName, limit.Name, "Limit name " + limitName);
            Assert.AreEqual(0, limit.SoftLimitThreshold, "No Soft limit " + limitName);
            Assert.AreEqual(0, limit.HardLimitThreshold, "No Hard limit " + limitName);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Limits_LimitsManager_ClientConfig()
        {
            const string filename = "Config_LogConsumers-ClientConfiguration.xml";
            var config = ClientConfiguration.LoadFromFile(filename);

            LimitManager.Initialize(config);

            string limitName;
            LimitValue limit;
            for (int i = 1; i <= 3; i++)
            {
                limitName = "Limit" + i;
                limit = LimitManager.GetLimit(limitName);
                Assert.IsNotNull(limit);
                Assert.AreEqual(limitName, limit.Name, "Limit name " + i);
                Assert.AreEqual(i, limit.SoftLimitThreshold, "Soft limit " + i);
                Assert.AreEqual(2 * i, limit.HardLimitThreshold, "Hard limit " + i);
            }

            limitName = "NoHardLimit";
            limit = LimitManager.GetLimit(limitName);
            Assert.IsNotNull(limit);
            Assert.AreEqual(limitName, limit.Name, "Limit name " + limitName);
            Assert.AreEqual(4, limit.SoftLimitThreshold, "Soft limit " + limitName);
            Assert.AreEqual(0, limit.HardLimitThreshold, "No Hard limit " + limitName);

            limitName = "NotPresent";
            limit = LimitManager.GetLimit(limitName);
            Assert.IsNotNull(limit);
            Assert.AreEqual(limitName, limit.Name, "Limit name " + limitName);
            Assert.AreEqual(0, limit.SoftLimitThreshold, "No Soft limit " + limitName);
            Assert.AreEqual(0, limit.HardLimitThreshold, "No Hard limit " + limitName);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Limits_ClientConfig_NotSpecified_WithDefault()
        {
            const string filename = "Config_LogConsumers-ClientConfiguration.xml";
            var config = ClientConfiguration.LoadFromFile(filename);

            LimitManager.Initialize(config);

            string limitName;
            LimitValue limit;
            for (int i = 1; i <= 3; i++)
            {
                limitName = "NotPresent" + i;
                limit = LimitManager.GetLimit(limitName, i);
                Assert.IsNotNull(limit);
                Assert.AreEqual(limitName, limit.Name, "Limit name " + i);
                Assert.AreEqual(i, limit.SoftLimitThreshold, "Soft limit " + i);
                Assert.AreEqual(0, limit.HardLimitThreshold, "No Hard limit " + i);
            }
            for (int i = 1; i <= 3; i++)
            {
                limitName = "NotPresent" + i;
                limit = LimitManager.GetLimit(limitName, i, 2*i);
                Assert.IsNotNull(limit);
                Assert.AreEqual(limitName, limit.Name, "Limit name " + i);
                Assert.AreEqual(i, limit.SoftLimitThreshold, "Soft limit " + i);
                Assert.AreEqual(2 * i, limit.HardLimitThreshold, "Hard limit " + i);
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Limits_ServerConfig_NotSpecified_WithDefault()
        {
            const string filename = "Config_LogConsumers-OrleansConfiguration.xml";
            var orleansConfig = new OrleansConfiguration();
            orleansConfig.LoadFromFile(filename);
            NodeConfiguration config = orleansConfig.GetConfigurationForNode("Primary");

            LimitManager.Initialize(config);

            string limitName;
            LimitValue limit;
            for (int i = 1; i <= 3; i++)
            {
                limitName = "NotPresent" + i;
                limit = LimitManager.GetLimit(limitName, i);
                Assert.IsNotNull(limit);
                Assert.AreEqual(limitName, limit.Name, "Limit name " + i);
                Assert.AreEqual(i, limit.SoftLimitThreshold, "Soft limit " + i);
                Assert.AreEqual(0, limit.HardLimitThreshold, "No Hard limit " + i);
            }
            for (int i = 1; i <= 3; i++)
            {
                limitName = "NotPresent" + i;
                limit = LimitManager.GetLimit(limitName, i, 2 * i);
                Assert.IsNotNull(limit);
                Assert.AreEqual(limitName, limit.Name, "Limit name " + i);
                Assert.AreEqual(i, limit.SoftLimitThreshold, "Soft limit " + i);
                Assert.AreEqual(2 * i, limit.HardLimitThreshold, "Hard limit " + i);
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Config_GetDataConnectionInfo()
        {
            string dataConnectionStringInput =
                "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=qAAAA+YAikJPCE8V5yPlWZWBRGns4oti9tqG6/oYAYFGI4kFAnT91HeiWMa6pddUzDcG5OAmri/gk7owTOQZ+A==";
            Console.WriteLine("Input = " + dataConnectionStringInput);
            string dataConnectionStringOutput = ConfigUtilities.PrintDataConnectionInfo(dataConnectionStringInput);
            Console.WriteLine("Output = " + dataConnectionStringOutput);
            Assert.IsTrue(dataConnectionStringOutput.EndsWith("AccountKey=<--SNIP-->"), "Removed account key info from data connection string " + dataConnectionStringOutput);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void Config_StorageProvider_1()
        {
            const string filename = "Config_StorageProvider1.xml";
            const int numProviders = 1;
            var orleansConfig = new OrleansConfiguration();
            orleansConfig.LoadFromFile(filename);
            var providerConfigs = orleansConfig.Globals.ProviderConfigurations["Storage"];
            Assert.IsNotNull(providerConfigs, "Null provider configs");
            Assert.IsNotNull(providerConfigs.Providers, "Null providers");
            Assert.AreEqual(numProviders, providerConfigs.Providers.Count, "Num provider configs");

            ProviderConfiguration pCfg = (ProviderConfiguration) providerConfigs.Providers.Values.ToList()[0];
            Assert.AreEqual("orleanstest1", pCfg.Name, "Provider name #1");
            Assert.AreEqual("AzureTable", pCfg.Type, "Provider type #1");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Config_StorageProvider_2()
        {
            const string filename = "Config_StorageProvider2.xml";
            const int numProviders = 2;
            var orleansConfig = new OrleansConfiguration();
            orleansConfig.LoadFromFile(filename);
            var providerConfigs = orleansConfig.Globals.ProviderConfigurations["Storage"];
            Assert.IsNotNull(providerConfigs, "Null provider configs");
            Assert.IsNotNull(providerConfigs.Providers, "Null providers");
            Assert.AreEqual(numProviders, providerConfigs.Providers.Count, "Num provider configs");
            
            ProviderConfiguration pCfg = (ProviderConfiguration) providerConfigs.Providers.Values.ToList()[0];
            Assert.AreEqual("orleanstest1", pCfg.Name, "Provider name #1");
            Assert.AreEqual("AzureTable", pCfg.Type, "Provider type #1");

            pCfg = (ProviderConfiguration)providerConfigs.Providers.Values.ToList()[1];
            Assert.AreEqual("orleanstest2", pCfg.Name, "Provider name #2");
            Assert.AreEqual("AzureTable", pCfg.Type, "Provider type #2");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Config_StorageProvider_NoConfig()
        {
            const string filename = "Config_StorageProvider_NoConfig.xml";
            const int numProviders = 2;
            var orleansConfig = new OrleansConfiguration();
            orleansConfig.LoadFromFile(filename);
            var providerConfigs = orleansConfig.Globals.ProviderConfigurations["Storage"];
            Assert.IsNotNull(providerConfigs, "Null provider configs");
            Assert.IsNotNull(providerConfigs.Providers, "Null providers");
            Assert.AreEqual(numProviders, providerConfigs.Providers.Count, "Num provider configs");
            for (int i = 0; i < providerConfigs.Providers.Count; i++)
            {
                IProviderConfiguration provider = providerConfigs.Providers.Values.ToList()[i];
                Assert.AreEqual("test" + i, ((ProviderConfiguration)provider).Name, "Provider name #" + i);
                Assert.AreEqual(typeof(MockStorageProvider).FullName, ((ProviderConfiguration)provider).Type, "Provider type #" + i);
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Config")]
        public void Config_StorageProvider_SomeConfig()
        {
            const string filename = "Config_StorageProvider_SomeConfig.xml";
            const int numProviders = 2;
            var orleansConfig = new OrleansConfiguration();
            orleansConfig.LoadFromFile(filename);
            var providerConfigs = orleansConfig.Globals.ProviderConfigurations["Storage"];
            Assert.IsNotNull(providerConfigs, "Null provider configs");
            Assert.IsNotNull(providerConfigs.Providers, "Null providers");
            Assert.AreEqual(numProviders, providerConfigs.Providers.Count, "Num provider configs");
            for (int i = 0; i < providerConfigs.Providers.Count; i++)
            {
                IProviderConfiguration provider = providerConfigs.Providers.Values.ToList()[i];
                Assert.AreEqual("config" + i, ((ProviderConfiguration)provider).Name, "Provider name #" + i);
                Assert.AreEqual(typeof(MockStorageProvider).FullName, ((ProviderConfiguration)provider).Type, "Provider type #"+i);
                for (int j = 0; j < 2; j++)
                {
                    int num = 2 * i + j;
                    string key = "Prop" + num; 
                    string cfg = provider.Properties[key];
                    Assert.IsNotNull(cfg, "Null config value " + key);
                    Assert.IsFalse(string.IsNullOrWhiteSpace(cfg), "Blank config value " + key);
                    Assert.AreEqual(num.ToString(CultureInfo.InvariantCulture), cfg, "Config value {0} = {1}", key, cfg);
                }
            }
        }

        internal static void DeleteIfExists(FileInfo fileInfo)
        {
            if (fileInfo.Exists)
            {
                fileInfo.Delete();
                fileInfo.Refresh();
            }
            Assert.IsFalse(File.Exists(fileInfo.FullName), "File.Exists: {0}", fileInfo.FullName);
            Assert.IsFalse(fileInfo.Exists, "FileInfo.Exists: {0}", fileInfo.FullName);
        }

        internal static void CreateIfNotExists(FileInfo fileInfo)
        {
            if (!File.Exists(fileInfo.FullName))
            {
                using (var stream = fileInfo.CreateText())
                {
                    stream.Flush();
                }
                fileInfo.Refresh();
            }
            Assert.IsTrue(File.Exists(fileInfo.FullName), "File.Exists: {0}", fileInfo.FullName);
            Assert.IsTrue(fileInfo.Exists, "FileInfo.Exists: {0}", fileInfo.FullName);
        }
    }

    public class DummyLogConsumer : ILogConsumer
    {
        public void Log(OrleansLogger.Severity severity, Logger.LoggerType loggerType, string caller, string message, IPEndPoint myIPEndPoint, Exception exception, int eventCode = 0)
        {
            throw new NotImplementedException();
        }
    }
}

// ReSharper restore ConvertToConstant.Local
// ReSharper restore RedundantTypeArgumentsOfMethod
// ReSharper restore CheckNamespace
