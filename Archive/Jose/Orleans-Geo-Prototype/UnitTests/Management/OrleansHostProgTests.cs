using System;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Host;
using Orleans.Runtime;

// ReSharper disable ConvertToConstant.Local

namespace UnitTests.Management
{
    [TestClass]
    public class OrleansHostProgTests
    {
        readonly string hostname;

        public OrleansHostProgTests()
        {
            this.hostname = Dns.GetHostName();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseNoArgs()
        {
            var expectedSiloName = this.hostname;
            var expectedSiloType = Silo.SiloType.Secondary;
            OrleansHost prog = new OrleansHost();
            Assert.IsTrue(prog.ParseArguments(new string[] { }));
            Assert.AreEqual(expectedSiloType, prog.SiloHost.SiloType);
            Assert.AreEqual(expectedSiloName, prog.SiloHost.SiloName);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseUsageArg()
        {
            OrleansHost prog = new OrleansHost();
            Assert.IsFalse(prog.ParseArguments(new string[] { "/?" }));
            Assert.IsFalse(prog.ParseArguments(new string[] { "-?" }));
            Assert.IsFalse(prog.ParseArguments(new string[] { "/help" }));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseUsageArgWithOtherArgs()
        {
            OrleansHost prog = new OrleansHost();
            Assert.IsFalse(prog.ParseArguments(new string[] { "/?", "SiloName", "CfgFile.xml" }));
            Assert.IsFalse(prog.ParseArguments(new string[] { "SiloName", "CfgFile.xml", "/?" }));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseBadArguments()
        {
            OrleansHost prog = new OrleansHost();
            Assert.IsFalse(prog.ParseArguments(new string[] { "/xyz" }));
            Assert.IsFalse(prog.ParseArguments(new string[] { "/xyz", "/abc" }));
            Assert.IsFalse(prog.ParseArguments(new string[] { "/xyz", "/abc", "/123" }));
            Assert.IsFalse(prog.ParseArguments(new string[] { "/xyz", "/abc", "/123", "/456" }));
            Assert.IsFalse(prog.ParseArguments(new string[] { "DeploymentId=" }));
            Assert.IsFalse(prog.ParseArguments(new string[] { "DeploymentGroup=" }));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseSiloNameArg()
        {
            var expectedSiloName = "MySilo";
            var expectedSiloType = Silo.SiloType.Secondary;
            OrleansHost prog = new OrleansHost();
            Assert.IsTrue(prog.ParseArguments(new string[] { expectedSiloName }));
            Assert.AreEqual(expectedSiloType, prog.SiloHost.SiloType);
            Assert.AreEqual(expectedSiloName, prog.SiloHost.SiloName);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParsePrimarySiloNameArg()
        {
            var expectedSiloName = "Primary";
            var expectedSiloType = Silo.SiloType.Primary;
            OrleansHost prog = new OrleansHost();
            Assert.IsTrue(prog.ParseArguments(new string[] { expectedSiloName }));
            prog.Init();
            Assert.AreEqual(expectedSiloType, prog.SiloHost.SiloType);
            Assert.AreEqual(expectedSiloName, prog.SiloHost.SiloName);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseConfigFileArg()
        {
            var expectedSiloName = "MySilo";
            var expectedSiloType = Silo.SiloType.Secondary;
            var expectedConfigFileName = @"OrleansConfiguration.xml";
            OrleansHost prog = new OrleansHost();
            Assert.IsTrue(prog.ParseArguments(new string[] { expectedSiloName, expectedConfigFileName }));
            Assert.AreEqual(expectedSiloType, prog.SiloHost.SiloType);
            Assert.AreEqual(expectedSiloName, prog.SiloHost.SiloName);
            Assert.AreEqual(expectedConfigFileName, prog.SiloHost.ConfigFileName);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseDeploymentIdArg()
        {
            var expectedSiloName = this.hostname;
            var expectedDeploymentId = Guid.NewGuid().ToString("D");
            OrleansHost prog = new OrleansHost();
            Assert.IsTrue(prog.ParseArguments(new string[] { "DeploymentId=" + expectedDeploymentId }));
            Assert.AreEqual(expectedSiloName, prog.SiloHost.SiloName);
            Assert.AreEqual(expectedDeploymentId, prog.SiloHost.DeploymentId);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseDeploymentGroupArg()
        {
            var expectedSiloName = this.hostname;
            var expectedDeploymentId = Guid.NewGuid().ToString("D");
            OrleansHost prog = new OrleansHost();
            Assert.IsTrue(prog.ParseArguments(new string[] { "DeploymentGroup=" + expectedDeploymentId }));
            Assert.AreEqual(expectedSiloName, prog.SiloHost.SiloName);
            Assert.IsNull(prog.SiloHost.DeploymentId);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseDeploymentGroupArgFormats()
        {
            var expectedDeploymentId = Guid.NewGuid().ToString("N");
            OrleansHost prog = new OrleansHost();
            Assert.IsTrue(prog.ParseArguments(new string[] { "DeploymentId=" + expectedDeploymentId }));
            Assert.AreEqual(expectedDeploymentId, prog.SiloHost.DeploymentId);

            prog = new OrleansHost();
            expectedDeploymentId = Guid.NewGuid().ToString("D");
            Assert.IsTrue(prog.ParseArguments(new string[] { "DeploymentId=" + expectedDeploymentId }));
            Assert.AreEqual(expectedDeploymentId, prog.SiloHost.DeploymentId);

            prog = new OrleansHost();
            expectedDeploymentId = Guid.NewGuid().ToString("B");
            Assert.IsTrue(prog.ParseArguments(new string[] { "DeploymentId=" + expectedDeploymentId }));
            Assert.AreEqual(expectedDeploymentId, prog.SiloHost.DeploymentId);

            prog = new OrleansHost();
            expectedDeploymentId = Guid.NewGuid().ToString("P");
            Assert.IsTrue(prog.ParseArguments(new string[] { "DeploymentId=" + expectedDeploymentId }));
            Assert.AreEqual(expectedDeploymentId, prog.SiloHost.DeploymentId);

            prog = new OrleansHost();
            expectedDeploymentId = Guid.NewGuid().ToString("X");
            Assert.IsTrue(prog.ParseArguments(new string[] { "DeploymentId=" + expectedDeploymentId }));
            Assert.AreEqual(expectedDeploymentId, prog.SiloHost.DeploymentId);

            prog = new OrleansHost();
            expectedDeploymentId = Guid.NewGuid().ToString("");
            Assert.IsTrue(prog.ParseArguments(new string[] { "DeploymentId=" + expectedDeploymentId }));
            Assert.AreEqual(expectedDeploymentId, prog.SiloHost.DeploymentId);

            prog = new OrleansHost();
            expectedDeploymentId = Guid.NewGuid().ToString();
            Assert.IsTrue(prog.ParseArguments(new string[] { "DeploymentId=" + expectedDeploymentId }));
            Assert.AreEqual(expectedDeploymentId, prog.SiloHost.DeploymentId);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseDeploymentGroupLastArgWins()
        {
            var expectedDeploymentId1 = Guid.NewGuid();
            var expectedDeploymentId2 = Guid.NewGuid();
            OrleansHost prog = new OrleansHost();
            Assert.IsTrue(prog.ParseArguments(new string[] { 
                "DeploymentId=" + expectedDeploymentId1,
                "DeploymentId=" + expectedDeploymentId2,
                "DeploymentGroup=" + expectedDeploymentId1,
                "DeploymentGroup=" + expectedDeploymentId2 
            }));
            Assert.AreEqual(expectedDeploymentId2.ToString(), prog.SiloHost.DeploymentId);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void OrleansHostParseMultipleArgs()
        {
            var expectedSiloName = "MySilo";
            var expectedConfigFileName = @"OrleansConfiguration.xml";
            var expectedDeploymentId = Guid.NewGuid();
            OrleansHost prog = new OrleansHost();
            Assert.IsTrue(prog.ParseArguments(new string[] { 
                expectedSiloName, 
                expectedConfigFileName, 
                "DeploymentId=" + expectedDeploymentId
            }));
            Assert.AreEqual(expectedSiloName, prog.SiloHost.SiloName);
            Assert.AreEqual(expectedConfigFileName, prog.SiloHost.ConfigFileName);
            Assert.AreEqual(expectedDeploymentId.ToString(), prog.SiloHost.DeploymentId);
        }
    }
}

// ReSharper restore ConvertToConstant.Local
