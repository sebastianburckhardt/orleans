using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using GrainClientGenerator;
using GrainInterfaceData = GrainClientGenerator.GrainInterfaceData;

namespace UnitTests
{
    #region simple interfaces

    public interface ITestGrain_VoidMethod : IAddressable
    {
        void VoidMethod();
    }

    public interface ITestGrain_IntMethod : IAddressable
    {
        int IntMethod();
    }

    public interface ITestGrain_IntProperty : IAddressable
    {
        int IntProperty { get; }
    }

    public interface ITestGrain_PropertySetter : IAddressable
    {
        Task<int> IntProperty { get; set; }
    }

    public interface ITestObserver_NonVoidMethod : IGrainObserver
    {
        Task NonVoidMethod();
    }

    public interface ITestObserver_Property : IGrainObserver
    {
        Task<int> IntProperty { get; }
    }

    public interface ITestGrain_OutArgument : IAddressable
    {
        Task Method(out int parameter);
    }

    public interface ITestGrain_RefArgument : IAddressable
    {
        Task Method(ref int parameter);
    }

    #endregion

    #region inheritance

    public interface IBaseGrain : IAddressable
    {
    }

    public interface IBaseObserver : IGrainObserver
    {
    }

    public interface IInheritedGrain_ObserverGrain_VoidMethod : IBaseGrain, IBaseObserver
    {
        void VoidMethod();
    }

    public interface IInheritedGrain_ObserverGrain_IntMethod : IBaseGrain, IBaseObserver
    {
        int IntMethod();
    }

    public interface IInheritedGrain_ObserverGrain_IntProperty : IBaseGrain, IBaseObserver
    {
        int IntProperty { get; }
    }
    
    public interface IInheritedGrain_ObserverGrain_PropertySetter : IBaseGrain, IBaseObserver
    {
        Task<int> IntProperty { get; set; }
    }

    public interface IBaseTaskGrain : IAddressable
    {
        Task VoidMethod();
    }

    public interface IBasePromiseGrain : IAddressable
    {
        Task VoidMethod();
    }

    public interface IDerivedTaskGrain : IBaseTaskGrain
    {
    }

    public interface IDerivedPromiseGrain : IBasePromiseGrain
    {
    }

    public interface IDerivedTaskGrainWithGrainRef : IBaseTaskGrain
    {
        Task<IBasePromiseGrain> GetGrain();
    }

    public interface IDerivedPromiseGrainWithGrainRef : IBasePromiseGrain
    {
        Task<IBaseTaskGrain> GetGrain();
    }

    #endregion

    /// <summary>
    /// Summary description for InterfaceRules
    /// </summary>
    [TestClass]
    public class InterfaceRulesTests
    {
        public InterfaceRulesTests()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        [TestInitialize]
        [TestCleanup]
        public void MyTestCleanup()
        {
            OrleansTask.Reset();
        }

        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        #region simple interfaces

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_VoidMethod()
        {
            new GrainInterfaceData(typeof(ITestGrain_VoidMethod));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_IntMethod()
        {
            new GrainInterfaceData(typeof(ITestGrain_IntMethod));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_IntProperty()
        {
            new GrainInterfaceData(typeof(ITestGrain_IntProperty));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_PropertySetter()
        {
            new GrainInterfaceData(typeof(ITestGrain_PropertySetter));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_Observer_NonVoidMethod()
        {
            new GrainInterfaceData(typeof(ITestObserver_NonVoidMethod));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_Observer_Property()
        {
            new GrainInterfaceData(typeof(ITestObserver_Property));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_OutArgument()
        {
            new GrainInterfaceData(typeof(ITestGrain_OutArgument));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_RefArgument()
        {
            new GrainInterfaceData(typeof(ITestGrain_RefArgument));
        }

        #endregion

        #region inheritence

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_ObserverGrain_VoidMethod()
        {
            new GrainInterfaceData(typeof(IInheritedGrain_ObserverGrain_VoidMethod));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_ObserverGrain_IntMethod()
        {
            new GrainInterfaceData(typeof(IInheritedGrain_ObserverGrain_IntMethod));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_ObserverGrain_IntProperty()
        {
            new GrainInterfaceData(typeof(IInheritedGrain_ObserverGrain_IntProperty));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [ExpectedException(typeof(ArgumentException))]
        public void InterfaceRules_ObserverGrain_PropertySetter()
        {
            new GrainInterfaceData(typeof(IInheritedGrain_ObserverGrain_PropertySetter));
        }

        #endregion

        #region inferring task-based types

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void InterfaceRules_TaskBased()
        {
            Assert.IsTrue((new GrainInterfaceData(typeof(IBaseTaskGrain))).IsTaskGrain, 
                "Basic task-based grain interface not detected as task-based");
            Assert.IsTrue((new GrainInterfaceData(typeof(IDerivedTaskGrain))).IsTaskGrain, 
                "Derived task-based grain interface not detected as task-based");
            Assert.IsTrue((new GrainInterfaceData(typeof(IDerivedTaskGrainWithGrainRef))).IsTaskGrain, 
                "Derived task-based grain interface with grain reference method not detected as task-based");
        }

        #endregion
    }
}
