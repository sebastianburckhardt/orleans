using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GrainClientGenerator;
using Orleans;

using SimpleGrain;
using UnitTestGrainInterfaces;
using UnitTestGrainInterfaces.Generic;
using UnitTestGrains;
using GrainInterfaceData = GrainClientGenerator.GrainInterfaceData;

// ReSharper disable ConvertToConstant.Local

namespace UnitTests.CodeGeneration
{
    /// <summary>
    /// Summary description for CodeGeneratorTests
    /// </summary>
    [TestClass]
    public class CodeGeneratorTests
    {
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

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ServiceInterface_IsGrainClass()
        {
            Type t = typeof(GrainBase);
            Assert.IsFalse(InvokerGenerator.IsGrainClass(t), t.FullName + " is not grain class");
            t = typeof(Orleans.Runtime.GrainDirectory.RemoteGrainDirectory);
            Assert.IsTrue(InvokerGenerator.IsGrainClass(t), t.FullName + " should be a grain class");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ServiceInterface_ServiceTypeName()
        {
            GrainInterfaceData si = new GrainInterfaceData(typeof(ISimpleGenericGrain<>));
            Assert.AreEqual("ISimpleGenericGrain<T>", si.ServiceTypeName, "ServiceTypeName [Client] = ISimpleGenericGrain<T>");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ServiceInterface_Generic_ClassName()
        {
            GrainInterfaceData si = new GrainInterfaceData(typeof(ISimpleGenericGrain<>));
            Assert.AreEqual("ISimpleGenericGrain<T>", si.Name);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ServiceInterface_Generic_FactoryClassName()
        {
            GrainInterfaceData si = new GrainInterfaceData(typeof(ISimpleGenericGrain<>));
            Assert.AreEqual("SimpleGenericGrainFactory<T>", si.FactoryClassName);
            Assert.AreEqual("SimpleGenericGrainFactory", si.FactoryClassBaseName);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ServiceInterface_Generic_ReferenceClassName()
        {
            GrainInterfaceData si = new GrainInterfaceData(typeof(ISimpleGenericGrain<>));
            Assert.AreEqual("SimpleGenericGrainReference<T>", si.ReferenceClassName);
            Assert.AreEqual("SimpleGenericGrainReference", si.ReferenceClassBaseName);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ServiceInterface_Generic_InvokerClassName()
        {
            GrainInterfaceData si = new GrainInterfaceData(typeof(ISimpleGenericGrain<>));
            Assert.AreEqual("SimpleGenericGrainMethodInvoker<T>", si.InvokerClassName);
            //Assert.AreEqual("SimpleGenericGrainMethodInvoker", interfaceData.InvokerClassBaseName);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ServiceInterface_Generic_RemoteInterfaceName()
        {
            GrainInterfaceData si = new GrainInterfaceData(typeof(ISimpleGenericGrain<>));
            Assert.AreEqual("ISimpleGenericGrain<T>", si.InterfaceTypeName);
            Assert.AreEqual("ISimpleGenericGrain`1", si.Type.Name);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void ServiceInterface_Generic_RemoteInterfaceTypeName()
        {
            GrainInterfaceData si = new GrainInterfaceData(typeof(ISimpleGenericGrain<>));
            Assert.AreEqual("ISimpleGenericGrain<T>", si.InterfaceTypeName);
            //Assert.AreEqual("ISimpleGenericGrain", interfaceData.RemoteInterfaceTypeBaseName);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ServiceInterface_Generic_GrainStateClassName()
        {
            GrainInterfaceData si = new GrainInterfaceData(typeof(ISimpleGenericGrain<>));
            Assert.AreEqual("SimpleGenericGrainState<T>", si.StateClassName);
            Assert.AreEqual("SimpleGenericGrainState", si.StateClassBaseName);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("CodeGen")]
        public void TypeUtils_RawClassName_Generic_1()
        {
            Type t = typeof(ISimpleGenericGrain<>);
            Assert.AreEqual("ISimpleGenericGrain`1", TypeUtils.GetRawClassName(TypeUtils.GetSimpleTypeName(t), t));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("CodeGen")]
        public void TypeUtils_RawClassName_Generic_2()
        {
            Type t = typeof(ISimpleGenericGrain2<,>);
            Assert.AreEqual("ISimpleGenericGrain2`2", TypeUtils.GetRawClassName(TypeUtils.GetSimpleTypeName(t), t));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("CodeGen")]
        public void TypeUtils_RawClassName_Generic_String_1()
        {
            string typeString = "GenericTestGrains.SimpleGenericGrain`1[[System.Object, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089]]";
            Assert.AreEqual("GenericTestGrains.SimpleGenericGrain`1", TypeUtils.GetRawClassName(typeString));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ServiceInterface_CacheableDuration()
        {
            var ifaceType = typeof (ICachedPropertiesGrain);
            var p = ifaceType.GetProperties();
            Assert.IsNotNull(p, "Properties found");
            Assert.AreEqual(2, p.Length, "Number of properties found");
            var pi = p[0];
            Assert.IsNotNull(pi, "Property found");
            Assert.AreEqual("A", pi.Name, "Property Name='A'");
            var mi = pi.GetGetMethod();
            var cacheTime = GrainInterfaceData.CacheableDuration(mi);
            Assert.AreNotEqual(TimeSpan.Zero, cacheTime, "Cacheable.Duration value not zero");
            Assert.IsTrue(cacheTime > TimeSpan.Zero, "Cacheable.Duration value=" + cacheTime);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ServiceInterface_CacheableDuration_NotDefined()
        {
            var ifaceType = typeof(ICachedPropertiesGrain);
            var p = ifaceType.GetProperties();
            Assert.IsNotNull(p, "Properties found");
            Assert.AreEqual(2, p.Length, "Number of properties found");
            var pi = p[1];
            Assert.IsNotNull(pi, "Property found");
            Assert.AreEqual("B", pi.Name, "Property Name='B'");
            var mi = pi.GetGetMethod();
            var cacheTime = GrainInterfaceData.CacheableDuration(mi);
            Assert.AreEqual(TimeSpan.Zero, cacheTime, "Cacheable.Duration value zero");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ConvertToTaskAsyncMethod()
        {
            string methName = "SetA";
            var meth = typeof(ISimpleGrain).GetMethod(methName);

            var ns = new GrainNamespace(null, "test");
            var codeGen = ns.ConvertToTaskAsyncMethod(meth, false);
            Assert.AreEqual(methName + "Async", codeGen.Name, "Method.Name");
            var expTypeName = TypeUtils.GetRawClassName(typeof(Task));
            Assert.AreEqual(expTypeName, codeGen.ReturnType.BaseType, "Method.ReturnType.BaseType");
            Assert.AreEqual(0, codeGen.ReturnType.TypeArguments.Count, "Method.ReturnType.TypeArguments.Count");
            Assert.AreEqual(1, codeGen.Parameters.Count, "Method.Parameters.Count");
            Assert.AreEqual(TypeUtils.GetRawClassName(typeof(int)), codeGen.Parameters[0].Type.BaseType, "Method.Parameters[0].Type");
            Assert.AreEqual("a", codeGen.Parameters[0].Name, "Method.Parameters[0].Name");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void IsGrainMethod()
        {
            Type t = typeof (ISimpleGrain);
            var meth = t.GetMethod("SetA");
            Assert.IsTrue(InvokerGeneratorBasic.IsGrainMethod(meth), "Method " + meth.DeclaringType + "." + meth.Name + " should be a grain method");
            meth = t.GetMethod("get_A");
            Assert.IsFalse(InvokerGeneratorBasic.IsGrainMethod(meth), "Method " + meth.DeclaringType + "." + meth.Name + " should NOT be a grain method");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void IsTaskGrainMethod()
        {
            Type t = typeof(Echo.IEchoTaskGrain);
            var meth = t.GetMethod("EchoAsync");
            Assert.IsTrue(InvokerGeneratorBasic.IsGrainMethod(meth), "Method " + meth.DeclaringType + "." + meth.Name + " should be a grain method");
            meth = t.GetMethod("EchoErrorAsync");
            Assert.IsTrue(InvokerGeneratorBasic.IsGrainMethod(meth), "Method " + meth.DeclaringType + "." + meth.Name + " should be a grain method");
            meth = t.GetMethod("get_LastEchoAsync");
            Assert.IsFalse(InvokerGeneratorBasic.IsGrainMethod(meth), "Method " + meth.DeclaringType + "." + meth.Name + " should NOT be a grain method");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void RequiresPropertiesClass()
        {
            GrainInterfaceData si = new GrainInterfaceData(typeof(ISimpleGrain));
            Assert.IsTrue(InvokerGeneratorBasic.RequiresPropertiesClass(si, true), si.Name + " should require properties class to be generated on Client");
            Assert.IsFalse(InvokerGeneratorBasic.RequiresPropertiesClass(si, false), si.Name + " should not require properties class to be generated on Server");

            si = new GrainInterfaceData(typeof(ISimpleGenericGrain<>));
            Assert.IsTrue(InvokerGeneratorBasic.RequiresPropertiesClass(si, true), si.Name + " should require properties class to be generated on Client");
            Assert.IsFalse(InvokerGeneratorBasic.RequiresPropertiesClass(si, false), si.Name + " should not require properties class to be generated on Server");

            si = new GrainInterfaceData(typeof(ISimpleGrainWithAsyncMethods));
            Assert.IsFalse(InvokerGeneratorBasic.RequiresPropertiesClass(si, true), si.Name + " should not require properties class to be generated on Client");
            Assert.IsFalse(InvokerGeneratorBasic.RequiresPropertiesClass(si, false), si.Name + " should not require properties class to be generated on Server");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("CodeGen")]
        public void ISelfManagedGrain_RequiresPropertiesClass()
        {
            Type t = typeof(IGrain);
            GrainInterfaceData si = GrainInterfaceData.FromGrainClass(t);
            Assert.IsFalse(GrainInterfaceData.IsGrainInterface(t), t.FullName + " should not be a service interface");
            Assert.IsFalse(InvokerGeneratorBasic.RequiresPropertiesClass(si, true), t.FullName + " should not require properties class to be generated on Client");
            Assert.IsFalse(InvokerGeneratorBasic.RequiresPropertiesClass(si, false), t.FullName + " should not require properties class to be generated on Server");

            t = typeof(IGrain);
            si = GrainInterfaceData.FromGrainClass(t);
            Assert.IsFalse(GrainInterfaceData.IsGrainInterface(t), t.FullName + " should not be a service interface");
            Assert.IsFalse(InvokerGeneratorBasic.RequiresPropertiesClass(si, true), t.FullName + " should not require properties class to be generated on Client");
            Assert.IsFalse(InvokerGeneratorBasic.RequiresPropertiesClass(si, false), t.FullName + " should not require properties class to be generated on Server");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void CodeGen_AC_TaskGrain_GetInterfaceInfo()
        {
            Type t = typeof(Echo.IEchoTaskGrain);
            InvokerGeneratorBasic.GrainInterfaceInfo grainInterfaceInfo = InvokerGeneratorBasic.GetInterfaceInfo(t);
            Assert.AreEqual(1, grainInterfaceInfo.Interfaces.Count, "Expected one interface - EchoTaskGrain");
            Type interfaceType = grainInterfaceInfo.Interfaces.Values.First().InterfaceType;
            int interfaceId = grainInterfaceInfo.Interfaces.Keys.First();
            Assert.AreEqual(-1626135387, interfaceId, "InterfaceId - EchoTaskGrain");
            Assert.AreEqual(typeof(Echo.IEchoTaskGrain), interfaceType, "InterfaceType - EchoTaskGrain");

            t = typeof(Echo.IEchoGrain);
            grainInterfaceInfo = InvokerGeneratorBasic.GetInterfaceInfo(t);
            Assert.AreEqual(1, grainInterfaceInfo.Interfaces.Count, "Expected one interface");
            interfaceType = grainInterfaceInfo.Interfaces.Values.First().InterfaceType;
            interfaceId = grainInterfaceInfo.Interfaces.Keys.First();
            Assert.AreEqual(-2033891083, interfaceId, "InterfaceId");
            Assert.AreEqual(typeof(Echo.IEchoGrain), interfaceType, "InterfaceType");
            Assert.IsTrue(typeof(IAddressable).IsAssignableFrom(interfaceType),
                "Expected AsyncCompletion version of interface GrainInterfaceInfo {0}, rather than type {1}", interfaceId, interfaceType);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void CodeGen_TaskGrain_GetInterfaceInfo()
        {
            Type t = typeof(Echo.IEchoTaskGrain);
            InvokerGeneratorBasic.GrainInterfaceInfo grainInterfaceInfo = InvokerGeneratorBasic.GetInterfaceInfo(t);
            Assert.AreEqual(1, grainInterfaceInfo.Interfaces.Count, "Expected one interface - Async");
            Type interfaceType = grainInterfaceInfo.Interfaces.Values.First().InterfaceType;
            int interfaceId = grainInterfaceInfo.Interfaces.Keys.First();
            Assert.AreEqual(-1626135387, interfaceId, "InterfaceId-Async");
            Assert.AreEqual(typeof(Echo.IEchoTaskGrain), interfaceType, "InterfaceType-Async");
            Assert.IsTrue(GrainInterfaceData.IsTaskBasedInterface(interfaceType),
                "Expected Task-based version of interface GrainInterfaceInfo {0}, rather than type {1}", interfaceId, interfaceType);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void CodeGen_AC_TaskGrain_GetMethods()
        {
            Type t = typeof(Echo.IEchoTaskGrain);

            MethodInfo[] methods = GrainInterfaceData.GetMethods(t);
            Assert.IsTrue(methods.Length >= 3, "Expected some Async methods - Got " + methods.Length);

            foreach (MethodInfo mi in methods)
            {
                Assert.IsTrue(mi.Name.EndsWith("Async"), "Method names end with Async - " + mi);

                Assert.IsTrue(InvokerGeneratorBasic.IsGrainMethod(mi) || GrainNamespace.IsGetPropertyMethod(mi) || GrainNamespace.IsSetPropertyMethod(mi),
                    mi + " should be a valid method: IsGrainMethod={0} IsGetPropertyMethod={1} IsSetPropertyMethod={2}", 
                        InvokerGeneratorBasic.IsGrainMethod(mi), GrainNamespace.IsGetPropertyMethod(mi), GrainNamespace.IsSetPropertyMethod(mi)
                );
            }

            InvokerGeneratorBasic.InterfaceInfo interfaceInfo = new InvokerGeneratorBasic.InterfaceInfo(t);
            methods = interfaceInfo.Methods.Values.ToArray();

            Assert.IsTrue(methods.Length >= 3, "Expected some Async methods - Got " + methods.Length);

            foreach (MethodInfo mi in methods)
            {
                Assert.IsTrue(mi.Name.EndsWith("Async"), "Method names end with Async - " + mi);

                Assert.IsTrue(InvokerGeneratorBasic.IsGrainMethod(mi) || GrainNamespace.IsGetPropertyMethod(mi) || GrainNamespace.IsSetPropertyMethod(mi),
                    mi + " should be a valid method: IsGrainMethod={0} IsGetPropertyMethod={1} IsSetPropertyMethod={2}",
                        InvokerGeneratorBasic.IsGrainMethod(mi), GrainNamespace.IsGetPropertyMethod(mi), GrainNamespace.IsSetPropertyMethod(mi)
                );
            }
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void CodeGen_TaskGrain_GetMethods()
        {
            Type t = typeof(Echo.IEchoTaskGrain);

            MethodInfo[] methods = GrainInterfaceData.GetMethods(t);
            Assert.IsTrue(methods.Length >= 3, "Expected some Async methods - Got " + methods.Length);

            foreach (MethodInfo mi in methods)
            {
                Assert.IsTrue(mi.Name.EndsWith("Async"), "Method names end with Async - " + mi);

                Assert.IsTrue(InvokerGeneratorBasic.IsGrainMethod(mi) || GrainNamespace.IsGetPropertyMethod(mi) || GrainNamespace.IsSetPropertyMethod(mi),
                    mi + " should be a valid method: IsGrainMethod={0} IsGetPropertyMethod={1} IsSetPropertyMethod={2}",
                        InvokerGeneratorBasic.IsGrainMethod(mi), GrainNamespace.IsGetPropertyMethod(mi), GrainNamespace.IsSetPropertyMethod(mi)
                );
            }

            InvokerGeneratorBasic.InterfaceInfo interfaceInfo = new InvokerGeneratorBasic.InterfaceInfo(t);
            methods = interfaceInfo.Methods.Values.ToArray();

            Assert.IsTrue(methods.Length >= 3, "Expected some Async methods - Got " + methods.Length);

            foreach (MethodInfo mi in methods)
            {
                Assert.IsTrue(mi.Name.EndsWith("Async"), "Method names end with Async - " + mi);

                Assert.IsTrue(InvokerGeneratorBasic.IsGrainMethod(mi) || GrainNamespace.IsGetPropertyMethod(mi) || GrainNamespace.IsSetPropertyMethod(mi),
                    mi + " should be a valid method: IsGrainMethod={0} IsGetPropertyMethod={1} IsSetPropertyMethod={2}",
                        InvokerGeneratorBasic.IsGrainMethod(mi), GrainNamespace.IsGetPropertyMethod(mi), GrainNamespace.IsSetPropertyMethod(mi)
                );
            }
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen")]
        public void CodeGen_ObjectTo_List()
        {
            List<String> list = new List<string> { "1", "2" };

            ArrayList arrayList = new ArrayList(list);
            List<String> list2 = Utils.ObjectToList<string>(arrayList);
            CheckOutputList(list, list2);

            string[] array = list.ToArray();
            List<String> list3 = Utils.ObjectToList<string>(array);
            CheckOutputList(list, list3);

            List<string> listCopy = list.ToList();
            List<String> list4 = Utils.ObjectToList<string>(listCopy);
            CheckOutputList(list, list4);

            IReadOnlyList<string> readOnlyList = list.ToList();
            List<String> list5 = Utils.ObjectToList<string>(readOnlyList);
            CheckOutputList(list, list5);
        }

        private static void CheckOutputList(List<string> expected, List<string> actual)
        {
            Assert.AreEqual(expected.Count, actual.Count, "Output list size");
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.IsNotNull(actual[i], "Output list element #{0}", i);
                Assert.AreEqual(expected[i], actual[i], "Output list element #{0}", i);
            }
        }
    }

    [TestClass]
    public class CodeGeneratorTestsRequiringSilo : UnitTestBase
    {
        [ClassCleanup]
        public static void ClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        // These test cases create GrainReferences, to we need to be connected to silo for that to work.

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen"), TestCategory("General"), TestCategory("UniqueKey"), TestCategory("CodeGen")]
        public void CodeGen_GrainId_TypeCode()
        {
            ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(1);
            GrainId id1 = ((GrainReference)g1).GrainId;
            UniqueKey k1 = id1.Key;
            Assert.IsTrue(id1.IsGrain, "GrainReference should be for self-managed type");
            Assert.AreEqual(UniqueKey.Category.Grain, k1.IdCategory, "GrainId should be for self-managed type");
            Assert.AreEqual(1, k1.PrimaryKeyToLong(), "Encoded primary key should match");
            Assert.AreEqual(-1929503321, k1.BaseTypeCode, "Encoded type code data should match");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("CodeGen"), TestCategory("General"), TestCategory("UniqueKey"), TestCategory("GC"), TestCategory("CodeGen")]
        public void CollectionTest_GrainId_TypeCode()
        {
            ICollectionTestGrain g1 = CollectionTestGrainFactory.GetGrain(1);
            GrainId id1 = ((GrainReference)g1).GrainId;
            UniqueKey k1 = id1.Key;
            Console.WriteLine("GrainId={0} UniqueKey={1} PK={2} KeyType={3} IdCategory={4}",
                id1, k1, id1.GetPrimaryKeyLong(), k1.IdCategory, k1.BaseTypeCode);
            Assert.IsTrue(id1.IsGrain, "GrainReference should be for self-managed type");
            Assert.AreEqual(UniqueKey.Category.Grain, k1.IdCategory, "GrainId should be for self-managed type");
            Assert.AreEqual(1, k1.PrimaryKeyToLong(), "Encoded primary key should match");
            Assert.AreEqual(-1096253375, k1.BaseTypeCode, "Encoded type code data should match");
        }
    }

}

// ReSharper restore ConvertToConstant.Local
