using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrainInterfaces;
using SimpleGrain;
namespace UnitTests.General
{
    public class Helper
    {
        static public void AreEqual<T>(ComplicatedTestType<T> obj1, ComplicatedTestType<T> obj2)
        {
            /*
             * public T Basic { get; set; }
        public T[] BasicArray { get; set; }
        public T[][] BasicMultiArray { get; set; }
        public List<T> BasicList { get; set; }
        public List<List<T>> BasicListOfList { get; set; }
        public List<T[]> BasicListOfArray { get; set; }
        public List<T>[] BasicArrayOfList { get; set; }
             */
            Assert.AreEqual(obj1.Basic, obj2.Basic);
            CollectionAssert.AreEqual(obj1.BasicArray, obj2.BasicArray);
            Assert.AreEqual(obj1.BasicMultiArray.Rank, obj2.BasicMultiArray.Rank);

            Assert.AreEqual(obj1.BasicList.Count, obj2.BasicList.Count);
            CollectionAssert.AreEqual(obj1.BasicList.ToArray(), obj2.BasicList.ToArray());

            Assert.AreEqual(obj1.BasicListOfList.Count, obj2.BasicListOfList.Count);
            for (int i = 0; i < obj1.BasicListOfList.Count; i++)
            {
                List<T> lst1 = obj1.BasicListOfList[i];
                List<T> lst2 = obj2.BasicListOfList[i];

                Assert.AreEqual(lst1.Count, lst2.Count);
                CollectionAssert.AreEqual(lst1.ToArray(), lst2.ToArray());
            }


            Assert.AreEqual(obj1.BasicArrayOfList.Length, obj2.BasicArrayOfList.Length);
            for (int i = 0; i < obj1.BasicArrayOfList.Length; i++)
            {
                List<T> lst1 = obj1.BasicArrayOfList[i];
                List<T> lst2 = obj2.BasicArrayOfList[i];

                Assert.AreEqual(lst1.Count, lst2.Count);
                CollectionAssert.AreEqual(lst1.ToArray(), lst2.ToArray());
            }


            Assert.AreEqual(obj1.BasicListOfArray.Count, obj2.BasicListOfArray.Count);
            for (int i = 0; i < obj1.BasicListOfArray.Count; i++)
            {
                T[] arr1 = obj1.BasicListOfArray[i];
                T[] arr2 = obj2.BasicListOfArray[i];

                CollectionAssert.AreEqual(arr1, arr2);
            }

        }
    }

    [TestClass]
    public class GetPropertiesTests : UnitTestBase
    {
        [TestMethod]
        public void TestSimpleGrainProperties()
        {
            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId()); ;

            for (int i = 0; i < 3; i++)
            {
                Task setPromise = grain.SetA(i);
                setPromise.Wait();
                Task<SimpleGrainProperties> propPromise = grain.GetProperties();
                SimpleGrainProperties props = propPromise.Result;
                Assert.AreEqual(props.A, i);
            }
        }

        [TestMethod]
        public void TestComplexGrainProperties()
        {
            IComplexGrain grain = ComplexGrainFactory.GetGrain(GetRandomGrainId());
            ComplicatedTestType<int> i = new ComplicatedTestType<int>();
            i.InitWithSeed(42);
            ComplicatedTestType<string> s = new ComplicatedTestType<string>();
            s.InitWithSeed("Don't Panic");

            grain.SeedFldInt(42).Wait();
            grain.SeedFldStr("Don't Panic").Wait();

            ComplexGrainProperties prop = grain.GetProperties().Result;
            Helper.AreEqual(i, prop.FldInt);
            Helper.AreEqual(s, prop.FldStr);

        }

        [TestMethod]
        public async Task TestLinkedList()
        {
            ILinkedListGrain head;
            ILinkedListGrain prev = head = LinkedListGrainFactory.GetGrain(GetRandomGrainId());
            head.SetValue(42).Wait();
            for (int i = 1; i < 5; i++)
            {
                ILinkedListGrain current = LinkedListGrainFactory.GetGrain(GetRandomGrainId());
                prev.SetNext(current).Wait();
                current.SetValue(42 + i).Wait();
                prev = current;
            }
            ILinkedListGrain toCheck = head;
            for (int i = 0; i < 5; i++)
            {
                Assert.AreEqual(await toCheck.Value, 42 + i);
                Assert.IsNotNull(toCheck.Next);
                toCheck = await toCheck.Next;
            }
            // last one should have null
            //Assert.IsNull(toCheck.Next);

            toCheck = head;
            for (int i = 0; i < 5; i++)
            {
                LinkedListGrainProperties props = toCheck.GetProperties().Result;
                Assert.AreEqual(props.Value, 42 + i);
                if(i!=4) Assert.IsNotNull(props.Next);
                else Assert.IsNull(props.Next);
                toCheck = props.Next;
            }
            
        }
    }
}
