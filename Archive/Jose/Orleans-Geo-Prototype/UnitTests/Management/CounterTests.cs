using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Counters;

namespace UnitTests
{
    [TestClass]
    public class CounterTests
    {
        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Management")]
        public void Counter_InitialValue()
        {
            StatName name = new StatName("Counter1");
            IOrleansCounter<long> ctr = CounterStatistic.FindOrCreate(name);
            Assert.AreEqual(name.ToString(), ctr.Name);
            Assert.IsTrue(ctr.ToString().Contains(name.Name));
            Assert.AreEqual(0, ctr.GetCurrentValue());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Management")]
        public void Counter_SetValue()
        {
            Random rng = new Random();
            StatName name = new StatName("Counter2");
            int val = rng.Next(1000000);
            CounterStatistic ctr = CounterStatistic.FindOrCreate(name);
            ctr.IncrementBy(val);
            Assert.AreEqual(val, ctr.GetCurrentValue());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Management")]
        public void Counter_Increment()
        {
            StatName name = new StatName("Counter3");
            CounterStatistic ctr = CounterStatistic.FindOrCreate(name);
            Assert.AreEqual(0, ctr.GetCurrentValue());
            ctr.Increment();
            Assert.AreEqual(1, ctr.GetCurrentValue());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Management")]
        public void Counter_IncrementBy()
        {
            StatName name = new StatName("Counter4");
            Random rng = new Random();
            int val = rng.Next(1000000);
            CounterStatistic ctr = CounterStatistic.FindOrCreate(name);
            ctr.IncrementBy(val);
            Assert.AreEqual(val, ctr.GetCurrentValue());
            ctr.Increment();
            Assert.AreEqual(val + 1, ctr.GetCurrentValue());
            ctr.Increment();
            Assert.AreEqual(val + 2, ctr.GetCurrentValue());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Management")]
        public void Counter_IncrementFromMinInt()
        {
            StatName name = new StatName("Counter5");
            int val = int.MinValue;
            CounterStatistic ctr = CounterStatistic.FindOrCreate(name);
            ctr.IncrementBy(val);
            Assert.AreEqual(val, ctr.GetCurrentValue());
            ctr.Increment();
            Assert.AreEqual(val + 1, ctr.GetCurrentValue());
            ctr.Increment();
            Assert.AreEqual(val + 2, ctr.GetCurrentValue());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Management")]
        public void Counter_IncrementFromMaxInt()
        {
            StatName name = new StatName("Counter6");
            int val = int.MaxValue;
            long longVal = int.MaxValue;
            Assert.AreEqual(longVal, val);
            CounterStatistic ctr = CounterStatistic.FindOrCreate(name);
            ctr.IncrementBy(val);
            Assert.AreEqual(val, ctr.GetCurrentValue());
            ctr.Increment();
            Assert.AreEqual(longVal + 1, ctr.GetCurrentValue());
            ctr.Increment();
            Assert.AreEqual(longVal + 2, ctr.GetCurrentValue());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Management")]
        public void Counter_DecrementBy()
        {
            StatName name = new StatName("Counter7");
            int startValue = 10;
            int newValue = startValue - 1;
            CounterStatistic ctr = CounterStatistic.FindOrCreate(name);
            ctr.IncrementBy(startValue);
            Assert.AreEqual(startValue, ctr.GetCurrentValue());
            ctr.DecrementBy(1);
            Assert.AreEqual(newValue, ctr.GetCurrentValue());
        }

        //[TestMethod]
        //[ExpectedException(typeof(System.Security.SecurityException))]
        //public void AdminRequiredToRegisterCountersWithWindows()
        //{
        //    OrleansCounterBase.RegisterAllCounters();
        //}

        //[TestMethod]
        //public void RegisterCountersWithWindows()
        //{
        //    OrleansCounterBase.RegisterAllCounters(); // Requires RunAs Administrator
        //    Assert.IsTrue(
        //        AreWindowsPerfCountersAvailable(),
        //        "Orleans perf counters are registered with Windows");
        //}
    }
}
