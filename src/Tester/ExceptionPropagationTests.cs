using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using UnitTests.GrainInterfaces;
using UnitTests.Tester;

namespace UnitTests.General
{
    using System;

    /// <summary>
    /// Tests that exceptions are correctly propagated.
    /// </summary>
    [TestClass]
    public class ExceptionPropagationTests : HostedTestClusterEnsureDefaultStarted
    {
        [TestMethod, TestCategory("BVT"), TestCategory("Functional")]
        public async Task TaskCancelationPropagation()
        {
            IExceptionGrain grain = GrainFactory.GetGrain<IExceptionGrain>(GetRandomGrainId());
            var actualException = default(Exception);
            try
            {
                await grain.Cancelled();
            }
            catch (Exception exception)
            {
                actualException = exception;
            }

            Assert.IsNotNull(actualException, "Expected grain call to throw a cancellation exception.");
            Assert.IsTrue(actualException is AggregateException);
            Assert.AreEqual(
                typeof(TaskCanceledException),
                ((AggregateException)actualException).InnerException.GetType());
        }
    }
}
