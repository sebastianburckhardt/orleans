using System;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using System.Collections.Generic;

namespace UnitTests
{
    /// <summary>
    /// Summary description for ErrorHandlingGrainTest
    /// </summary>
    [TestClass]
    public class ErrorPropagationTest
    {
        private static readonly TimeSpan timeout = TimeSpan.FromSeconds(10);
        //ResultHandle result;

        [TestInitialize]
        [TestCleanup]
        public void MyTestCleanup()
        {
            OrleansTask.Reset();
        }

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext { get; set; }

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
        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        // in this test schedule ContinueWiths BEFORE the first promise fails. The should all fail regardless the order
        public void ContinueWithErrorPropagation1()
        {
            bool doThrow = true;
            ResultHandle result = new ResultHandle();
            AsyncValue<Int32> p1 = AsyncValue<Int32>.StartNew(() =>
                {
                    if (!doThrow)
                        return 0;
                    Thread.Sleep(1000);
                    throw new Exception("MyError1");
                });
            AsyncCompletion p2 = p1.ContinueWith(() =>{});
            AsyncCompletion p3 = p2.ContinueWith(() =>{});
            AsyncCompletion p4 = p3.ContinueWith(() =>{});

            try
            {
                p4.Wait();
                Assert.Fail();
            }
            catch (Exception exc1)
            {

                result.Result = 1;
                result.Exception = exc1;
                result.Done = true;
            }

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.AreEqual(1, result.Result);
            Assert.AreEqual(result.Exception.GetBaseException().Message, (new Exception("MyError1")).Message);
            Assert.IsTrue(p4.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(p3.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(p2.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(p1.Status == AsyncCompletionStatus.Faulted);
        }


        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        // in this test schedule ContinueWiths AFTER the first promise fails. The should all fail regardless the order
        public void ContinueWithErrorPropagation2()
        {
            ResultHandle result = new ResultHandle();
            bool doThrow = true;
            AsyncValue<Int32> p1 = AsyncValue<Int32>.StartNew(() =>
            {
                if (!doThrow)
                    return 0;
                throw new Exception("MyError1");
            });

            try
            {
                p1.Wait();
                Assert.Fail();
            }
            catch (Exception)
            {
                Assert.IsTrue(p1.Status == AsyncCompletionStatus.Faulted);
            }

            AsyncCompletion p2 = p1.ContinueWith(() => { });
            AsyncCompletion p3 = p2.ContinueWith(() => { });
            AsyncCompletion p4 = p3.ContinueWith(() => { });

            try
            {
                p4.Wait();
                Assert.Fail();
            }
            catch (Exception exc1)
            {
                
                result.Result = 1;
                result.Exception = exc1;
                result.Done = true;
            }
            try
            {
                p2.Wait();
                Assert.Fail();
            }
            catch (Exception){}

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.AreEqual(1, result.Result);
            Assert.AreEqual(result.Exception.GetBaseException().Message, (new Exception("MyError1")).Message);
            Assert.IsTrue(p4.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(p3.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(p2.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(p1.Status == AsyncCompletionStatus.Faulted);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        // in this test schedule ContinueWiths and fail one of the continuations. The first ones should finish OK, the dependant ones should fail.
        public void ContinueWithErrorPropagation3()
        {
            ResultHandle result = new ResultHandle();
            AsyncValue<Int32> p1 = AsyncValue<Int32>.StartNew(() =>
            {
                Thread.Sleep(1000);
                return 1;
            });

            AsyncCompletion p2 = p1.ContinueWith(() => { });
            AsyncCompletion p3 = p2.ContinueWith(() =>
            {
                throw new Exception("MyError1");
            });
            AsyncCompletion p4 = p3.ContinueWith(() => { });
            AsyncCompletion p5 = p4.ContinueWith(() => { });

            try
            {
                p5.Wait();
                Assert.Fail();
            }
            catch (Exception exc1)
            {
                result.Result = 1;
                result.Exception = exc1;
                result.Done = true;
            }
            try
            {
                p2.Wait();
                Assert.Fail();
            }
            catch (Exception) { }

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.AreEqual(1, result.Result);
            Assert.AreEqual(result.Exception.GetBaseException().Message, (new Exception("MyError1")).Message);
            Assert.IsTrue(p5.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(p4.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(p3.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(p2.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(p1.Status == AsyncCompletionStatus.CompletedSuccessfully);
        }

        public void ExceptionStackTrace()
        {
            ExceptionStackTrace(1);
            ExceptionStackTrace(2);
            ExceptionStackTrace(3);
            ExceptionStackTrace(4);
            ExceptionStackTrace(5);
            ExceptionStackTrace(6);
        }

        public void ExceptionStackTrace(int throwMethod)
        {
            AsyncValue<int> p1 = AsyncValue<int>.StartNew(() =>
            {
                int tmp = 0;
                if(tmp==1) return 0;
                throw new InvalidOperationException("First Exception!");
            }).ContinueWith(
                () => { return new AsyncValue<int>(2);  }, 
                (Exception exc) => 
                {
                    if (throwMethod == 1)
                        throw exc;
                    else if (throwMethod == 2)
                        throw new KeyNotFoundException("Second Exception!");
                    else if (throwMethod == 3)
                        throw new KeyNotFoundException("Second Exception!", exc);
                    else if (throwMethod == 4)
                        return new AsyncValue<int>(exc);
                    else if (throwMethod == 5)
                        return new AsyncValue<int>(new KeyNotFoundException("Second Exception!"));
                    else
                        return new AsyncValue<int>(new KeyNotFoundException("Second Exception!", exc));
                });
            try
            {
                p1.Wait();
            }
            catch (Exception exc) 
            {
                Console.WriteLine("\n----------------------------------------------");
                Console.WriteLine(Logger.PrintException(exc));
            }
        }
    }
}
