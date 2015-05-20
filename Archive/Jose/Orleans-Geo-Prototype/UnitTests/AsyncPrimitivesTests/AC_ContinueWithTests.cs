using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using System.Diagnostics;
using System.Threading;

#pragma warning disable 618

namespace UnitTests
{
    /// <summary>
    /// Summary description for UnitTest1
    /// </summary>
    [TestClass]
    public class AC_ContinueWithTests
    {
        private Logger logger = Logger.GetLogger("AC_ContinueWithTests", Logger.LoggerType.Application);

        public AC_ContinueWithTests()
        {
            logger.Info("----------------------------- STARTING AC_ContinueWithTests -------------------------------------");
        }

        [TestInitialize]
        [TestCleanup]
        public void MyTestCleanup()
        {
            OrleansTask.Reset();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_BasicContinueWith()
        {
            AsyncValue<int> promise1 = AsyncValue<int>.StartNew(() =>
            {
                Thread.Sleep(1000);
                return 1000;
            });
            AsyncCompletion promise2 = promise1.ContinueWith(promise =>
            {
                Thread.Sleep(2000);
            });
            AsyncValue<string> promise3 = promise1.ContinueWith<string>(promise =>
            {
                Thread.Sleep(2000);
                return "BB";
            });
            AsyncValue<float> promise4 = promise1.ContinueWith<float>(promise =>
            {
                Thread.Sleep(2000);
                return 0;
            });

            promise1.Wait();
            promise2.Wait();
            promise3.Wait();
            promise4.Wait();
            logger.Info("DONE."); ;
        }

        private void AC_ContinueWithErrors1(bool doError, bool doRecover, bool useExcHandler)
        {
            logger.Info("Starting AC_ContinueWithErrors doError = " + doError + " doRecover = " + doRecover + " useExcHandler = " + useExcHandler);
            AsyncCompletion acPromise = AsyncCompletion.StartNew(() =>
            {
                logger.Info("1. Inside acPromise.");
                Thread.Sleep(1000);
                if (doError)
                {
                    logger.Info("2. About to throw.");
                    throw new ArgumentException("AC_ContinueWithErrors throwing");
                }
                logger.Info("3. acPromise done");
            });

            Action<Exception> excHandler = (Exception exc) =>
            {
                logger.Info("4. Inside excHandler.");
                if (!doError)
                {
                    logger.Info("5. About to fail.");
                    Assert.Fail("Should not have executed an error continuation");
                }
                if (doRecover)
                {
                    logger.Info("6. Error ContinueWith done OK.");
                    return;
                }
                else
                {
                    logger.Info("7. Error ContinueWith throwing.");
                    throw exc;
                }
            };
            Action succHandler = ( ) =>
            {
                logger.Info("8. Inside succHandler.");
                if (doError)
                {
                    logger.Info("9. About to fail.");
                    Assert.Fail("Should not have executed a success continuation");
                }
                logger.Info("10. Success ContinueWith done");
            };

            AsyncCompletion contPromise = null;
            if (useExcHandler)
            {
                logger.Info("11. About to ContinueWith");
                contPromise = acPromise.ContinueWith(succHandler, excHandler);
            }
            else
            {
                logger.Info("12. About to ContinueWith.");
                contPromise = acPromise.ContinueWith(succHandler);
            }

            try
            {
                logger.Info("13. Before Wait().");
                if (contPromise == null)
                {
                    logger.Info("14. contPromise == null");
                }
                Assert.IsNotNull(contPromise);
                contPromise.Wait();
                logger.Info("15. After Wait().");
                if (doError && !doRecover)
                {
                    Assert.Fail("16. Should have thrown");
                }
            }
            catch (Exception exc)
            {
                logger.Error(0, "17. Inside catch = " + doError + " doRecover = " + doRecover, exc);
                Assert.IsTrue(doError, "contPromise Should not have thrown exception since doError = " + doError);
                Assert.IsFalse(doRecover, "contPromise Should not have thrown exception since doRecover = " + doRecover);
            }
            logger.Info("--------------------------------------------------------\n");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWith_AC()
        {
            //OrleansTask.InitializeSchedulerForTesting(new object());

            AC_ContinueWithErrors1(false, true, true);
            AC_ContinueWithErrors1(false, false, true);
            AC_ContinueWithErrors1(true, true, true);
            AC_ContinueWithErrors1(true, false, true);

            AC_ContinueWithErrors1(true, false, false);
            AC_ContinueWithErrors1(false, false, false);
        }

        private void AV_ContinueWithErrors1(bool doError, bool doRecover, bool useExcHandler)
        {
            logger.Info("Starting AV_ContinueWithErrors1 doError = " + doError + " doRecover = " + doRecover);
            AsyncValue<int> acPromise = AsyncValue<int>.StartNew(() =>
            {
                Thread.Sleep(1000);
                if (doError)
                {
                    throw new ArgumentException("AV_ContinueWithErrors1 throwing");
                }
                logger.Info("avPromise done");
                return 7;
            });
            Func<Exception, int> excHandler = (Exception exc) =>
            {
                if (!doError)
                {
                    Assert.Fail("Should not have executed an error continuation");
                }
                if (doRecover)
                {
                    logger.Info("Error ContinueWith done OK.");
                    return 20;
                }
                else
                {
                    logger.Info("Error ContinueWith throwing.");
                    throw exc;
                }
            };
            Func<int, int> succHandler = (int i) =>
            {
                if (doError)
                {
                    Assert.Fail("Should not have executed a success continuation");
                }
                logger.Info("Success ContinueWith done");
                return i * 2;
            };

            AsyncValue<int> contPromise = null;
            if (useExcHandler)
            {
                contPromise = acPromise.ContinueWith<int>(succHandler, excHandler);
            }
            else
            {
                contPromise = acPromise.ContinueWith<int>(succHandler);
            }

            try
            {
                int result = contPromise.GetValue();
                if (doError && !doRecover)
                {
                    Assert.Fail("Should have thrown");
                }
                if (!doError)
                {
                    Assert.AreEqual(14, result);
                }
                if (doError && doRecover)
                {
                    Assert.AreEqual(20, result);
                }
            }
            catch (Exception)
            {
                Assert.IsTrue(doError, "contPromise Should not have thrown exception since doError = " + doError);
                Assert.IsFalse(doRecover, "contPromise Should not have thrown exception since doRecover = " + doRecover);
            }
            logger.Info("--------------------------------------------------------\n");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWith_AV()
        {
            AV_ContinueWithErrors1(false, true, true);
            AV_ContinueWithErrors1(false, false, true);
            AV_ContinueWithErrors1(true, true, true);
            AV_ContinueWithErrors1(true, false, true);

            AV_ContinueWithErrors1(true, false, false);
            AV_ContinueWithErrors1(false, false, false);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWithAsyncFunc()
        {
            Stopwatch stopwatch1 = new Stopwatch();
            Stopwatch stopwatch2 = new Stopwatch();
            int timeout = 3000;
            stopwatch2.Start();

            AsyncValue<int> promise = AsyncValue<int>.StartNew(() => { Thread.Sleep(500); return 1; }); // errorGrain.GetAxB(1, 2);
            AsyncValue<string> contPromise = promise.ContinueWith<string>(ivalue =>
            {
                stopwatch1.Start();
                //return "BLA-BLA";
                return AsyncValue<string>.StartNew(() =>
                {
                    Thread.Sleep(timeout);
                    return "BLA-BLA";
                });
            });

            contPromise.Wait();
            stopwatch1.Stop();
            stopwatch2.Stop();
            int expected = timeout - 100;
            Assert.IsTrue(stopwatch1.ElapsedMilliseconds >= expected, "Waited for " + stopwatch1.ElapsedMilliseconds + " which is shorter than " + expected + "ms");
            Assert.IsTrue(stopwatch2.ElapsedMilliseconds >= (expected + 500), "Waited for " + stopwatch2.ElapsedMilliseconds + " which is shorter than " + (expected + 500) + "ms");
        }


        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWithAsyncException()
        {
            Stopwatch stopwatch1 = new Stopwatch();
            Stopwatch stopwatch2 = new Stopwatch();
            Stopwatch stopwatch3 = new Stopwatch();
            int timeout = 3000;
            bool doThrow  = true;
            stopwatch1.Start();

            AsyncValue<int> promise = AsyncValue<int>.StartNew(() => { Thread.Sleep(500); return 1; });
            AsyncValue<string> contPromise = promise.ContinueWith<string>(ivalue =>
            {
                stopwatch2.Start();
                return AsyncValue<string>.StartNew(() =>
                {
                    if (!doThrow)
                        return "";
                    Thread.Sleep(timeout);
                    throw new Exception("BLA-BLA-BLA.");
                });
            }, (Exception exc1) => 
            {
                stopwatch3.Start();
                Thread.Sleep(timeout);
                return "BOOM";
            });

            contPromise.Wait();
            stopwatch1.Stop();
            stopwatch2.Stop();
            stopwatch3.Stop();
            int expected1 = timeout + 500 - 100 ;
            int expected2 = 2 * timeout   - 100;
            int expected3 = timeout       - 100;
            Assert.IsTrue(stopwatch1.ElapsedMilliseconds >= expected1, "Waited for " + stopwatch1.ElapsedMilliseconds + " which is shorter than " + expected1 + "ms");
            Assert.IsTrue(stopwatch2.ElapsedMilliseconds >= expected2, "Waited for " + stopwatch2.ElapsedMilliseconds + " which is shorter than " + expected2 + "ms");
            Assert.IsTrue(stopwatch3.ElapsedMilliseconds >= expected3, "Waited for " + stopwatch2.ElapsedMilliseconds + " which is shorter than " + expected3 + "ms");
        }

        private AsyncValue<string> Lookup2()
        {
            AsyncValue<int> referencePromise = AsyncValue<int>.StartNew(() => { return 1; });
            AsyncValue<float> cont1 = referencePromise.ContinueWith<float>((int ivalue1) =>
            {
                logger.Info("First ContinueWith start");
                AsyncValue<float> invoke = AsyncValue<float>.StartNew(() => { Thread.Sleep(2000); return (float)2.0; });
                return invoke;
            });
            AsyncValue<string> cont2 = cont1.ContinueWith<string>((float fvalue2) =>
                {
                    logger.Info("Second ContinueWith start");
                    Thread.Sleep(2000);
                    logger.Info("Second ContinueWith end");
                    return "BLA";
                });
            referencePromise.Ignore();
            cont1.Ignore();
            return cont2;
        }

        private AsyncValue<string> Lookup1()
        {
            AsyncValue<int> referencePromise = AsyncValue<int>.StartNew(() => { return 1; });
            return referencePromise.ContinueWith<string>((int ivalue1) =>
            {
                logger.Info("First ContinueWith start");
                AsyncValue<float> invoke = AsyncValue<float>.StartNew(() => { Thread.Sleep(2000); return (float)2.0; });
                return invoke.ContinueWith<string>((float fvalue2) =>
                    {
                        logger.Info("Second ContinueWith start");
                        Thread.Sleep(3000);
                        logger.Info("Second ContinueWith end");
                        return "BLA";
                    });;
            });
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWith_DoubleAsyncContinueWith()
        {
            Stopwatch stopwatch1 = new Stopwatch();
            int timeout = 5000;
            stopwatch1.Start();
            AsyncValue<string> contPromise = Lookup1();
            AsyncCompletion compl = contPromise.ContinueWith(() => { logger.Info("All done!"); stopwatch1.Stop(); });
            contPromise.Ignore();
            compl.Wait();
            Assert.IsTrue(stopwatch1.ElapsedMilliseconds +100 >= timeout, "Waited for " + stopwatch1.ElapsedMilliseconds + " which is shorter than " + timeout + "ms");
        }

        //--------------------------------------------

        private AsyncCompletion AsyncOperation1()
        {
            AsyncValue<int> referencePromise = AsyncValue<int>.StartNew(() => { Thread.Sleep(2000); return 1; });
            return referencePromise.ContinueWith<string>((int ivalue1) =>
            {
                logger.Info("First ContinueWith start");
                AsyncValue<string> invoke = AsyncValue<string>.StartNew(() => { Thread.Sleep(3000); return "BBB"; });
                return invoke;
            });
        }
        private AsyncCompletion AsyncOperation2()
        {
            AsyncValue<int> referencePromise = AsyncValue<int>.StartNew(() => { Thread.Sleep(2000); return 1; });
            return referencePromise.ContinueWith((int ivalue1) =>
            {
                logger.Info("First ContinueWith start");
                AsyncValue<string> invoke = AsyncValue<string>.StartNew(() => { Thread.Sleep(3000); logger.Info("Inner invoke done"); return "BBB"; });
                AsyncCompletion invoke2 = (AsyncCompletion)(invoke);
                return invoke2;
            });
        }

        private AsyncCompletion AsyncOperation3()
        {
            AsyncCompletion referencePromise = AsyncCompletion.StartNew(() => { Thread.Sleep(2000); });
            return referencePromise.ContinueWith( () =>
            {
                logger.Info("First ContinueWith start");
                AsyncCompletion invoke = AsyncCompletion.StartNew(() => { Thread.Sleep(3000); logger.Info("Inner invoke done"); });
                return invoke;
            });
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWith_AsyncValueOfAsyncCompletion1()
        {
            Stopwatch stopwatch1 = new Stopwatch();
            int timeout = 5000;
            stopwatch1.Start();
            AsyncCompletion contPromise = AsyncOperation1();
            contPromise.Wait();
            stopwatch1.Stop();
            //AsyncValue<string> apromise = (AsyncValue<string>)contPromise;
           // string value = (string)apromise.GetValue();
            //string value = (string)contPromise.GetObjectValue();
            string value = "";
            logger.Info("Waited for " + stopwatch1.ElapsedMilliseconds + " for value " + value);
            Assert.IsTrue(stopwatch1.ElapsedMilliseconds + 100 >= timeout, "Waited for " + stopwatch1.ElapsedMilliseconds + " which is shorter than " + timeout + "ms");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWith_AsyncValueOfAsyncCompletion2()
        {
            Stopwatch stopwatch1 = new Stopwatch();
            int timeout = 5000;
            stopwatch1.Start();
            AsyncCompletion contPromise = AsyncOperation2();
            contPromise.Wait();
            stopwatch1.Stop();
            logger.Info("Waited for " + stopwatch1.ElapsedMilliseconds);
            Assert.IsTrue(stopwatch1.ElapsedMilliseconds + 100 >= timeout, "Waited for " + stopwatch1.ElapsedMilliseconds + " which is shorter than " + timeout + "ms");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWith_AsyncValueOfAsyncCompletion3()
        {
            Stopwatch stopwatch1 = new Stopwatch();
            int timeout = 5000;
            stopwatch1.Start();
            AsyncCompletion contPromise = AsyncOperation3();
            contPromise.Wait();
            stopwatch1.Stop();
            logger.Info("Waited for " + stopwatch1.ElapsedMilliseconds);
            Assert.IsTrue(stopwatch1.ElapsedMilliseconds + 100 >= timeout, "Waited for " + stopwatch1.ElapsedMilliseconds + " which is shorter than " + timeout + "ms");
        }
    }
}

#pragma warning restore 618
