using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrainInterfaces;

namespace UnitTests 
{
    [TestClass]
    public class IndexGrain : UnitTestBase
    {
        public IndexGrain()
            : base(true)
        {
        }
        
        [ClassCleanup]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        /// <summary>
        /// Illustrates creating grain thru writer interface and querying via reader interface
        /// ReaderFactory should not support create (should have [Creatable=false]
        /// </summary>
//        [TestMethod, TestCategory("Failures")]        
//        public void IndexedGrainUsingWriterInterface()
//        {
//            WriterFactory.DeleteWhere(_ => true).Wait();
//            var row1 = WriterFactory.CreateGrain(0, 0);
//            var row2 = WriterFactory.CreateGrain(1, 1);
//            var list = ReaderFactory.Where(x => x.PrimaryKey == 0 && x.SecondaryKey == 0);
//            Assert.IsNotNull(list.Result);
//            Assert.AreEqual(1, list.Result.Count);
//            list = ReaderFactory.Where(x => x.PrimaryKey == 1 && x.SecondaryKey == 1);
//            Assert.AreEqual(1, list.Result.Count);
//            list = ReaderFactory.Where(x => x.PrimaryKey == 1 && x.SecondaryKey == 0);
//            Assert.AreEqual(0, list.Result.Count);
//            list = ReaderFactory.Where(x => x.PrimaryKey == 0 && x.SecondaryKey == 1);
//            Assert.AreEqual(0, list.Result.Count);
//            list = ReaderFactory.Where(x => x.PrimaryKey == 0 && x.SecondaryKey == 1);
//            Assert.AreEqual(0, list.Result.Count);
//            list = ReaderFactory.Where(x => x.PrimaryKey == 0 || x.PrimaryKey == 1);
//            Assert.AreEqual(2, list.Result.Count);
//            list = ReaderFactory.Where(x => x.SecondaryKey == 0 || x.SecondaryKey == 1);
//            Assert.AreEqual(2, list.Result.Count);
//            var reader = ReaderFactory.LookupPrimaryKey(0);
//            Assert.AreEqual(reader.PrimaryKey.Result, 0);
//// todo: Entity Framework does not yet support multiple unique keys
//#if TODO
//            reader = ReaderFactory.LookupSecondaryKey(1);
//            Assert.AreEqual(reader.SecondaryKey.Result, 1);
//#endif
//        }



        /// <summary>
        /// Similar to previous example accept that a third creatable interface is introduced which is polymorphic with both reader and writer (similar patter to multifacet grain)
        /// </summary>
//        [TestMethod, TestCategory("Failures")]
//        public void IndexedGrainUsingMultiFacetInterface()
//        {
//            WriterFactory.DeleteWhere(_ => true).Wait();
//            var row1 = WriterFactory.CreateGrain(0, 0);
//            var row2 = WriterFactory.CreateGrain(1, 1);
//            var list = ReaderFactory.Where(x => x.PrimaryKey == 0 && x.SecondaryKey == 0);
//            Assert.AreEqual(1, list.Result.Count);
//            list = ReaderFactory.Where(x => x.PrimaryKey == 1 && x.SecondaryKey == 1);
//            Assert.AreEqual(1, list.Result.Count);
//            list = ReaderFactory.Where(x => x.PrimaryKey == 1 && x.SecondaryKey == 0);
//            Assert.AreEqual(0, list.Result.Count);
//            list = ReaderFactory.Where(x => x.PrimaryKey == 1 || x.PrimaryKey == 0);
//            Assert.AreEqual(2, list.Result.Count);
//            list = ReaderFactory.Where(x => x.PrimaryKey == 0 && x.SecondaryKey == 1);
//            Assert.AreEqual(0, list.Result.Count);
//            list = ReaderFactory.Where(x => x.SecondaryKey == 1 || x.SecondaryKey == 0);
//            Assert.AreEqual(2, list.Result.Count);
//            var reader = ReaderFactory.LookupPrimaryKey(0);
//            Assert.AreEqual(reader.PrimaryKey.Result, 0);
//// todo: Entity Framework does not yet support multiple unique keys
//#if TODO
//            reader = ReaderFactory.LookupSecondaryKey(1);
//            Assert.AreEqual(reader.SecondaryKey.Result, 1);
//#endif
//        }

        //[TestMethod, TestCategory("Nightly"), TestCategory("General")]
        //[ExpectedException(typeof(System.InvalidOperationException))]
        //public void IndexedGrainUniquenessContraintViolationPrimaryKey()
        //{
        //    WriterFactory.DeleteWhere(_ => true).Wait();
        //    try 
        //    {
        //        var row1 = WriterFactory.CreateGrain(0, 0);
        //        row1.Wait();
        //        Assert.AreEqual(0, row1.PrimaryKey.Result);
        //        var row2 = WriterFactory.CreateGrain(0, 1);
        //        row2.Wait();
        //    }
        //    catch (Exception exc)
        //    {
        //        while (exc is AggregateException)
        //        {
        //            exc = exc.InnerException;
        //        }
        //        throw exc;
        //    }
        //}

        // todo: Entity Framework does not yet support multiple indices
        //[TestMethod]
        //[ExpectedException(typeof(System.InvalidOperationException))]
        //public void IndexedGrainUniquenessContraintViolationSecondaryKey()
        //{
        //    try
        //    {
        //        var row1 = WriterFactory.CreateGrain(1, 0);
        //        Assert.AreEqual(1, row1.PrimaryKey.Result);
        //        var row2 = WriterFactory.CreateGrain(0, 0);
        //        row2.Wait();
        //    }
        //    catch (Exception exc)
        //    {
        //        while (exc is AggregateException)
        //        {
        //            exc = exc.InnerException;
        //        }
        //        throw exc;
        //    }
        //}

        //[TestMethod, TestCategory("Nightly"), TestCategory("General")]
        //[ExpectedException(typeof(System.InvalidOperationException))]
        //public void IndexedGrain_CreateSameGrainTwiceFails()
        //{
        //    int pk = 11;

        //    WriterFactory.DeleteWhere((g) => g.PrimaryKey == pk).Wait();

        //    var row1 = WriterFactory.CreateGrain(pk);
        //    row1.Wait();

        //    try
        //    {
        //        var row2 = WriterFactory.CreateGrain(pk);
        //        row2.Wait();
        //    }
        //    catch (Exception exc)
        //    {
        //        while (exc is AggregateException)
        //        {
        //            exc = exc.InnerException;
        //        }
        //        throw exc;
        //    }
        //}

        //[TestMethod, TestCategory("Nightly"), TestCategory("General")]
        //public void IndexedGrain_CreateSameGrainAfterDelete()
        //{
        //    int pk = 12;

        //    WriterFactory.DeleteWhere((g) => g.PrimaryKey == pk).Wait(); // Should be a no-op

        //    var row1 = WriterFactory.CreateGrain(pk);
        //    row1.Wait();

        //    WriterFactory.Delete(row1).Wait();
            
        //    var row2 = WriterFactory.CreateGrain(pk);
        //    row2.Wait();
        //}

        //[TestMethod, TestCategory("Failures")]
        //public void IndexedGrain_CreateSameGrainAfterDeleteWhere()
        //{
        //    int pk = 13;

        //    WriterFactory.DeleteWhere((g) => g.PrimaryKey == pk).Wait(); // Should be a no-op

        //    var row1 = WriterFactory.CreateGrain(pk);
        //    row1.Wait();

        //    WriterFactory.DeleteWhere((g) => g.PrimaryKey == pk).Wait();

        //    var row2 = WriterFactory.CreateGrain(pk);
        //    row2.Wait();
        //}

        //[TestMethod]
        ////[ExpectedException(typeof(KeyNotFoundException))]
        //public void IndexedGrain_ContinueWithAfterLookupNotFound()
        //{
        //    int pk = 14;

        //    WriterFactory.DeleteWhere((g) => g.PrimaryKey == pk).Wait(); // Should be a no-op

        //    var row1 = WriterFactory.LookupPrimaryKey(pk);
        //    Assert.IsNotNull(row1);
        //    row1.ContinueWith(
        //        () => {
        //            Assert.Fail("Lookup(" + pk + ") should not have succeeded - received " + row1);
        //        },
        //        (exc) => {
        //            while (exc is AggregateException) exc = exc.InnerException;

        //            if (exc is KeyNotFoundException) return;

        //            throw exc;
        //        }
        //    ).Wait();
        //}

        //[TestMethod]
        ////[ExpectedException(typeof(KeyNotFoundException))]
        //public void IndexedGrain_ContinueWithCallsAfterLookupNotFound()
        //{
        //    int pk = 15;

        //    WriterFactory.DeleteWhere((g) => g.PrimaryKey == pk).Wait(); // Should be a no-op

        //    var row1 = WriterFactory.LookupPrimaryKey(pk);
        //    Assert.IsNotNull(row1);
        //    row1.WriteValues(1,2,3).ContinueWith(
        //        () =>
        //        {
        //            Assert.Fail("Lookup(" + pk + ") should not have succeeded - received " + row1);
        //        },
        //        (exc) =>
        //        {
        //            while (exc is AggregateException) exc = exc.InnerException;

        //            if (exc is KeyNotFoundException) return;

        //            throw exc;
        //        }
        //    ).Wait();
        //}
    }
}
