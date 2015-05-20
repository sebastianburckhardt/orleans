using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Runtime;

namespace UnitTests
{
    // replace with persistence tests on new framework
#if TODO
    [TestClass]
    public class FileStoreTest
    {
        private static IPersistenceStore stateStore;
        private static string fileName = "file3.txt";
        private static int DATA_SIZE = 10000;
        private static int NUM_CONCURRENT_WRITERS = 10;
        private static byte[] dataBuffer0 = new byte[DATA_SIZE];
        private static byte[] dataBuffer1 = new byte[DATA_SIZE];
        private static byte[] dataBuffer2 = new byte[DATA_SIZE];

        public FileStoreTest() 
        {
            ServerConfigManager configManager = ServerConfigManager.LoadConfigManager();
            stateStore = (new FilePersistenceProvider()).OpenStore(configManager.FileStoreConfig);
            for (int i = 0; i < DATA_SIZE; i++)
            {
                dataBuffer0[i] = 5;
                dataBuffer1[i] = 7;
                dataBuffer2[i] = 9;
            }
            //byte[] dataBuffer = TestSerializer.SerializeObject(data);
        }

        private byte[] pickRandomBuffer()
        {
            int num = (new Random()).Next(3);
            if (num == 0) return dataBuffer0;
            if (num == 1) return dataBuffer1;
            if (num == 2) return dataBuffer2;
            else return dataBuffer0;
        }

        public StoreVersion ReadFile(out byte[] fileData, bool mustSucceed)
        {
            StoreVersion prevVersion = StoreVersion.Null;
            fileData = null;

            StoreOperationResult readResult = stateStore.ReadItem(fileName).Result;
            if (mustSucceed)
            {
                Assert.AreEqual(StoreOperationResultCode.Success, readResult.Result);
            }
            if (readResult.Result == StoreOperationResultCode.ItemNotFound)
            {
            }
            else if (readResult.Result == StoreOperationResultCode.Success)
            {
                prevVersion = readResult.Version;
                fileData = readResult.Data;
            }
            else
            {
                Assert.Fail("Shouldn't happen.");
            }
            return prevVersion;
        }

        public void ValidateRead(byte[] fileData)
        {
            Assert.AreEqual(DATA_SIZE, fileData.Length);
            byte first = 0;

            for (int i = 0; i < fileData.Length; i++)
            {
                if (first==0)
                {
                    first = fileData[i];
                }
                else
                {
                    Assert.AreEqual(first, fileData[i]);
                }
            }
        }

        [TestMethod]
        public void FileStoreTest_SingleThreaded()
        {
            byte[] fileData = null;
            StoreVersion prevVersion = ReadFile(out fileData, false);
            Console.WriteLine("InFile version is " + prevVersion);

            StoreVersion writeVersion = prevVersion.IncrementSeqNumber();
            StoreOperationResult writeResult = stateStore.WriteItem(fileName, writeVersion, prevVersion, pickRandomBuffer()).Result;
            Assert.AreEqual(StoreOperationResultCode.Success, writeResult.Result);
            Assert.AreEqual(writeVersion, writeResult.Version);

            //----------------
            prevVersion = ReadFile(out fileData, true);
            ValidateRead(fileData);
            Assert.AreEqual(writeVersion, prevVersion);
        }

        public int FileStoreTest_OneConcurrentWriter()
        {
            int numWrites = 0;
            byte[] fileData = null;
            StoreVersion prevVersion = ReadFile(out fileData, false);
            Console.WriteLine("Read InFile version is " + prevVersion);

            StoreVersion writeVersion = prevVersion.IncrementSeqNumber();
            StoreOperationResult writeResult = stateStore.WriteItem(fileName, writeVersion, prevVersion, pickRandomBuffer()).Result;
            if (writeResult.Result == StoreOperationResultCode.NewerVersionExists)
            {
                Assert.IsTrue(writeResult.Version.CompareTo(prevVersion) > 0);
            }
            else
            {
                Assert.AreEqual(StoreOperationResultCode.Success, writeResult.Result);
                Assert.AreEqual(writeVersion, writeResult.Version);
                numWrites++;
            }
            //----------------
            prevVersion = ReadFile(out fileData, true);
            ValidateRead(fileData);
            Assert.IsTrue(prevVersion.CompareTo(writeVersion) >= 0);
            return numWrites;
        }


        [TestMethod]
        public void FileStoreTest_MultipleConcurrentAccesses()
        {
            int numWrites = 0;
            byte[] fileData = null;
            StoreVersion firstVersion = ReadFile(out fileData, false);
            Console.WriteLine("FIRST version is " + firstVersion);

            AsyncCompletion[] clients = new AsyncCompletion[NUM_CONCURRENT_WRITERS];
            for (int i = 0; i < clients.Length; i++)
            {
                clients[i] = AsyncCompletion.StartNew(() =>
                {
                    int writes = FileStoreTest_OneConcurrentWriter();
                    lock(this)
                    {
                        numWrites += writes;
                    }
                });
            }
            for (int i = 0; i < clients.Length; i++)
            {
                clients[i].Wait();
            }
            StoreVersion lastVersion = ReadFile(out fileData, true);
            Console.WriteLine("LAST version is " + lastVersion);
            Console.WriteLine("Number of succesfull writes is " + numWrites + " out of " + clients.Length + " attempted");
//            Assert.AreEqual(numWrites, lastVersion.SeqNumber - firstVersion.SeqNumber);
        }

        //internal static class TestSerializer // TODO: Default binary serialization for now. Might need a more robust solution later.
        //{
        //    public static object DeserializeObject(byte[] data)
        //    {
        //        if (data.Length == 0)
        //            return null;

        //        BinaryFormatter formatter = new BinaryFormatter();
        //        using (MemoryStream stream = new MemoryStream(data))
        //        {
        //            return formatter.Deserialize(stream);
        //        }
        //    }
        //    public static byte[] SerializeObject(object o)
        //    {
        //        if (o != null)
        //        {
        //            BinaryFormatter formatter = new BinaryFormatter();
        //            using (MemoryStream stream = new MemoryStream())
        //            {
        //                formatter.Serialize(stream, o);
        //                return stream.ToArray();
        //            }
        //        }
        //        else
        //            return null;
        //    }
        //}
    }
#endif
}

