using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Threading;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

//using Orleans.Communicators;

namespace UnitTests
{
    [TestClass]
    public class MessageCenterAdvancedTests
    {
        // Since the ICommunicator interface includes a bunch of delegates, we need to make these static so that the 
        // callbacks can see them
        //private MessageCenterCommunicator sender;
        //private MessageCenterCommunicator receiver;
        //private static readonly GrainID targetGrain = new GrainID();
        //private static readonly ActivationID targetActivation = new ActivationID();
        //private static readonly GrainID sendingGrain = new GrainID();
        //private static readonly ActivationID sendingActivation = new ActivationID();
        //private static readonly byte[] msg = { 0x01, 0x02, 0x03, 0x04 };
        //private static readonly byte[] reply = { 0x04, 0x03, 0x02, 0x01 };
        //private static readonly int receiverPort = 55667;
        //private AutoResetEvent doneFlag;

        public MessageCenterAdvancedTests()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        [TestInitialize]
        [TestCleanup]
        public void MyTestCleanup()
        {
            //OrleansTask.Reset();
        }

        //[TestMethod]
        public void MessageCenterTest_RestartReceiver()
        {
        //    StartReceiver();
        //    StartSender();

        //    DoTest();

        //    Console.WriteLine("Completed first pass, about to stop receiver");
        //    receiver.Stop();
        //    Console.WriteLine("Receiver stopped");
        //    Console.WriteLine("========================================================\n\n\n");

        //    //Thread.Sleep(5000);

        //    StartReceiver();
        //    ResetSender();  // To fix up routing
        //    Console.WriteLine("Receiver started");
        //    Console.WriteLine("========================================================\n\n\n");

        //    DoTest();

        //    sender.Stop();
        //    receiver.Stop();
        }

        //[TestMethod]
        //public void MessageCenterTest_RestartSender()
        //{
        //    StartReceiver();
        //    StartSender();

        //    DoTest();
           
        //    sender.Stop();
        //    StartSender();
        //    DoTest();

        //    sender.Stop();
        //    receiver.Stop();
        //}

        //[TestMethod]
        //public void MessageCenterTest_ResendMessage()
        //{
        //    StartSender();

        //    DoRetryTest();

        //    sender.Stop();
        //}

        //private int currentCount;
        //private string[] expectedMessages;
        //private int[] prefixLengths;
        //private int[] messageLengths;
        //private int[] messageOffsets;
        //private byte[] rawData;
        //private int currentOffset;

        //private void DoTestStep(IncomingMessageAcceptor.ReceiveCallbackContext rcc, int byteCount)
        //{
        //    if (byteCount < 0)
        //    {
        //        byteCount = rawData.Length - currentOffset;
        //    }

        //    // Calculate how many messages should end in this block
        //    int msgCount = 0;
        //    for (int i = 0; i < messageLengths.Length; i++)
        //    {
        //        int end = messageOffsets[i] + messageLengths[i];
        //        if ((currentOffset < end) && (end <= currentOffset + byteCount))
        //        {
        //            msgCount++;
        //        }
        //    }
        //    int n = currentCount + msgCount;

        //    Array.Copy(rawData, currentOffset, rcc.Buffer, 0, byteCount);
        //    currentOffset += byteCount;

        //    rcc.ProcessReceivedBuffer(byteCount, MessageHandler);
        //    Assert.AreEqual<int>(n, currentCount, "Incorrect number of messages found");

        //    // Now let's see if our state is correct
        //    if (n < expectedMessages.Length)
        //    {
        //        //Console.WriteLine("Processed a test step");
        //        //Console.WriteLine("    Current count:" + n);
        //        //Console.WriteLine("   Current offset:" + currentOffset);
        //        int j = currentOffset - messageOffsets[n];
        //        if (j < prefixLengths[n])
        //        {
        //            Assert.AreEqual<int>(-1, rcc.ExpectedLength, "Expected length is incorrect in the middle of the message " + n + " prefix");
        //        }
        //        else
        //        {
        //            Assert.AreEqual<int>(messageLengths[n] - j, rcc.ExpectedLength,
        //                "ExpectedLength is incorrect in the middle of the message " + n + " body");
        //            Assert.AreEqual<int>(j - prefixLengths[n], (int)rcc.Data.Length, "Data length is incorrect in the middle of the message " + n + " body");
        //            //Console.WriteLine("      Body offset:" + rcc.Data.Length);
        //        }
        //    }
        //}

        //[TestMethod]
        //public void MessageCenterTest_BufferProcessing()
        //{
        //    expectedMessages = new string[] { "Nineteen characters", "Only 7!", "Only nine", "Four", 
        //                            "Eight...", "Twenty characters!!!", "It's eleven" };
        //    prefixLengths = new int[expectedMessages.Length];
        //    messageLengths = new int[expectedMessages.Length];
        //    messageOffsets = new int[expectedMessages.Length];

        //    currentCount = 0;

        //    int n = 0;
        //    int l = 0;
        //    StringBuilder sb = new StringBuilder();
        //    //Console.WriteLine(" #  Length  PfxLen  Offset");
        //    foreach (string s in expectedMessages)
        //    {
        //        messageOffsets[n] = l;
        //        sb.Append(s.Length);
        //        sb.Append('\n');
        //        prefixLengths[n] = sb.ToString().Length - messageOffsets[n];
        //        messageLengths[n] = s.Length + prefixLengths[n];
        //        l += messageLengths[n];
        //        sb.Append(s);
        //        //Console.WriteLine(" " + n + String.Format("  {0,6}  {1,6}  {2,6}", messageLengths[n], prefixLengths[n], messageOffsets[n]));
        //        n++;
        //    }
        //    rawData = ASCIIEncoding.ASCII.GetBytes(sb.ToString());
        //    int totalLength = rawData.Length;
            
        //    IncomingMessageAcceptor.ReceiveCallbackContext rcc = new IncomingMessageAcceptor.ReceiveCallbackContext(null, null);

        //    // Get exactly the first message (message 0)
        //    DoTestStep(rcc, messageLengths[0]);

        //    // Get the second and part of the third (prefix + 3)
        //    DoTestStep(rcc, messageLengths[1] + prefixLengths[2] + 3);

        //    // Get the rest of the third, all of the 4th, and just the prefix of the 5th
        //    DoTestStep(rcc, messageLengths[2] - prefixLengths[2] - 3 + messageLengths[3] + prefixLengths[4]);

        //    // Get the rest of the 5th and 1 byte of the prefix of the 6th
        //    DoTestStep(rcc, messageLengths[4] - prefixLengths[4] + 1);

        //    // More of the prefix of the 6th (which must be 10-99 characters long)
        //    DoTestStep(rcc, 1);

        //    // The rest of the prefix of the 6th (1 byte) and 3 bytes of the data
        //    DoTestStep(rcc, 4);

        //    // 5 more bytes of the data of the 6th
        //    DoTestStep(rcc, 5);

        //    // The rest of the 6th and one byte of the prefix of the 7th
        //    DoTestStep(rcc, messageLengths[5] - 10 + 1);

        //    // The rest of the data (DoTestStep handles -1 specially)
        //    DoTestStep(rcc, -1);
        //}

        //private void MessageHandler(IncomingMessageAcceptor ima, byte[] buffer)
        //{
        //    Assert.AreEqual<string>(expectedMessages[currentCount], ASCIIEncoding.ASCII.GetString(buffer), "Got a wrong message");
        //    currentCount++;
        //}

        //private void DoTest()
        //{
        //    doneFlag = new AutoResetEvent(false);
        //    sender.SendRemoteMessage(targetGrain, sendingGrain, sendingActivation, msg, doneFlag, (Action<Message, object>)ResponseReceivedCallback);
        //    Assert.IsTrue(doneFlag.WaitOne(5000), "No response received within 5 seconds");
        //    Console.WriteLine("========================================================\n\n\n");
        //    Console.WriteLine("Success!!");
        //}

        //private void DoRetryTest()
        //{
        //    doneFlag = new AutoResetEvent(false);
        //    sender.SendRemoteMessage(targetGrain, sendingGrain, sendingActivation, msg, doneFlag, (Action<Message, object>)RetryErrorReceivedCallback);
        //    Assert.IsTrue(doneFlag.WaitOne(5000), "No response received within 5 seconds");
        //    Console.WriteLine("========================================================\n\n\n");
        //    Console.WriteLine("Success!!");
        //}

        //private void StartSender()
        //{
        //    MessageCenter mc = new MessageCenter(new IPEndPoint(Utils.GetLocalIPAddress(), 0));
        //    mc.Router = new TestRouter(mc);
        //    sender = new MessageCenterCommunicator(mc);
        //    sender.Name = "sender";
        //    sender.Start();
        //    // Set up routing
        //    if (receiver != null)
        //    {
        //        sender.AddRouting(targetGrain, receiver.GetAddress());
        //    }
        //    else
        //    {
        //        sender.AddRouting(targetGrain, new SiloAddress(new IPEndPoint(Utils.GetLocalIPAddress(), receiverPort + 1)));
        //    }
        //}

        //private void ResetSender()
        //{
        //    // Set up routing
        //    if (receiver != null)
        //    {
        //        sender.AddRouting(targetGrain, receiver.GetAddress());
        //    }
        //    else
        //    {
        //        sender.AddRouting(targetGrain, new SiloAddress(new IPEndPoint(Utils.GetLocalIPAddress(), receiverPort + 1)));
        //    }
        //}

        //private void StartReceiver()
        //{
        //    MessageCenter mc = new MessageCenter(new IPEndPoint(Utils.GetLocalIPAddress(), receiverPort));
        //    mc.Router = new TestRouter(mc);
        //    receiver = new MessageCenterCommunicator(mc);
        //    receiver.Name = "receiver";
        //    receiver.RegisterRequestListener(targetGrain, RequestArrivedCallback);
        //    receiver.Start();
        //    // Set up the request-arrived callback
        //}

        //void RequestArrivedCallback(Message request)
        //{
        //    AssertArrayEquals<byte>(request.Body, msg, "Request message bodies don't agree");
        //    receiver.SendResponse(targetGrain, targetActivation, request, reply);
        //}

        //void ResponseReceivedCallback(Message response, Object context)
        //{
        //    AssertArrayEquals<byte>(response.Body, reply, "Response message bodies don't agree");
        //    Assert.AreEqual<Object>(context, doneFlag, "Context object is not correct");
        //    //AssertEquals<Object>(context, doneFlag, "Context object is not correct");
        //    doneFlag.Set();
        //}

        //void RetryErrorReceivedCallback(Message response, Object context)
        //{
        //    //string s = UTF8Encoding.UTF8.GetString(response.Body);
        //    Assert.AreEqual<Message.ResponseTypes>(Message.ResponseTypes.Rejection, response.Result, "Response type is incorrect");
        //    Assert.AreEqual<Object>(context, doneFlag, "Context object is not correct");
        //    doneFlag.Set();
        //}

        public static void AssertArrayEquals<T>(T[] val1, T[] val2, string error)
        {
            if (val1.Length == val2.Length)
            {
                for (int n = 0; n < val1.Length; n++)
                {
                    if (!val1[n].Equals(val2[n]))
                    {
                        throw new ApplicationException(error + ": value 1 is '" + val1.ToString() + "', value 2 is '" + val2.ToString() + "'");
                    }
                }
            }
            else
            {
                throw new ApplicationException(error + ": value 1 is '" + val1.ToString() + "', value 2 is '" + val2.ToString() + "'");
            }
        }
    }
}
