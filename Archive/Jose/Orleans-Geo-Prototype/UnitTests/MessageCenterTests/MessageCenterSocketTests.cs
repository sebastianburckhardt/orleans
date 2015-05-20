﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Threading;
using System.Diagnostics;
using System.Net.Sockets;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using System.Net.NetworkInformation;


#pragma warning disable 618

namespace UnitTests
{
    [TestClass]
    public class MessageCenterSocketTests
    {
        private static readonly byte[] msg = { 0x01, 0x02, 0x03, 0x04 };
        private static readonly int receiverPort = 55667;
        private Logger logger = Logger.GetLogger("MessageCenterSocketTests");
        private Socket sendSocket;
        private Socket acceptSocket;

        public MessageCenterSocketTests()
        {
        }

        //http://msdn.microsoft.com/en-us/library/ms738547(VS.85).aspx
        //http://msdn.microsoft.com/en-us/library/ms738547(VS.85).aspx

        //[TestMethod]
        public void DoTest()
        {
            DoTestOne();
            //DoTestOne();
            //DoTestOne();
            //DoTestOne();
        }

        public void TestBindAny()
        {
            IPAddress listenAddress = IPAddress.Any;
            IPEndPoint listenEndpoint = new IPEndPoint(listenAddress, 0);
            Socket listenSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            listenSock.Bind(listenEndpoint);
            Console.WriteLine("Bound to socket at local address " + listenSock.LocalEndPoint.ToString());
            listenSock.Listen(5);
            Console.WriteLine("Listening to socket at local address " + listenSock.LocalEndPoint.ToString());
            AutoResetEvent done = new AutoResetEvent(false);
            listenSock.BeginAccept(new AsyncCallback(AcceptHandler), new Tuple<Socket, AutoResetEvent>(listenSock, done));
            Console.WriteLine("Accepting on socket at local address " + listenSock.LocalEndPoint.ToString());
            Socket sendSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            sendSock.Connect(listenSock.LocalEndPoint);
            Console.WriteLine("Connected a socket at local address " + sendSock.LocalEndPoint.ToString() + " to remote address " + 
                sendSock.RemoteEndPoint.ToString());
            done.WaitOne(5000);
            sendSock.Close();
        }

        private static void AcceptHandler(IAsyncResult res)
        {
            Tuple<Socket, AutoResetEvent> info = res.AsyncState as Tuple<Socket, AutoResetEvent>;

            Socket receiveSock = info.Item1.EndAccept(res);
            Console.WriteLine("Opened a new receiving socket at local address " + receiveSock.LocalEndPoint.ToString());
            info.Item2.Set();
            receiveSock.Close();
        }

        public void TestNetworkInfo()
        {
            NetworkInterface[] nics = NetworkInterface.GetAllNetworkInterfaces();
            int i = 0;
            foreach (NetworkInterface nic in nics)
            {
                i++;
                Console.WriteLine("NIC " + i.ToString() + ": " + nic.Id);
                Console.WriteLine("            Name: " + nic.Name);
                Console.WriteLine("     Description: " + nic.Description);
                Console.WriteLine("  Interface type: " + nic.NetworkInterfaceType.ToString());
                IPInterfaceProperties ipp = nic.GetIPProperties();
                foreach (UnicastIPAddressInformation uipai in ipp.UnicastAddresses)
                {
                    Console.WriteLine("      IP Address: " + uipai.Address);
                }
            }
        }

        public void DoTestOne()
        {
            AutoResetEvent receiverAllDoneFlag = new AutoResetEvent(false);
            AutoResetEvent senderAllDoneFlag = new AutoResetEvent(false);
            AutoResetEvent waitForReceiverToStart = new AutoResetEvent(false);
            AutoResetEvent waitForReceiverToStop = new AutoResetEvent(false);

            AsyncCompletion.StartNew(() =>
            {
                try
                {
                    StartReceiver(waitForReceiverToStart);
                }
                catch (Exception exc)
                {
                    logger.Error(0, "", exc);
                }
                waitForReceiverToStop.Set();
                receiverAllDoneFlag.Set();
            }).Ignore();

            AsyncCompletion.StartNew(() =>
            {
                try
                {
                    StartSender(waitForReceiverToStart);
                    waitForReceiverToStop.WaitOne();
                    logger.Info("Sender is about to send for the second time on a send socket that is Connected = " + sendSocket.Connected);

                    int count = sendSocket.Send(msg);
                    logger.Info("Sender has sent for the second time " + count + " bytes.");
                    Assert.AreEqual(count, msg.Length, count.ToString());
                    sendSocket.Close();
                    logger.Info("Sender has finished for the 2nd time.");
                    senderAllDoneFlag.Set();
                }
                catch (Exception exc)
                {
                    logger.Error(0, "", exc);
                }
            }).Ignore();

            Assert.IsTrue(receiverAllDoneFlag.WaitOne(5000), "receiver did not finish");
            Assert.IsTrue(senderAllDoneFlag.WaitOne(5000), "sender did not finish");
            Console.WriteLine("========================================================\n\n\n");
        }

        private void StartSender(AutoResetEvent waitForReceiverToStart)
        {
            waitForReceiverToStart.WaitOne();
            IPEndPoint here = new IPEndPoint(OrleansConfiguration.GetLocalIPAddress(), receiverPort);
            byte[] buffer = new byte[10];

            sendSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            logger.Info("Sender is about to connect.");
            sendSocket.Connect(here);
            sendSocket.LingerState = new LingerOption(false, 0);

            logger.Info("Sender is about to send.");
            int count = sendSocket.Send(msg);
            logger.Info("Sender has sent.");
            Assert.AreEqual(count, msg.Length, count.ToString());
            //sendSocketForTCP.Close();
            logger.Info("Sender has finished.");
        }

        private void StartReceiver(AutoResetEvent waitForReceiverToStart)
        {
            IPEndPoint here = new IPEndPoint(OrleansConfiguration.GetLocalIPAddress(), receiverPort);
            byte[] buffer = new byte[msg.Length];

            acceptSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            acceptSocket.Bind(here);
            acceptSocket.Listen(10);

            waitForReceiverToStart.Set();
            logger.Info("*Receiver is about to start accepting.");

            Socket receiveSocket = acceptSocket.Accept();
            receiveSocket.LingerState = new LingerOption(true, 5);

            logger.Info("*Receiver is about to start receiving.");
            int count = receiveSocket.Receive(buffer, 0, msg.Length, SocketFlags.None);
            logger.Info("*Receiver has received.");
            MessageCenterAdvancedTests.AssertArrayEquals<byte>(buffer, msg, "Response message bodies don't agree");

            SocketManager.CloseSocket(acceptSocket);
            SocketManager.CloseSocket(receiveSocket);

            logger.Info("*Receiver has finished.");
        }
    }
}

#pragma warning restore 618
