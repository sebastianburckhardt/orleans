using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using PingUtil;
using System.IO;
using System.Diagnostics;
using System.Globalization;

namespace PingUtil
{
    /// <summary>
    /// Some log file
    /// </summary>
    public class Log
    {
        public enum Severity
        {
            Off = TraceLevel.Off,
            Error = TraceLevel.Error,
            Warning = TraceLevel.Warning,
            Info = TraceLevel.Info,
            Verbose = TraceLevel.Verbose,
        }

        private static string fileName;
        private static EventLog eventLog;
        private static readonly string DateTimeFormat = "yyyy-MM-dd " + "HH:mm:ss.fff 'GMT'"; // Example: 2010-09-02 09:50:43.341 GMT - Variant of UniversalSorta­bleDateTimePat­tern
        private static string Name;
        private static Severity severity;

        public static void Init(Severity severity, string name, string fileName, string eventName = "Windows Error Reporting")
        {
            Log.fileName = fileName;
            Log.Name = name;
            Log.severity = severity;
            try
            {
                eventLog = new EventLog("Application", ".", eventName);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error while creating event log {0}", e);
            }
        }

        public static void Write(Severity level, string format, params object[] arr)
        {
            if (level > Log.severity)
                return;
            lock (fileName)
            {
                string str = String.Format("[PingUtil: {0} {1}] {2}", DateTime.UtcNow.ToString(DateTimeFormat, CultureInfo.InvariantCulture), (Name != null ? Name : ""), string.Format(format, arr));
                try
                {
                    using (StreamWriter writer = new StreamWriter(fileName, append: true))
                    {
                        writer.WriteLine(str);
                        Console.WriteLine(str);
                        writer.Flush();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error while writing to file {0}. Exception = {1}", fileName, e);
                }
                try
                {
                    if (null != eventLog)
                    {
                        EventLogEntryType logEntryType = GetLogTypeFromSeverity(level);

                        eventLog.WriteEntry(str, logEntryType);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error while writing to event log. Exception = {0}", e);
                }
                
            }
        }

        /// <summary>
        /// Turns a Severity into an EventLogEntryType
        /// </summary>
        private static EventLogEntryType GetLogTypeFromSeverity(Severity severity)
        {
            switch(severity)
            {
                case Severity.Info:
                case Severity.Verbose:
                    return EventLogEntryType.Information;
                case Severity.Warning:
                    return EventLogEntryType.Warning;
                case Severity.Error:
                    return EventLogEntryType.Error;
                default:
                    return EventLogEntryType.Warning;
            }
        }
    }

    public class MessageInfo
    {
        /// <summary>
        /// Unique id 
        ///  "msg_+ agentName + uniqueMessageId_on_agent;
        /// </summary>
        public string MessageID { get; set; }
        /// <summary>
        /// Name of the agent sending
        /// </summary>
        public string From { get; set; }
        /// <summary>
        /// Name of the agent recieveing
        /// </summary>
        public string To { get; set; }

        public string SenderEndPoint { get; set; }

        public string RecieverEndPoint { get; set; }
        
        public DateTime TimeStamp { get; set; }
        /// <summary>
        /// what stage of the message 
        /// Currently just three
        ///   PING -> ACK -> COMPLETE
        ///   PING is repsonded with ACK
        ///   ACK makes the ping complete, a completed ping is not responed to 
        /// </summary>
        public string Stage { get; set; }

        public MessageInfo()
        {
            TimeStamp = DateTime.UtcNow;
        }
        
        /// <summary>
        /// Parses the messages from string
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public static MessageInfo Parse(string message)
        {
            MessageInfo info = new MessageInfo();
            string[] parts = message.Split(',');
            if (parts.Length < 7)
                throw new Exception("Message Corrupted/Ill formed");
            
            info.MessageID = parts[0];
            info.Stage = parts[1];
            info.From = parts[2];
            info.SenderEndPoint = parts[3];
            info.To = parts[4];
            info.RecieverEndPoint = parts[5];
            info.TimeStamp = DateTime.Parse(parts[6]);
            return info;
        }
        /// <summary>
        /// Writes message as string
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("{0},{1},{2},{3},{4},{5},{6}",
                            MessageID,
                            Stage,
                            From,
                            SenderEndPoint.ToString(),
                            To,
                            RecieverEndPoint.ToString(),
                            TimeStamp);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class Agent : IDisposable
    {
        public static int PING_PORT = 30042;
        
        private object lockObj = new object();

        private TimeSpan SendInterval;

        /// <summary>
        /// Use utf8
        /// </summary>
        private static Encoding utf8 = new UTF8Encoding();

        /// <summary>
        ///  Name
        /// </summary>
        private string MyName { get; set; }
        
        /// <summary>
        /// my public IPEndpoint that others will use to ping me
        /// </summary>
        private IPEndPoint MyPublicEndPoint;

        /// <summary>
        /// To keep track of active tasks.
        /// </summary>
        private readonly List<Task> activeTasks = new List<Task>();

        /// <summary>
        /// Record of active pings send to each remote machine.
        /// When we send a ping we'll add to this list
        /// When we recieve a reply we'll remove it from this list
        /// When we start a new cycle we'll handle older oustanding/late pings if any.
        /// </summary>
        private Dictionary<string, MessageInfo> activePings = new Dictionary<string, MessageInfo>();
        
        /// <summary>
        /// 
        /// </summary>
        private Socket incomingSocket;
        
        /// <summary>
        /// Map of names of others to thier IPEndPoints
        /// </summary>
        private Dictionary<string,IPEndPoint> groupMembers;

        /// <summary>
        /// Keep monitoring until active
        /// </summary>
        private bool isActive = false;

        /// <summary>
        /// counter for messages
        /// </summary>
        private static int uniqueMessageId;

        private SiloMetricsDataReporter siloMetricsDataReporter;
        
        /// <summary>
        /// </summary>
        /// <param name="name"></param>
        /// <param name="IPEndPoint"></param>
        /// <param name="groupMembers"></param>
        /// <param name="protocol"></param>
        public Agent(string name, IPEndPoint IPEndPoint, Dictionary<string, IPEndPoint> groupMembers, TimeSpan sleep, string connectionString, string deploymentId)
        {
            this.MyName = name;
            this.groupMembers = groupMembers;
            this.MyPublicEndPoint = IPEndPoint;
            this.SendInterval = sleep;
            if(connectionString!=null)
                siloMetricsDataReporter = new SiloMetricsDataReporter(connectionString, deploymentId);
        }
        
        /// <summary>
        /// Ping others in a loop
        /// </summary>
        public void PingOthers()
        {
            while (isActive)
            {
                // sleep 
                Log.Write(Log.Severity.Verbose, "Sleeping :{0}", MyName);
                Thread.Sleep(SendInterval);
                Log.Write(Log.Severity.Verbose, "Wakeup :{0}", MyName);

                activeTasks.Clear(); // remove references

                if (siloMetricsDataReporter != null)
                {
                    siloMetricsDataReporter.GetFromAzure(ref groupMembers);
                }
                if (!groupMembers.ContainsKey(MyName))
                {
                    Log.Write(Log.Severity.Info, "Skipping sending Ping as I am not yet part of the group");
                    continue;
                }
                foreach (string remoteName in groupMembers.Keys.Where(name => !name.Equals(MyName))) //others only
                {
                    try
                    {
                        Interlocked.Increment(ref uniqueMessageId);
                        string messageId = "msg_" + MyName + "_" + uniqueMessageId;

                        // At this point here should not be any active ping from earlier cycles
                        CheckUnansweredPing(remoteName);

                        // create a message
                        MessageInfo newPing = new MessageInfo();
                        newPing.MessageID = messageId;
                        newPing.Stage = "PING";
                        newPing.From = MyName;
                        newPing.SenderEndPoint = MyPublicEndPoint.ToString();
                        newPing.To = remoteName;
                        newPing.RecieverEndPoint = groupMembers[remoteName].ToString();
                        
                        // start a new message thread
                        lock (activePings)
                        {
                            activePings.Add(newPing.To, newPing);
                        }
                        
                        //send 
                        Send(newPing);
                    }
                    catch (Exception e)
                    {
                        Log.Write(Log.Severity.Error, "\t{0}", e);
                    }

                }
            }
        }

        private void CheckUnansweredPing(string remote)
        {
            MessageInfo lastUnansweredPing = null;
            lock (activePings)
            {
                if (activePings.ContainsKey(remote))
                {
                    // we'll come here only if we have started a handshake and it is still not complete.
                    lastUnansweredPing = activePings[remote];
                    activePings.Remove(remote);
                }
            }
            if (lastUnansweredPing != null)
            {
                string str = string.Format("\n\t{0}", lastUnansweredPing);
                DateTime lastContact = lastUnansweredPing.TimeStamp;
                Log.Write(Log.Severity.Warning, "MISSED PING:{0} Last contact {1} Time Elapsed {2}\n{3}", remote, lastContact.ToString("R"), (DateTime.UtcNow - lastContact), str);
            }
        }

        private void Send(MessageInfo message) 
        {
            if (!groupMembers.ContainsKey(MyName))
            {
                Log.Write(Log.Severity.Info, "Skipping as I am not yet part of the group");
                return;
            }
            if (!groupMembers.ContainsKey(message.To))
            {
                Log.Write(Log.Severity.Info, "Skipping as {0} not yet part of the group", message.To);
                return;
            }

            bool isConnected = false;
            Socket remote = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                remote.Connect(groupMembers[message.To]);
                isConnected = true;
                string msgStr = message.ToString();
                //Log.Write("          {0} {1}", remote.LocalEndPoint, remote.RemoteEndPoint);
                byte[] toSend = utf8.GetBytes(msgStr);
                remote.Send(toSend);
                Log.Write(Log.Severity.Info, "Sending : \t{0}->{1}\t{2}\t{3}", message.From, message.To, message.Stage, message.MessageID);
            }
            catch(Exception e)
            {
                Log.Write(Log.Severity.Error, "Error while sending to: {0}\n{1}", message.To, e);
            }
            finally
            {
                if (isConnected)
                {
                    try
                    {
                        remote.Disconnect(false);
                    }
                    catch (Exception)
                    {
                    }
                    try
                    {
                        remote.Close();
                    }
                    catch (Exception)
                    {
                    }
                }
            }
        }

        /// <summary>
        /// Starts listening
        /// </summary>
        public void Listen()
        {
            // initialize group before we start listening.
            if (siloMetricsDataReporter != null)
            {
                siloMetricsDataReporter.GetFromAzure(ref groupMembers);
            }
            incomingSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            incomingSocket.Bind(MyPublicEndPoint);
            isActive = true;

            incomingSocket.Listen(25);
            incomingSocket.BeginAccept(new AsyncCallback(AcceptConnection), incomingSocket);
        }

        /// <summary>
        /// Called asynchronously everytime there is connection
        /// </summary>
        /// <param name="request"></param>
        private void AcceptConnection(IAsyncResult request)
        {
            //Log.Write("Accept Connection Called");
            lock (lockObj)
            {
                Socket listener = (Socket)request.AsyncState;
                try
                {
                    // accept the connection
                    Socket remote = listener.EndAccept(request);

                    // Lauch reading on the task
                    Task connectionTask = Task.Factory.StartNew(() =>
                    {
                        // we recived a request , now respond to it.
                        Log.Write(Log.Severity.Verbose, "Respond Called");
                        Respond(remote);
                        Log.Write(Log.Severity.Verbose, "Respond ended");
                    });
                    activeTasks.Add(connectionTask);
                }
                catch (Exception e)
                {
                    Log.Write(Log.Severity.Error, "Error while accepting connection\n{0}", e);
                }
                finally
                {
                    // Dont forget to accept new 
                    listener.BeginAccept(new AsyncCallback(AcceptConnection), listener);
                }
            }
            Log.Write(Log.Severity.Verbose, "Accept Connection ended");
        }

        private void Respond(Socket remote)
        {
            try
            {
                if (!groupMembers.ContainsKey(MyName))
                {
                    Log.Write(Log.Severity.Info, "Skipping as not yet part of the group");
                }
                else
                {
                    // STEP 1:  Recieve message 
                    MessageInfo rcvd = Recieve(remote);
                    if(rcvd.Stage == "ACK")
                    {
                        lock (activePings)
                        {
                            MessageInfo original;
                            if (activePings.TryGetValue(rcvd.From, out original))
                            {
                                // Make sure you are not dealing with a late response.
                                if (original.MessageID == rcvd.MessageID)
                                {
                                    activePings.Remove(rcvd.From);
                                }
                            }
                        }
                    }
                    Log.Write(Log.Severity.Info, "Recieved \t{1}->{2}\t{3}\t{0}", rcvd.MessageID, rcvd.From, rcvd.To, rcvd.Stage);
                    if (rcvd.Stage == "PING")
                    {
                        // STEP 2:  ping back with reply;
                        MessageInfo ack = CreateReplyMessage(rcvd);
                        Send(ack);    
                    }
                }
            }
            catch (Exception e)
            {
                Log.Write(Log.Severity.Error, "-----\n\t{0}", e);
            }
            finally
            {
                try { remote.Close(); } catch (Exception e) { Log.Write(Log.Severity.Error, "-----\n\t{0}", e); }
            }
        }

        /// <summary>
        /// Helper for creating the right response.
        /// </summary>
        /// <param name="original"></param>
        /// <returns></returns>
        public MessageInfo CreateReplyMessage(MessageInfo original)
        {
            if (original.Stage != "PING")
                throw new ApplicationException("Invalid state: only 'PING' can be replied to");

            MessageInfo reply = new MessageInfo();
            reply.Stage = "ACK";
            reply.MessageID = original.MessageID;
            reply.From = original.To;
            reply.SenderEndPoint = original.RecieverEndPoint;
            reply.To = original.From;
            reply.RecieverEndPoint = original.SenderEndPoint;
            return reply;
        }

        /// <summary>
        /// Recieve data on given socket
        /// </summary>
        /// <param name="sender"></param>
        /// <returns></returns>
        private static MessageInfo Recieve(Socket sender)
        {
            string message;
            byte[] data = new byte[1024];
            int count = 0;
            if ((count = sender.Receive(data)) > 0)
            {
                var chars = utf8.GetChars(data, 0, count);
                message = new string(chars);
            }
            else
            {
                throw new Exception("Did not receive data");
            }
            // parse received data
            MessageInfo rcvd = MessageInfo.Parse(message);
            return rcvd;
        }

        public void Dispose()
        {
            isActive = false;
        }
    }
}
