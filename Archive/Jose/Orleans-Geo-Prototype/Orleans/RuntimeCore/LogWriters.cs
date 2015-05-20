#define USE_STREAM_WRITER
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
#if !USE_STREAM_WRITER
using Microsoft.VisualBasic.Logging;
#endif


namespace Orleans
{
    /// <summary>
    /// The Log Writer base class provides default partial implementation suitable for most specific log writer.
    /// </summary>
    public abstract class LogWriterBase : ILogConsumer
    {
        /// <summary>
        /// The method to call during logging.
        /// This method should be very fast, since it is called synchronously during Orleans logging.
        /// </summary>
        /// <remarks>
        /// To customize functionality in a log writter derived from this base class, 
        /// you should override the <c>FormatLogMessage</c> and/or <c>WriteLogMessage</c> 
        /// methods rather than overriding this method directly.
        /// </remarks>
        /// <seealso cref="FormatLogMessage"/>
        /// <seealso cref="WriteLogMessage"/>
        /// <param name="severity">The severity of the message being traced.</param>
        /// <param name="loggerType">The type of logger the message is being traced through.</param>
        /// <param name="caller">The name of the logger tracing the message.</param>
        /// <param name="myIPEndPoint">The <see cref="IPEndPoint"/> of the Orleans client/server if known. May be null.</param>
        /// <param name="message">The message to log.</param>
        /// <param name="exception">The exception to log. May be null.</param>
        /// <param name="eventCode">Numeric event code for this log entry. May be zero, meaning 'Unspecified'.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void Log(            
            OrleansLogger.Severity severity,
            Logger.LoggerType loggerType,
            string caller,
            string message,
            IPEndPoint myIPEndPoint,
            Exception exception,
            int errorCode)    
        {
            DateTime now = DateTime.UtcNow;

            string msg = FormatLogMessage(
                now,
                severity,
                loggerType,
                caller,
                message,
                myIPEndPoint,
                exception,
                errorCode);

            try
            {
                WriteLogMessage(msg, severity);
            }
            catch (Exception exc)
            {
                Trace.TraceError("Error writing log message {0} -- Exception={1}", msg, exc);
            }
        }

        /// <summary>
        /// The method to call during logging to format the log info into a string ready for output.
        /// </summary>
        /// <param name="severity">The severity of the message being traced.</param>
        /// <param name="loggerType">The type of logger the message is being traced through.</param>
        /// <param name="caller">The name of the logger tracing the message.</param>
        /// <param name="myIPEndPoint">The <see cref="IPEndPoint"/> of the Orleans client/server if known. May be null.</param>
        /// <param name="message">The message to log.</param>
        /// <param name="exception">The exception to log. May be null.</param>
        /// <param name="eventCode">Numeric event code for this log entry. May be zero, meaning 'Unspecified'.</param>
        protected virtual string FormatLogMessage(
            DateTime timestamp,
            OrleansLogger.Severity severity,
            Logger.LoggerType loggerType,
            string caller,
            string message,
            IPEndPoint myIPEndPoint,
            Exception exception,
            int errorCode)
        {
            if (severity == OrleansLogger.Severity.Error)
                message = "!!!!!!!!!! " + message;

            string ip = myIPEndPoint == null ? String.Empty : myIPEndPoint.ToString();
            if (loggerType.Equals(Logger.LoggerType.Grain))
            {
                // GK: Grain identifies itself, so I don't want an additional long string in the prefix.
                // This is just a temporal solution to ease the dev. process, can remove later.
                ip = String.Empty;
            }

            string msg = String.Format("[{0} {1,5}\t{2}\t{3}\t{4}\t{5}]\t{6}\t{7}",
                Logger.ShowDate ? Logger.PrintDate(timestamp) : Logger.PrintTime(timestamp),            //0
                Thread.CurrentThread.ManagedThreadId,   //1
                Logger.SeverityTable[(int)severity],    //2
                errorCode,                              //3
                caller,                                 //4
                ip,                                     //5
                message,                                //6
                Logger.PrintException(exception));      //7

            return msg;
        }

        /// <summary>
        /// The method to call during logging to write the log message by this log.
        /// </summary>
        /// <param name="msg">Message string to be writter</param>
        /// <param name="severity">The severity level of this message</param>
        protected abstract void WriteLogMessage(string msg, OrleansLogger.Severity severity);
    }

    /// <summary>
    /// The Log Writer class is a convenient wrapper around the .Net Trace class.
    /// </summary>
    public class LogWriterToTrace : LogWriterBase, IFlushableLogConsumer
    {
        /// <summary>Write the log message for this log.</summary>
        protected override void WriteLogMessage(string msg, OrleansLogger.Severity severity)
        {
            switch (severity)
            {
                case OrleansLogger.Severity.Off:
                    break;
                case OrleansLogger.Severity.Error:
                    Trace.TraceError(msg);
                    break;
                case OrleansLogger.Severity.Warning:
                    Trace.TraceWarning(msg);
                    break;
                case OrleansLogger.Severity.Info:
                    Trace.TraceInformation(msg);
                    break;
                case OrleansLogger.Severity.Verbose:
                case OrleansLogger.Severity.Verbose2:
                case OrleansLogger.Severity.Verbose3:
                    Trace.WriteLine(msg);
                    break;
            }
            Flush();
        }

        /// <summary>Flush any pending output for this log.</summary>
        public void Flush()
        {
            Trace.Flush();
        }
    }

    /// <summary>
    /// The Log Writer class is a wrapper around the .Net Console class.
    /// </summary>
    public class LogWriterToConsole : LogWriterBase
    {
        private readonly bool useCompactConsoleOutput;

        private readonly string logFormat;

        /// <summary>
        /// Default constructor
        /// </summary>
        public LogWriterToConsole() : this(false, false)
        {
        }
        /// <summary>
        /// Constructor which allow some limited overides to the format of log message output,
        /// primarily intended for allow simpler Console screen output.
        /// </summary>
        /// <param name="useCompactConsoleOutput"></param>
        /// <param name="showMessageOnly"></param>
        internal LogWriterToConsole(bool useCompactConsoleOutput, bool showMessageOnly)
        {
            this.useCompactConsoleOutput = useCompactConsoleOutput;

            if (useCompactConsoleOutput)
            {
                // Log format items:
                // {0} = timestamp
                // {1} = severity
                // {2} = errorCode
                // {3} = caller
                // {4} = message
                // {5} = exception

                this.logFormat = showMessageOnly ? "{4}  {5}" : "{0} {1} {2} {3} - {4}\t{5}";
            }
        }

        /// <summary>Format the log message into the format used by this log.</summary>
        protected override string FormatLogMessage(
            DateTime timestamp,
            OrleansLogger.Severity severity,
            Logger.LoggerType loggerType,
            string caller,
            string message,
            IPEndPoint myIPEndPoint,
            Exception exception,
            int errorCode)
        {
            if (!useCompactConsoleOutput)
            {
                return base.FormatLogMessage(timestamp, severity, loggerType, caller, message, myIPEndPoint, exception, errorCode);
            }

            string msg = String.Format(logFormat,
                Logger.PrintTime(timestamp),            //0
                Logger.SeverityTable[(int)severity],    //1
                errorCode,                              //2
                caller,                                 //3
                message,                                //4
                Logger.PrintException(exception));      //5

            return msg;
        }

        /// <summary>Write the log message for this log.</summary>
        protected override void WriteLogMessage(string msg, OrleansLogger.Severity severity)
        {
            Console.WriteLine(msg);
        }
    }

    /// <summary>
    /// This Log Writer class is an Orleans Log Consumer wrapper class which writes to a specified log file.
    /// </summary>
    public class LogWriterToFile : LogWriterBase, IFlushableLogConsumer, ICloseableLogConsumer
    {
        private string logFileName;
        private readonly bool useFlush;
#if USE_STREAM_WRITER
        private StreamWriter logOutput;
#else
        private FileLogTraceListener logOutput;
#endif

        private readonly object lockObj = new Object();

        /// <summary>
        /// Constructor, specifying the file to send output to.
        /// </summary>
        /// <param name="logFile">The log file to be written to.</param>
        public LogWriterToFile(FileInfo logFile)
        {
            this.logFileName = logFile.FullName;

            bool fileExists = File.Exists(logFileName);
#if USE_STREAM_WRITER
            if (fileExists)
            {
                this.logOutput = logFile.AppendText();
            }
            else
            {
                this.logOutput = logFile.CreateText();
            }
#else
            this.logOutput = new FileLogTraceListener("Orleans-LogWriterToFile");
            logOutput.Location = LogFileLocation.Custom;
            logOutput.CustomLocation = logFile.Directory != null ? logFile.Directory.FullName : ".";
            //logOutput.BaseFileName = logFile.Name;
            logOutput.BaseFileName = Path.GetFileNameWithoutExtension(logFile.Name);
            logOutput.Append = fileExists;
            if (!fileExists)
            {
                // Force file creation
                using (var stream = logFile.CreateText())
                {
                    stream.Flush();
                }
            }
#endif
            this.useFlush = !logOutput.AutoFlush;
            logFile.Refresh(); // Refresh the cached view of FileInfo
        }

        /// <summary>Close this log file, after flushing any pending output.</summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void Close()
        {
            if (logOutput == null) // was already closed.
            {
                return;
            }
            try
            {
                lock (lockObj)
                {
                    if (logOutput == null) // was already closed.
                    {
                        return;
                    }
                    logOutput.Flush();
                    logOutput.Close();
                }
            }
            catch (Exception exc)
            {
                string msg = string.Format("Ignoring error closing log file {0} - {1}", logFileName, Logger.PrintException(exc));
                Console.WriteLine(msg);
            }
            this.logOutput = null;
            this.logFileName = null;
        }

        /// <summary>Write the log message for this log.</summary>
        protected override void WriteLogMessage(string msg, OrleansLogger.Severity severity)
        {
            lock (lockObj)
            {
                if (logOutput == null) return;
                logOutput.WriteLine(msg);
                if (useFlush)
                {
                    logOutput.Flush(); // We need to explicitly flush each log write
                }
            }
        }

        /// <summary>Flush any pending output for this log.</summary>
        public void Flush()
        {
            lock (lockObj)
            {
                if (logOutput == null) return;
                logOutput.Flush();
            }
        }
    }

    /// <summary>
    /// Just a simple log writer wrapper class with public WriteToLog method directly, without formatting.
    /// Mainly to be used from tests and external utilities.
    /// </summary>
    public class SimpleLogWriterToFile : LogWriterToFile
    {
        /// <summary>
        /// Constructor, specifying the file to send output to.
        /// </summary>
        /// <param name="logFile">The log file to be written to.</param>
        public SimpleLogWriterToFile(FileInfo logFile)
            : base(logFile)
        {
        }

        /// <summary>
        /// Output message directly to log file -- no formatting is performed.
        /// </summary>
        /// <param name="msg">Message text to be logged.</param>
        /// <param name="severity">Severity of this log message -- ignored.</param>
        public void WriteToLog(string msg, OrleansLogger.Severity severity)
        {
            WriteLogMessage(msg, severity);
        }
    }

    ///// <summary>
    ///// The Log Writer class is a convenient wrapper around the .Net ETW Trace class.
    ///// </summary>
    //public class LogWriterToEtw : LogWriterBase
    //{
    //    private static readonly Guid orleansEventProviderId = new Guid("8D3BABF3-9602-40BA-B527-01DD44A1274E");

    //    // Microsoft.ServiceModel.Samples.EtwTraceListener
    //    private readonly EventProvider etwProvider;
    //    private EventDescriptor etwEventDescriptor;
        
    //    public LogWriterToEtw()
    //    {
    //        // Construct event descriptor.
    //        this.etwEventDescriptor = new EventDescriptor(1, 1, 1, 1, 1, 1, 1);

    //        // Instantiate event provider.
    //        this.etwProvider = new EventProvider(orleansEventProviderId);
    //    }

    //    protected override void WriteLogMessage(string msg)
    //    {
    //        // Write an event to ETW
    //        etwProvider.WriteEvent(ref etwEventDescriptor, msg);
    //    }
    //}
}
