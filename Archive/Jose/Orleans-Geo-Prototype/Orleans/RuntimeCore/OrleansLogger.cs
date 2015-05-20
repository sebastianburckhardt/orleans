using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace Orleans
{
    /// <summary>
    /// Interface of Orleans RunTime for logging services. 
    /// </summary>
    [Serializable]
    public abstract class OrleansLogger
    {
        /// <summary> Severity levels for log messages. </summary>
        public enum Severity
        {
            Off = TraceLevel.Off,
            Error = TraceLevel.Error,
            Warning = TraceLevel.Warning,
            Info = TraceLevel.Info,
            Verbose = TraceLevel.Verbose,
            Verbose2 = TraceLevel.Verbose + 1,
            Verbose3 = TraceLevel.Verbose + 2
        }

        /// <summary> Current SeverityLevel set for this logger. </summary>
        public abstract Severity SeverityLevel
        {
            get;
        }

        /// <summary> Whether the current SeverityLevel would output <c>Info</c> messages for this logger. </summary>
        [DebuggerHidden]
        public bool IsInfo
        {
            get { return SeverityLevel >= Severity.Info; }
        }

        /// <summary> Whether the current SeverityLevel would output <c>Verbose</c> messages for this logger. </summary>
        [DebuggerHidden]
        public bool IsVerbose
        {
            get { return SeverityLevel >= Severity.Verbose; }
        }

        /// <summary> Whether the current SeverityLevel would output <c>Verbose2</c> messages for this logger. </summary>
        [DebuggerHidden]
        public bool IsVerbose2
        {
            get { return SeverityLevel >= Severity.Verbose2; }
        }

        /// <summary> Whether the current SeverityLevel would output <c>Verbose3</c> messages for this logger. </summary>
        [DebuggerHidden]
        public bool IsVerbose3
        {
            get { return SeverityLevel >= Severity.Verbose3; }
        }

        /// <summary> Output the specified message at <c>Verbose</c> log level. </summary>
        public abstract void Verbose(string format, params object[] args);

        /// <summary> Output the specified message at <c>Verbose2</c> log level. </summary>
        public abstract void Verbose2(string format, params object[] args);

        /// <summary> Output the specified message at <c>Verbose3</c> log level. </summary>
        public abstract void Verbose3(string format, params object[] args);

        /// <summary> Output the specified message at <c>Info</c> log level. </summary>
        ////[Obsolete("Use method Info(logCode,format,args) instead")]
        public abstract void Info(string format, params object[] args);

#region Public log methods using int LogCode categorization.
        /// <summary> Output the specified message and Exception at <c>Error</c> log level with the specified log id value. </summary>
        public abstract void Error(int logCode, string message, Exception exception = null);
        /// <summary> Output the specified message at <c>Warning</c> log level with the specified log id value. </summary>
        public abstract void Warn(int logCode, string format, params object[] args);
        /// <summary> Output the specified message and Exception at <c>Warning</c> log level with the specified log id value. </summary>
        public abstract void Warn(int logCode, string message, Exception exception);
        /// <summary> Output the specified message at <c>Info</c> log level with the specified log id value. </summary>
        public abstract void Info(int logCode, string format, params object[] args);
        /// <summary> Output the specified message at <c>Verbose</c> log level with the specified log id value. </summary>
        public abstract void Verbose(int logCode, string format, params object[] args);
        /// <summary> Output the specified message at <c>Verbose2</c> log level with the specified log id value. </summary>
        public abstract void Verbose2(int logCode, string format, params object[] args);
        /// <summary> Output the specified message at <c>Verbose3</c> log level with the specified log id value. </summary>
        public abstract void Verbose3(int logCode, string format, params object[] args);
#endregion
    }
}
