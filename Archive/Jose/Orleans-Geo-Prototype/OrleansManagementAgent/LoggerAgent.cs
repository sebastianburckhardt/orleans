using System;
using System.Text.RegularExpressions;
using ManagementFramework.Events;
using Orleans.Management.Events;
using Orleans.RuntimeCore;

namespace Orleans.Management.Agents
{
    public class LoggerAgent : OrleansManagementAgent
    {
        public LoggerAgent()
            : base("LoggerAgent")
        {
            AddSubscriptionType(typeof(SetLogLevelEvent), this.ProcessSetLogLevelEvent);
            AddSubscriptionType(typeof(SearchLogsRequestEvent), this.ProcessSearchLogsRequestEvent);
            AddPublishType(typeof(SearchLogsResponseEvent));
        }


        /// <summary>
        /// Processes a SetLogLevelEvent.
        /// </summary>
        /// <param name="eventType">We're expecting a SetLogLevelEvent.</param>
        /// <param name="ae">We're delivered a SetLogLevelEvent but we'll have to cast it from the AbstractEvent.</param>
        private void ProcessSetLogLevelEvent(Guid eventType, AbstractEvent ae)
        {
            SetLogLevelEvent req = ae as SetLogLevelEvent;

            if (req == null) return; // Ignore - not for us

            logger.Info("Received management event: EvtGuid={0} Event Contents={1}", eventType, ae.ToString());

            if (req.LogLevels == null)
            {
                SendReply(CreateCommandAcknowledgement(req, new ArgumentNullException("No LogLevels specified")));
            }

            foreach (string loggerName in req.LogLevels.Keys) {
                int newLogLevel = req.LogLevels[loggerName];

                if (loggerName == "SYS") {
                    logger.Info("Setting system log level to {0}", Enum.GetName(typeof(Logger.Severity), newLogLevel));
                    Logger.SetRuntimeLogLevel((Logger.Severity)newLogLevel);
                }
                else if (loggerName == "APP") {
                    logger.Info("Setting app log level to {0}", Enum.GetName(typeof(Logger.Severity), newLogLevel));
                    Logger.SetAppLogLevel((Logger.Severity)newLogLevel);
                }
                else if (loggerName == "ALL") {
                    logger.Info("Setting system log level to {0}", Enum.GetName(typeof(Logger.Severity), newLogLevel));
                    Logger.SetRuntimeLogLevel((Logger.Severity)newLogLevel);

                    logger.Info("Setting app log level to {0}", Enum.GetName(typeof(Logger.Severity), newLogLevel));
                    Logger.SetAppLogLevel((Logger.Severity)newLogLevel);
                }
                else {
                    Logger log = Logger.FindLogger(loggerName);
                    if (log != null) {
                        logger.Info("Setting logger '{0}' to log level {1}", loggerName, Enum.GetName(typeof(Logger.Severity), newLogLevel));
                        log.SetSeverityLevel((Logger.Severity)newLogLevel);
                    }
                    else {
                        logger.Warn("Warning: Cannot find logger '{0}' -- ignoring request to set log level to {1}", loggerName, Enum.GetName(typeof(Logger.Severity), newLogLevel));
                    }
                }
            }

            SendReply(CreateCommandAcknowledgement(req, null));
        }

        /// <summary>
        /// Processes a SearchLogsRequestEvent.
        /// </summary>
        /// <param name="eventType">We're expecting a SearchLogsRequestEvent.</param>
        /// <param name="ae">We're delivered a SearchLogsRequestEvent but we'll have to cast it from the AbstractEvent.</param>
        private void ProcessSearchLogsRequestEvent(Guid eventType, AbstractEvent ae)
        {
            SearchLogsRequestEvent req = ae as SearchLogsRequestEvent;

            if (req == null) return; // Ignore - not for us

            logger.Info("Received management event: EvtGuid={0} Event Contents={1}", eventType, ae.ToString());

            SearchLogsResponseEvent response = CreateSearchResponse(req);

            var results = DoSearch(req.LogName, req.SearchFrom, req.SearchTo, req.SearchPattern);
            response.LogEntries.AddRange(results);

            SendReply(response);
        }

        private SearchLogsResponseEvent CreateSearchResponse(SearchLogsRequestEvent req)
        {
            SearchLogsResponseEvent resp = new SearchLogsResponseEvent();
            InitializeManagementEvent(resp, req);
            // Copy search params from the request 
            resp.LogName = req.LogName;
            resp.SearchFrom = req.SearchFrom;
            resp.SearchTo = req.SearchTo;
            resp.SearchPattern = req.SearchPattern;
            resp.SiloName = req.SiloName;
            resp.Timestamp = DateTime.Now;
            return resp;
        }

        private string[] DoSearch(string logName, DateTime searchFrom, DateTime searchTo, Regex searchPattern)
        {
            return Logger.SearchLogFile(logName, searchFrom, searchTo, searchPattern);
        }
    }
}
