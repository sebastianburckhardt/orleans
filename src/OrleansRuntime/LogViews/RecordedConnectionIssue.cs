﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.LogViews;

namespace Orleans.Runtime.LogViews
{
    /// <summary>
    /// Utility class for recording connection issues
    /// Is public because it is likely to be useful when implementing new log view providers
    /// </summary>
    public struct RecordedConnectionIssue
    {
        /// <summary>
        /// The recorded connection issue, or null if none
        /// </summary>
        public ConnectionIssue Issue { get; private set; }

        /// <summary>
        /// record a connection issue, filling in timestamps etc.
        /// and notify the listener
        /// </summary>
        /// <param name="newIssue">the connection issue to be recorded</param>
        /// <param name="listener">the listener for connection issues</param>
        /// <param name="services">for reporting exceptions in listener</param>
        public void Record(ConnectionIssue newIssue, IConnectionIssueListener listener, IProtocolServices services)
        {
            newIssue.TimeStamp = DateTime.UtcNow;
            if (Issue != null)
            {
                newIssue.TimeOfFirstFailure = Issue.TimeOfFirstFailure;
                newIssue.NumberOfConsecutiveFailures = Issue.NumberOfConsecutiveFailures + 1;
                newIssue.RetryDelay = newIssue.ComputeRetryDelay(Issue.RetryDelay);
            }
            else
            {
                newIssue.TimeOfFirstFailure = newIssue.TimeStamp;
                newIssue.NumberOfConsecutiveFailures = 1;
                newIssue.RetryDelay = newIssue.ComputeRetryDelay(null);
            }
            try
            {
                listener.OnConnectionIssue(newIssue);
            }
            catch (Exception e)
            {
                services.CaughtUserCodeException("OnConnectionIssue", nameof(Record), e);
            }
        }

        /// <summary>
        /// if there is a recorded issue, notify listener and clear it.
        /// </summary>
        /// <param name="listener">the listener for connection issues</param>
        /// <param name="services">for reporting exceptions in listener</param>
        public void Resolve(IConnectionIssueListener listener, IProtocolServices services)
        {
            if (Issue != null)
            {
                try
                {
                    listener.OnConnectionIssueResolved(Issue);
                }
                catch (Exception e)
                {
                    services.CaughtUserCodeException("OnConnectionIssueResolved", nameof(Record), e);
                }
                Issue = null;
            }
        }

        /// <summary>
        /// delays if there was an issue in last attempt, for the duration specified by the retry delay
        /// </summary>
        /// <returns></returns>
        public async Task DelayBeforeRetry()
        {
            if (Issue == null)
                return;

            await Task.Delay(Issue.RetryDelay);
        }

        public override string ToString()
        {
            if (Issue == null)
                return "";
            else
                return Issue.ToString();
        }
    }

}
