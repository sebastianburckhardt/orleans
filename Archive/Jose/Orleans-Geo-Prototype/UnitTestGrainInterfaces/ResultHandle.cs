using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Orleans;

namespace UnitTests
{
    // This class is used for testing.
    /// <summary>
    /// This class is for internal testing use only.
    /// </summary>
    public class ResultHandle : MarshalByRefObject
    {
        bool done = false;
        bool continueFlag = false;

        /// <summary>
        /// This method is for internal use only.
        /// </summary>
        public virtual void Reset()
        {
            Exception = null;
            Result = null;
            done = false;
            continueFlag = false;
        }

        /// <summary>
        /// This property is for internal use only.
        /// </summary>
        public bool Done
        {
            get { return done; }
            set { done = value; }
        }

        public bool Continue
        {
            get { return continueFlag; }
            set { continueFlag = value; }
        }

        public Exception Exception { get; set; }

        public object Result { get; set; }

        /// <summary>
        /// This method is for internal use only.
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns>Returns <c>true</c> if operation completes before timeout</returns>
        public bool WaitForFinished(TimeSpan timeout)
        {
            return WaitFor(timeout, ref done);
        }

        /// <summary>
        /// This method is for internal use only.
        /// </summary>
        /// <param name="timeoutMsec"></param>
        /// <returns>Returns <c>true</c> if operation completes before timeout</returns>
        public bool WaitForContinue(TimeSpan timeout)
        {
            return WaitFor(timeout, ref continueFlag);
        }

        /// <summary>
        /// This method is for internal use only.
        /// </summary>
        /// <param name="timeoutMsec"></param>
        /// <param name="flag"></param>
        /// <returns>Returns <c>true</c> if operation completes before timeout</returns>
        public bool WaitFor(TimeSpan timeout, ref bool flag)
        {
            int remaining = (int)timeout.TotalMilliseconds;
            while (!flag)
            {
                if (remaining < 0)
                {
                    //throw new TimeoutException("Timeout waiting for result for " + timeout);
                    return false;
                }

                Thread.Sleep(200);
                remaining -= 200;
            }

            return true;
        }
    }
}
