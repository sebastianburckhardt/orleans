using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Remoting.Messaging;


namespace Orleans
{
    /// <summary>
    /// This class holds information regarding the request currently being processed.
    /// It is explicitly intended to be available to application code.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The request context is represented as a property bag.
    /// Some values are provided by default; others are derived from messages headers in the
    /// request that led to the current processing.
    /// </para>
    /// <para>
    /// Information stored in RequestContext is propagated from 
    /// Orleans clients to Orleans grains automatically 
    /// by the Orleans runtime.
    /// RequestContext data is not automatically propagated across 
    /// TPL thread-switch boundaries -- <see cref="CallContext"/> 
    /// for that type of functionality.
    /// </para>
    /// </remarks>
    public static class RequestContext
    {
        /// <summary>
        /// Whether Trace.CorrelationManager.ActivityId settings should be propagated into grain calls.
        /// </summary>
        public static bool PropagateActivityId { get; set; }

        internal const string CallChainRequestContextHeader = "#RC_CCH";
        internal const string E2ETracing_ActivityId_Header = "#RC_AI";
        internal const string Orleans_RequestContext_Key = "#ORL_RC";

        /// <summary>
        /// Retrieve a value from the RequestContext key-value bag.
        /// </summary>
        /// <param name="key">The key for the value to be retrieved.</param>
        /// <returns>The value currently in the RequestContext for the specified key, 
        /// otherwise returns <c>null</c> if no data is present for that key.</returns>
        public static object Get(string key)
        {
            Dictionary<string, object> values = GetContextData();
            object result;
            if ((values != null) && values.TryGetValue(key, out result))
            {
                return result;
            }
            return null;
        }

        /// <summary>
        /// Sets a value into the RequestContext key-value bag.
        /// </summary>
        /// <param name="key">The key for the value to be updated / added.</param>
        /// <param name="value">The value to be stored into RequestContext.</param>
        public static void Set(string key, object value)
        {
            Dictionary<string, object> values = GetContextData();

            if (values == null)
            {
                values = new Dictionary<string, object>();
            }
            else
            {
                // Have to copy the actual Dictionary value, mutate it and set it back.
                // http://blog.stephencleary.com/2013/04/implicit-async-context-asynclocal.html
                // This is since LLC is only copy-on-write copied only upon LogicalSetData.
                values = new Dictionary<string, object>(values);
            }
            values[key] = value;
            SetContextData(values);
        }

        /// <summary>
        /// Remove a value from the RequestContext key-value bag.
        /// </summary>
        /// <param name="key">The key for the value to be removed.</param>
        /// <returns>Boolean <c>True</c> if the value was previously in the RequestContext key-value bag and has now been removed, otherwise returns <c>False</c>.</returns>
        public static bool Remove(string key)
        {
            Dictionary<string, object> values = GetContextData();

            if (values == null || values.Count == 0 || !values.ContainsKey(key))
            {
                return false;
            }
            Dictionary<string, object> newValues = new Dictionary<string, object>(values);
            bool retValue = newValues.Remove(key);
            SetContextData(newValues);
            return retValue;
        }

        internal static void ImportFromMessage(Message msg)
        {
            Dictionary<string, object> values = new Dictionary<string, object>();

            msg.GetApplicationHeaders(values);

            if (PropagateActivityId)
            {
                object activityIdObj;
                if (!values.TryGetValue(E2ETracing_ActivityId_Header, out activityIdObj))
                {
                    activityIdObj = Guid.Empty;
                }
                Trace.CorrelationManager.ActivityId = (Guid) activityIdObj;
            }
            if (values.Count > 0)
            {
                // We have some data, so store RC data into LogicalCallContext
                SetContextData(values);
            }
            else
            {
                // Clear any previous RC data from LogicalCallContext.
                // MUST CLEAR the LLC, so that previous request LLC does not leak into this one.
                Clear();
            }
        }

        internal static void ExportToMessage(Message msg)
        {
            Dictionary<string, object> values = GetContextData();

            if (PropagateActivityId)
            {
                Guid activityId = Trace.CorrelationManager.ActivityId;
                if (activityId != Guid.Empty)
                {
                    if (values == null)
                    {
                        values = new Dictionary<string, object>();
                    }
                    else
                    {
                        // Create new copy before mutating data
                        values = new Dictionary<string, object>(values);
                    }
                    values[E2ETracing_ActivityId_Header] = activityId;
                    // We have some changed data, so write RC data back into LogicalCallContext
                    SetContextData(values);
                }
            }
            if (values != null && values.Count != 0)
            {
                msg.SetApplicationHeaders(values);
            }
        }

        internal static void Clear()
        {
            // Remove the key to prevent passing of its value from this point on
            CallContext.FreeNamedDataSlot(Orleans_RequestContext_Key);
        }

        private static void SetContextData(Dictionary<string, object> values)
        {
            CallContext.LogicalSetData(Orleans_RequestContext_Key, values);
        }

        private static Dictionary<string, object> GetContextData()
        {
            return (Dictionary<string, object>) CallContext.LogicalGetData(Orleans_RequestContext_Key);
        }

    }
}
