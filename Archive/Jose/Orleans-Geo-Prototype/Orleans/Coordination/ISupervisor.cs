using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Runtime.Transactions;

namespace Orleans.Coordination
{
    /// <summary>
    /// For future use
    /// </summary>
    public interface ISupervisor : IGrain
    {
        /// <summary>
        /// invoked when a grain fails, returns advice for retry (advice might be "fail!")
        /// includes outer supervisor to fall back on
        /// </summary>
        /// <param name="type"></param>
        /// <param name="context"></param>
        /// <param name="reason"></param>
        /// <param name="outer"></param>
        /// <returns></returns>
        AsyncValue<Advice> OnError(Type type, Dictionary<string, object> context, Reason reason, ISupervisor outer);
    }

    /// <summary>
    /// 
    /// </summary>
    public class Supervision
    {
        /// <summary>
        /// supervisor to call on failure
        /// </summary>
        public ISupervisor Supervisor { get; set; }

        /// <summary>
        /// properties to capture on creation, and send to supervisor on failure
        /// </summary>
        public List<string> Capture { get; set; }

        /// <summary>
        /// constant context values to send to supervisor on failure
        /// </summary>
        public Dictionary<string, object> Context { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Supervision()
        {
            Capture = new List<string>();
            Context = new Dictionary<string, object>();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public static class SupervisionExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="supervisor"></param>
        /// <returns></returns>
        public static Supervision Use(this ISupervisor supervisor)
        {
            return new Supervision { Supervisor = supervisor };
        }

        // supervisor.Capture("Email", "Zip")
        /// <summary>
        /// 
        /// </summary>
        /// <param name="supervisor"></param>
        /// <param name="properties"></param>
        /// <returns></returns>
        public static Supervision Capture(this ISupervisor supervisor, params string[] properties)
        {
            return new Supervision { Supervisor = supervisor, Capture = properties.ToList() };
        }

        // supervisor.Context("Setup", 22).Capture("Email", "Zip")
        /// <summary>
        /// 
        /// </summary>
        /// <param name="prior"></param>
        /// <param name="properties"></param>
        /// <returns></returns>
        public static Supervision Capture(this Supervision prior, params string[] properties)
        {
            return new Supervision
            {
                Supervisor = prior.Supervisor,
                Capture = prior.Capture.Concat(properties).ToList(),
                Context = prior.Context
            };
        }

        // supervisor.Context("Setup", 22).Context("Language", "en-us")
        /// <summary>
        /// 
        /// </summary>
        /// <param name="supervisor"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Supervision Context(this ISupervisor supervisor, string key, object value)
        {
            return new Supervision
            {
                Supervisor = supervisor,
                Context = new Dictionary<string, object> { { key, value } }
            };
        }

        // supervisor.Capture("Bar", "Baz").Context("Setup", 22)
        /// <summary>
        /// 
        /// </summary>
        /// <param name="prior"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Supervision Context(this Supervision prior, string key, object value)
        {
            return new Supervision
            {
                Supervisor = prior.Supervisor,
                Capture = prior.Capture,
                Context = new Dictionary<string, object>(prior.Context) { { key, value } }
            };
        }
    }

}
