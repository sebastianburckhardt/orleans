using Orleans;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace UnitTests.GrainInterfaces
{
    public interface ITransactionRunner : IGrainWithGuidKey
    {

        Task Run(string Id, string txName, ISchedulerGrain scheduler, ITransactionTestGrain[] grains);


        [Transaction(TransactionOption.RequiresNew)]
        Task Read_Add10(string Id, ISchedulerGrain scheduler, ITransactionTestGrain[] x);

        [Transaction(TransactionOption.RequiresNew)]
        Task Set10_Abort(string Id, ISchedulerGrain scheduler, ITransactionTestGrain[] x);




    }




    [Serializable]
    public class UserExplicitAbortException : OrleansException
    {
        public UserExplicitAbortException() : base("User code aborted transaction by throwing this exception.") { }

        public UserExplicitAbortException(string message) : base(message) { }

        public UserExplicitAbortException(string message, Exception innerException) : base(message, innerException) { }
#if !NETSTANDARD
        protected UserExplicitAbortException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
        }
#endif
    }
}

