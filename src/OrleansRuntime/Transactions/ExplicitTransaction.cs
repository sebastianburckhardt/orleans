using Orleans.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Transactions
{
    internal static class ExplicitTransaction 
    {

        public static async Task<T> RunTransaction<T>(ITransactionAgent agent, TransactionOptions options, Func<Task<T>> transaction)
        {
            var context = TransactionContext.GetTransactionInfo();
            bool isOutermostScope;

            if (context != null)
            {
                isOutermostScope = false;

                if (options != null && options.MustBeOutermostScope)
                    throw new InvalidOperationException($"Transaction may not be started inside existing transaction");
            }
            else
            {
                isOutermostScope = true;

                context = await agent.StartTransaction(options != null && options.ReadOnly, TimeSpan.FromSeconds(10));

                // set the context... flows into the execution of the transaction
                TransactionContext.SetTransactionInfo(context);
            }

            try
            {
                // execute the transaction.
                var result = await transaction();

                if (isOutermostScope)
                {
                    if (context.PendingCalls > 0)
                        throw new OrleansOrphanCallException(context.TransactionId, context.PendingCalls);

                    await agent.Commit(context);
                }

                return result;
            }
            catch(Exception e)
            {
                if (isOutermostScope)
                {
                    agent.Abort(TransactionContext.GetTransactionInfo());
                    TransactionContext.GetTransactionInfo().IsAborted = true;
                }

                throw e;
            }
        }
    }
}
