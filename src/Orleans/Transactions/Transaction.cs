using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    internal class Transaction
    {
        public long TransactionId { get; set; }

        public TransactionState State { get; set; }

        // Sequence of the transaction in the log.
        // LSN is valid only if State is Committed.
        public long LSN { get; set; }

        // Time to abort the transaction if it was not completed.
        public long ExpirationTime { get; set; }

        public TransactionInfo Info { get; set; }

        // Transactions waiting on the result of this transactions.
        public HashSet<Transaction> WaitingTransactions { get; private set; }

        // Number of transactions this transaction is waiting for their outcome.
        public int PendingCount { get; set; }

        public TaskCompletionSource<bool> Completion { get; set; }

        public long HighestActiveTransactionIdAtCheckpoint { get; set; }

        public Transaction(long transactionId)
        {
            TransactionId = transactionId;
            WaitingTransactions = new HashSet<Transaction>();
            PendingCount = 0;
            LSN = 0;
            HighestActiveTransactionIdAtCheckpoint = 0;
            Completion = null; 
        }
    }
}
