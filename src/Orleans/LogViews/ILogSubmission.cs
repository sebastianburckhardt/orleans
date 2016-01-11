using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    public interface ILogSubmission<TLogEntry>
    {
        /// Submit an entry to be appended to the global log
        void Submit(TLogEntry entry);

        /// <summary>
        /// Confirm all submitted entries.
        ///<para>Waits until sumitted entries appear in the confirmed prefix of the log.</para>
        /// </summary>
        Task ConfirmSubmittedEntriesAsync();

        /// <summary>
        /// Confirm all submitted entries and get the latest log view.
        ///<para>Waits until sumitted entries appear in the confirmed prefix of the log, and forces a refresh of the confirmed prefix.</para>
        /// </summary>
        /// <returns></returns>
        Task SynchronizeNowAsync();

        /// <summary>
        /// A list of the submitted entries that do not yet appear in the confirmed prefix.
        /// </summary>
        IEnumerable<TLogEntry> UnconfirmedSuffix { get; }
    }
}
