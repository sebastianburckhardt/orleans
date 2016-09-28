﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    /// <summary>
    /// Log view storage interface. For grains whose state is defined as the view of a log.
    /// </summary>
    /// <typeparam name="TView">The type of the view (state of the grain)</typeparam>
    /// <typeparam name="TLogEntry">The type of log entries.</typeparam>
    public interface ILogViewStorageInterface<TView, TLogEntry>
        : ILogViewRead<TView,TLogEntry>, 
          ILogViewUpdate<TLogEntry>
    { }


    /// <summary>
    /// Interface for reading the log view.
    /// </summary>
    /// <typeparam name="TView">The type of the view (state of the grain).</typeparam>
    /// <typeparam name="TLogEntry">The type of log entries.</typeparam>
    public interface ILogViewRead<TView, TLogEntry> 
    {
        /// <summary>
        /// Local, tentative view of the log (reflecting both confirmed and unconfirmed entries)
        /// </summary>
        TView TentativeView { get; }

        /// <summary>
        /// Confirmed view of the log (reflecting only confirmed entries)
        /// </summary>
        TView ConfirmedView { get; }

        /// <summary>
        /// The length of the confirmed prefix of the log
        /// </summary>
        int ConfirmedVersion { get; }

        /// <summary>
        /// A list of the submitted entries that do not yet appear in the confirmed prefix.
        /// </summary>
        IEnumerable<TLogEntry> UnconfirmedSuffix { get; }

    }

    /// <summary>
    /// Interface for updating the log.
    /// </summary>
    /// <typeparam name="TLogEntry">The type of log entries.</typeparam>
    public interface ILogViewUpdate<TLogEntry>
    {
        /// <summary>
        /// Submit a single log entry to be appended to the global log,
        /// either at the current or at any later position.
        /// </summary>
        void Submit(TLogEntry entry);

        /// <summary>
        /// Submit a range of log entries to be appended atomically to the global log,
        /// either at the current or at any later position.
        /// </summary>
        void SubmitRange(IEnumerable<TLogEntry> entries);

        /// <summary>
        /// Try to append a single log entry at the current position of the log.
        /// </summary>
        /// <returns>true if the entry was appended successfully, or false 
        /// if there was a concurrency conflict (i.e. some other entries were previously appended).
        /// </returns>
        Task<bool> TryAppend(TLogEntry entry);

        /// <summary>
        /// Try to append a range of log entries atomically at the current position of the log.
        /// </summary>
        /// <returns>true if the entries were appended successfully, or false 
        /// if there was a concurrency conflict (i.e. some other entries were previously appended).
        /// </returns>
        Task<bool> TryAppendRange(IEnumerable<TLogEntry> entries);

        /// <summary>
        /// Confirm all submitted entries.
        ///<para>Waits until all previously submitted entries appear in the confirmed prefix of the log.</para>
        /// </summary>
        Task ConfirmSubmittedEntriesAsync();

        /// <summary>
        /// Confirm all submitted entries and get the latest log view.
        ///<para>Waits until all previously submitted entries appear in the confirmed prefix of the log, and forces a refresh of the confirmed prefix.</para>
        /// </summary>
        /// <returns></returns>
        Task SynchronizeNowAsync();
    }

}