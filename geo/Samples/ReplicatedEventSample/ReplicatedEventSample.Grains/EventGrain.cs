﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using ReplicatedEventSample.Interfaces;
using Orleans;
using Orleans.Providers.LogViews;
using Orleans.LogViews;
using Orleans.Concurrency;
using Orleans.Core;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.Providers;
using Orleans.QueuedGrains;
using Orleans.Runtime;

namespace ReplicatedEventSample.Grains
{
    [LogViewProvider(ProviderName = "CustomStorage")]
    public class EventGrain : 
        QueuedGrain<EventState, Outcome>, 
        IEventGrain,
        ICustomStorageInterface<EventState,Outcome>
    {

        public string EventName
        {
            get {
                return this.GetPrimaryKeyString();
            }
        }

        public async Task NewOutcome(Outcome outcome)
        {
            logger.Info("{3} new outcome {0} {1} {2}", outcome.When, outcome.Name, outcome.Score, EventName);

            EnqueueUpdate(outcome);

            // optional: wait for the updates to be acked from storage
            await this.ConfirmUpdates();
        }

        public Task<List<KeyValuePair<string, int>>> GetTopThree()
        {
            var result = ConfirmedState.outcomes
                .OrderByDescending(o => o.Value.Score)
                .Take(3)
                .Select(o => new KeyValuePair<string, int>(o.Value.Name, o.Value.Score))
                .ToList();
           
            return Task.FromResult(result);
        }

        protected override void ApplyDeltaToState(EventState state, Outcome delta)
        {
            state.Apply(delta);
        }

        public override async Task OnActivateAsync()
        {
            // get reference to ticker grain (there is just one per deployment, it has key 0)
            tickergrain = GrainFactory.GetGrain<ITickerGrain>(0);

            // we create a logger for each event grain
            logger = GetLogger();

            // initialize storage interface
            await InitStorageInterface();

            // read from storage NOW
            // (we want to ensure grain does not execute methods before reading from storage)
            await SynchronizeNowAsync();
        }

        Orleans.Runtime.Logger logger;


        bool results_have_started;
        string last_announced_leader;
        ITickerGrain tickergrain;


        // we override this virtual method to have an opportunity to react
        // to changes in the confirmed state
        protected override void OnConfirmedStateChanged()
        {
            string message = null;

            // notify on first results
            if (!results_have_started && ConfirmedState.outcomes.Count > 0)
            {
                message = string.Format("first results arriving for {0}", EventName);
                results_have_started = true;
            }

            // notify about leader after first 5 results are in
            if (ConfirmedState.outcomes.Count > 5)
            {
                var leader = ConfirmedState.outcomes.OrderByDescending(o => o.Value.Score).First().Value.Name;
                if (last_announced_leader == null)
                    message = string.Format("{0} is leading {1}", leader, EventName);
                else if (last_announced_leader != leader)
                    message = string.Format("{0} is now leading {1}", leader, EventName);
                last_announced_leader = leader;
            }

            if (message != null)
            {
                // send message as a background task
                var bgtask = tickergrain.SomethingHappened(message);
            }
        }



        #region storage interface

        // for now, we keep one ReplicatedEventTable object per grain
        // I think this is o.k. for Azure table storage
        // for other types of storage, it may make sense to do connection pooling
        private ReplicatedEventTable table;

        private async Task InitStorageInterface()
        {
            table = new ReplicatedEventTable();
            await table.Connect();
        }

        public async Task<KeyValuePair<int, EventState>> ReadStateFromStorageAsync()
        {
            var state = await table.ReadEventState(EventName);

            // in this simple sample, the version is always equal to the number of outcomes
            // usually, version number will probably need to be stored explicitly somewhere
            var version = state.outcomes.Count;

            return new KeyValuePair<int, EventState>(version, state);
        }

        public async Task<bool> ApplyUpdatesToStorageAsync(IReadOnlyList<Outcome> updates, int expectedversion)
        {
            try
            {
                await table.ApplyUpdatesToStorageAsync(EventName, expectedversion, updates);

                return true;
            }
            catch (Exception e)
            {
                // we will log this for diagnostic purposes
                logger.Info("Store Operation Failed: {0}", e);

                // there is no need to distinguish between exception types.
                return false;
            }
        }
        

 
        #endregion

    }
}
