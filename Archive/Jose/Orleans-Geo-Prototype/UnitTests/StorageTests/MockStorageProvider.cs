using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;

using Orleans.Storage;

namespace UnitTests.StorageTests
{
    public class MockStorageProvider : MarshalByRefObject, IStorageProvider
    {
        private int initCount, closeCount, readCount, writeCount, deleteCount;

        public int InitCount { get { return initCount; } }
        public int CloseCount { get { return closeCount; } }
        public int ReadCount { get { return readCount; } }
        public int WriteCount { get { return writeCount; } }
        public int DeleteCount { get { return deleteCount; } }

        private readonly Dictionary<Guid, Dictionary<string, object>> StateStore;

        public Guid LastId { get; private set; }
        public Dictionary<string, object> LastState
        {
            get { lock (StateStore) { return StateStore[LastId]; } }
        }

        public string Name { get; private set; }
        public OrleansLogger Log { get; protected set; }

        public MockStorageProvider()
        {
            StateStore = new Dictionary<Guid, Dictionary<string, object>>();
        }

        public virtual void SetValue(Guid id, string name, object val)
        {
            lock (StateStore)
            {
                Log.Info("Setting stored value {0} for {1} to {2}", name, id, val);
                Dictionary<string, object> storedState = GetLastState(id);
                storedState[name] = val;
                LastId = id;
            }
        }

        public Dictionary<string, object> GetLastState(Guid id)
        {
            lock (StateStore)
            {
                Dictionary<string, object> storedState;
                if (!StateStore.TryGetValue(id, out storedState))
                {
                    storedState = StateStore[id] = new Dictionary<string, object>();
                }
                LastId = id;
                return storedState;
            } 
        }

        public virtual Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            this.Name = name;
            Log = providerRuntime.GetLogger(this.GetType().FullName, Logger.LoggerType.Application);
            Log.Info(0, "Init");
            Interlocked.Increment(ref initCount);
            return TaskDone.Done;
        }

        public virtual Task Close()
        {
            Log.Info(0, "Close");
            Interlocked.Increment(ref closeCount);
            StateStore.Clear();
            return TaskDone.Done;
        }

        public virtual Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            Log.Info(0, "ReadStateAsync for {0} {1}", grainType, grainReference);
            Interlocked.Increment(ref readCount);
            lock (StateStore)
            {
                Guid id = grainReference.GrainId.GetPrimaryKey();
                Dictionary<string, object> storedState = GetLastState(id);
                grainState.SetAll(storedState); // Read current state data
            }
            return TaskDone.Done;
        }

        public virtual Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            Log.Info(0, "WriteStateAsync for {0} {1}", grainType, grainReference);
            Interlocked.Increment(ref writeCount);
            lock (StateStore)
            {
                Guid id = grainReference.GrainId.GetPrimaryKey();
                StateStore[id] = grainState.AsDictionary(); // Store current state data
                LastId = id;
            }
            return TaskDone.Done;
        }

        public virtual Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            Log.Info(0, "ClearStateAsync for {0} {1}", grainType, grainReference);
            Interlocked.Increment(ref deleteCount);
            lock (StateStore)
            {
                Guid id = grainReference.GrainId.GetPrimaryKey();
                StateStore[id].Clear();
            }
            return TaskDone.Done;
        }
    }

    [Serializable]
    public enum ErrorInjectionPoint
    {
        Unknown = 0,
        None = 1,
        BeforeRead = 2,
        AfterRead = 3,
        BeforeWrite = 4,
        AfterWrite = 5
    }

    [Serializable]
    public class StorageProviderInjectedError : Exception
    {
        private readonly ErrorInjectionPoint errorInjectionPoint;

        public StorageProviderInjectedError(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public StorageProviderInjectedError(ErrorInjectionPoint errorPoint)
        {
            errorInjectionPoint = errorPoint;
        }

        public StorageProviderInjectedError()
        {
            errorInjectionPoint = ErrorInjectionPoint.Unknown;
        }

        public override string Message
        {
            get
            {
                return "ErrorInjectionPoint=" + Enum.GetName(typeof(ErrorInjectionPoint), errorInjectionPoint);
            }
        }
    }

    public class ErrorInjectionStorageProvider : MockStorageProvider
    {
        public ErrorInjectionPoint ErrorInjection { get; private set; }

        internal static bool DoInjectErrors = true;

        public void SetErrorInjection(ErrorInjectionPoint errorInject)
        {
            ErrorInjection = errorInject;
            Log.Info(0, "Set ErrorInjection to {0}", ErrorInjection);
        }

        public async override Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            Log = providerRuntime.GetLogger(this.GetType().FullName, Logger.LoggerType.Application);
            Log.Info(0, "Init ErrorInjection={0}", ErrorInjection);
            try
            {
                SetErrorInjection(ErrorInjectionPoint.None);
                await base.Init(name, providerRuntime, config);
            }
            catch (Exception exc)
            {
                Log.Error(0, "Unexpected error during Init", exc);
                throw;
            }
        }

        public async override Task Close()
        {
            Log.Info(0, "Close ErrorInjection={0}", ErrorInjection);
            try
            {
                SetErrorInjection(ErrorInjectionPoint.None);
                await base.Close();
            }
            catch (Exception exc)
            {
                Log.Error(0, "Unexpected error during Close", exc);
                throw;
            }
        }

        public async override Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            Log.Info(0, "ReadStateAsync for {0} {1} ErrorInjection={2}", grainType, grainReference, ErrorInjection);
            try
            {
                if (ErrorInjection == ErrorInjectionPoint.BeforeRead && DoInjectErrors) throw new StorageProviderInjectedError(ErrorInjection);
                await base.ReadStateAsync(grainType, grainReference, grainState);
                if (ErrorInjection == ErrorInjectionPoint.AfterRead && DoInjectErrors) throw new StorageProviderInjectedError(ErrorInjection);
            }
            catch (Exception exc)
            {
                Log.Warn(0, "Injected error during ReadStateAsync for {0} {1} Exception = {2}", grainType, grainReference, exc);
                throw;
            }
        }

        public async override Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            Log.Info(0, "WriteStateAsync for {0} {1} ErrorInjection={0}", grainType, grainReference, ErrorInjection);
            try
            {
                if (ErrorInjection == ErrorInjectionPoint.BeforeWrite && DoInjectErrors) throw new StorageProviderInjectedError(ErrorInjection);
                await base.WriteStateAsync(grainType, grainReference, grainState);
                if (ErrorInjection == ErrorInjectionPoint.AfterWrite && DoInjectErrors) throw new StorageProviderInjectedError(ErrorInjection);
            }
            catch (Exception exc)
            {
                Log.Warn(0, "Injected error during WriteStateAsync for {0} {1} Exception = {2}", grainType, grainReference, exc);
                throw;
            }
        }
    }
}
