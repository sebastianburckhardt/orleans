using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Echo;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    internal class SimpleActivateDeactivateTestGrain : GrainBase, ISimpleActivateDeactivateTestGrain
    {
        private readonly OrleansLogger logger;

        private IActivateDeactivateWatcherGrain watcher;

        private bool doingActivate;
        private bool doingDeactivate;

        public SimpleActivateDeactivateTestGrain()
        {
            this.logger = GetLogger();
        }

        public override async Task ActivateAsync()
        {
            logger.Info("ActivateAsync");
            this.watcher = ActivateDeactivateWatcherGrainFactory.GetGrain(0);
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Not doing Deactivate yet");
            doingActivate = true;
            await watcher.RecordActivateCall(this._Data.ActivationId);
            Assert.IsTrue(doingActivate, "Activate method still running");
            doingActivate = false;
        }

        public override async Task DeactivateAsync()
        {
            logger.Info("DeactivateAsync");
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Not doing Deactivate yet");
            doingDeactivate = true;
            await watcher.RecordDeactivateCall(this._Data.ActivationId);
            Assert.IsTrue(doingDeactivate, "Deactivate method still running");
            doingDeactivate = false;
        }

        public Task<ActivationId> DoSomething()
        {
            logger.Info("DoSomething");
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Deactivate method should not be running yet");
            return Task.FromResult(this._Data.ActivationId);
        }

        public Task DoDeactivate()
        {
            logger.Info("DoDeactivate");
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Deactivate method should not be running yet");
            base.DeactivateOnIdle();
            return TaskDone.Done;
        }
    }

    internal class TailCallActivateDeactivateTestGrain : GrainBase, ITailCallActivateDeactivateTestGrain
    {
        private readonly OrleansLogger logger;

        private IActivateDeactivateWatcherGrain watcher;

        private bool doingActivate;
        private bool doingDeactivate;

        public TailCallActivateDeactivateTestGrain()
        {
            this.logger = GetLogger();
        }

        public override Task ActivateAsync()
        {
            logger.Info("ActivateAsync");
            this.watcher = ActivateDeactivateWatcherGrainFactory.GetGrain(0);
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Not doing Deactivate yet");
            doingActivate = true;
            return watcher.RecordActivateCall(this._Data.ActivationId)
                .ContinueWith((Task t) =>
                {
                    Assert.IsFalse(t.IsFaulted, "RecordActivateCall failed");
                    Assert.IsTrue(doingActivate, "Doing Activate");
                    doingActivate = false;
                });
        }

        public override Task DeactivateAsync()
        {
            logger.Info("DeactivateAsync");
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Not doing Deactivate yet");
            doingDeactivate = true;
            return watcher.RecordDeactivateCall(this._Data.ActivationId)
                .ContinueWith((Task t) =>
                {
                    Assert.IsFalse(t.IsFaulted, "RecordDeactivateCall failed");
                    Assert.IsTrue(doingDeactivate, "Doing Deactivate");
                    doingDeactivate = false;
                });
        }

        public Task<ActivationId> DoSomething()
        {
            logger.Info("DoSomething");
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Deactivate method should not be running yet");
            return Task.FromResult(this._Data.ActivationId);
        }

        public Task DoDeactivate()
        {
            logger.Info("DoDeactivate");
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Deactivate method should not be running yet");
            base.DeactivateOnIdle();
            return TaskDone.Done;
        }
    }

    internal class LongRunningActivateDeactivateTestGrain : GrainBase, ILongRunningActivateDeactivateTestGrain
    {
        private readonly OrleansLogger logger;

        private IActivateDeactivateWatcherGrain watcher;

        private bool doingActivate;
        private bool doingDeactivate;

        public LongRunningActivateDeactivateTestGrain()
        {
            this.logger = GetLogger();
        }

        public override async Task ActivateAsync()
        {
            this.watcher = ActivateDeactivateWatcherGrainFactory.GetGrain(0);

            Assert.IsFalse(doingActivate, "Not doing Activate yet");
            Assert.IsFalse(doingDeactivate, "Not doing Deactivate yet");
            doingActivate = true;

            logger.Info("ActivateAsync");

            // Spawn Task to run on default .NET thread pool
            Task task = Task.Factory.StartNew(() =>
            {
                logger.Info("Started-ActivateAsync-SubTask");
                Assert.IsTrue(TaskScheduler.Current == TaskScheduler.Default, "Running under default .NET Task scheduler");
                Assert.IsTrue(doingActivate, "Still doing Activate in Sub-Task");
                logger.Info("Finished-ActivateAsync-SubTask");
            }, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
            await task;

            logger.Info("Started-ActivateAsync");

            await watcher.RecordActivateCall(this._Data.ActivationId);
            Assert.IsTrue(doingActivate, "Doing Activate");

            logger.Info("ActivateAsync-Sleep");
            Thread.Sleep(TimeSpan.FromSeconds(1));
            Assert.IsTrue(doingActivate, "Still doing Activate after Sleep");

            logger.Info("Finished-ActivateAsync");
            doingActivate = false;
        }

        public override async Task DeactivateAsync()
        {
            logger.Info("DeactivateAsync");

            Assert.IsFalse(doingActivate, "Not doing Activate yet");
            Assert.IsFalse(doingDeactivate, "Not doing Deactivate yet");
            doingDeactivate = true;

            logger.Info("Started-DeactivateAsync");

            await watcher.RecordDeactivateCall(this._Data.ActivationId);
            Assert.IsTrue(doingDeactivate, "Doing Deactivate");

            logger.Info("DeactivateAsync-Sleep");
            Thread.Sleep(TimeSpan.FromSeconds(1));
            logger.Info("Finished-DeactivateAsync");
            doingDeactivate = false;
        }

        public Task<ActivationId> DoSomething()
        {
            logger.Info("DoSomething");
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Deactivate method should not be running yet");
            return Task.FromResult(this._Data.ActivationId);
        }

        public Task DoDeactivate()
        {
            logger.Info("DoDeactivate");
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Deactivate method should not be running yet");
            base.DeactivateOnIdle();
            return TaskDone.Done;
        }
    }

    internal class TaskActionActivateDeactivateTestGrain : GrainBase, ITaskActionActivateDeactivateTestGrain
    {
        private readonly OrleansLogger logger;

        private IActivateDeactivateWatcherGrain watcher;

        private bool doingActivate;
        private bool doingDeactivate;

        public TaskActionActivateDeactivateTestGrain()
        {
            this.logger = GetLogger();
        }

        public override Task ActivateAsync()
        {
            var startMe = 
                new Task(
                    () =>
                    {
                        logger.Info("ActivateAsync");

                        this.watcher = ActivateDeactivateWatcherGrainFactory.GetGrain(0);

                        Assert.IsFalse(doingActivate, "Not doing Activate");
                        Assert.IsFalse(doingDeactivate, "Not doing Deactivate");
                        doingActivate = true;
                    });
            // we want to use Task.ContinueWith with an async lambda, an explicitly typed variable is required to avoid
            // writing code that doesn't do what i think it is doing.
            Func<Task> asyncCont =
                async () =>
                {
                    logger.Info("Started-ActivateAsync");

                    Assert.IsTrue(doingActivate, "Doing Activate");
                    Assert.IsFalse(doingDeactivate, "Not doing Deactivate");

                    try
                    {
                        logger.Info("Calling RecordActivateCall");
                        await watcher.RecordActivateCall(this._Data.ActivationId);
                        logger.Info("Returned from calling RecordActivateCall");
                    }
                    catch (Exception exc)
                    {
                        string msg = "RecordActivateCall failed with error " + exc;
                        logger.Error(0, msg);
                        Assert.Fail(msg);
                    }

                    Assert.IsTrue(doingActivate, "Doing Activate");
                    Assert.IsFalse(doingDeactivate, "Not doing Deactivate");

                    await Task.Delay(TimeSpan.FromSeconds(1));

                    doingActivate = false;

                    logger.Info("Finished-ActivateAsync");
                };
            var awaitMe = startMe.ContinueWith(_ => asyncCont()).Unwrap();
            startMe.Start();
            return awaitMe;
        }

        public override Task DeactivateAsync()
        {
            Task.Factory.StartNew(() => logger.Info("DeactivateAsync"));

            Assert.IsFalse(doingActivate, "Not doing Activate");
            Assert.IsFalse(doingDeactivate, "Not doing Deactivate");
            doingDeactivate = true;

            logger.Info("Started-DeactivateAsync");
            return watcher.RecordDeactivateCall(this._Data.ActivationId)
            .ContinueWith((Task t) =>
            {
                Assert.IsFalse(t.IsFaulted, "RecordDeactivateCall failed");
                Assert.IsTrue(doingDeactivate, "Doing Deactivate");
                Thread.Sleep(TimeSpan.FromSeconds(1));
                doingDeactivate = false;
            })
            .ContinueWith((Task t) => logger.Info("Finished-DeactivateAsync"), TaskContinuationOptions.ExecuteSynchronously);
        }

        public Task<ActivationId> DoSomething()
        {
            logger.Info("DoSomething");
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Deactivate method should not be running yet");
            return Task.FromResult(this._Data.ActivationId);
        }

        public Task DoDeactivate()
        {
            logger.Info("DoDeactivate");
            Assert.IsFalse(doingActivate, "Activate method should have finished");
            Assert.IsFalse(doingDeactivate, "Deactivate method should not be running yet");
            base.DeactivateOnIdle();
            return TaskDone.Done;
        }
    }

    public class BadActivateDeactivateTestGrain : GrainBase, IBadActivateDeactivateTestGrain
    {
        private readonly OrleansLogger logger;

        public BadActivateDeactivateTestGrain()
        {
            this.logger = GetLogger();
        }

        public override Task ActivateAsync()
        {
            logger.Info("ActivateAsync");
            throw new ApplicationException("Thrown from ActivateAsync");
        }

        public override Task DeactivateAsync()
        {
            logger.Info("DeactivateAsync");
            throw new ApplicationException("Thrown from DeactivateAsync");
        }

        public Task ThrowSomething()
        {
            logger.Info("ThrowSomething");
            throw new InvalidOperationException("Exception should have been thrown from Activate");
        }

        public Task<long> GetKey()
        {
            logger.Info("GetKey");
            //return this.GetPrimaryKeyLong();
            throw new InvalidOperationException("Exception should have been thrown from Activate");
        }
    }

    internal class BadConstructorTestGrain : GrainBase, IBadConstructorTestGrain
    {
        private readonly OrleansLogger logger;

        public BadConstructorTestGrain()
        {
            this.logger = GetLogger();
            throw new ApplicationException("Thrown from Constructor");
        }

        public override Task ActivateAsync()
        {
            logger.Info("ActivateAsync");
            throw new NotImplementedException("ActivateAsync should not have been called");
        }

        public override Task DeactivateAsync()
        {
            logger.Info("DeactivateAsync");
            throw new NotImplementedException("DeactivateAsync should not have been called");
        }

                public Task<ActivationId> DoSomething()
        {
            logger.Info("DoSomething");
            throw new NotImplementedException("DoSomething should not have been called");
        }
    }

    internal class ActivateDeactivateWatcherGrain : GrainBase, IActivateDeactivateWatcherGrain
    {
        private readonly OrleansLogger logger;

        private readonly List<ActivationId> activationCalls = new List<ActivationId>();
        private readonly List<ActivationId> deactivationCalls = new List<ActivationId>();

        public Task<ActivationId[]> ActivateCalls { get { return Task.FromResult(activationCalls.ToArray()); } }
        public Task<ActivationId[]> DeactivateCalls { get { return Task.FromResult(deactivationCalls.ToArray()); } }

        public ActivateDeactivateWatcherGrain()
        {
            this.logger = GetLogger();
        }

        public Task Clear()
        {
            logger.Info("Clear");
            activationCalls.Clear();
            deactivationCalls.Clear();
            return TaskDone.Done;
        }
        public Task RecordActivateCall(ActivationId activation)
        {
            logger.Info("RecordActivateCall");
            activationCalls.Add(activation);
            return TaskDone.Done;
        }

        public Task RecordDeactivateCall(ActivationId activation)
        {
            logger.Info("RecordDeactivateCall");
            deactivationCalls.Add(activation);
            return TaskDone.Done;
        }
    }

    internal class CreateGrainReferenceTestGrain : GrainBase, ICreateGrainReferenceTestGrain
    {
        private readonly OrleansLogger logger;

        //private IEchoGrain orleansManagedGrain;
        private ISimpleSelfManagedGrain selfManagedGrain;

        public CreateGrainReferenceTestGrain()
        {
            this.logger = GetLogger();
            selfManagedGrain = SimpleSelfManagedGrainFactory.GetGrain(1);
        }

        public override Task ActivateAsync()
        {
            logger.Info("ActivateAsync");
            selfManagedGrain = SimpleSelfManagedGrainFactory.GetGrain(1);
            return TaskDone.Done;
        }

        public async Task<ActivationId> DoSomething()
        {
            logger.Info("DoSomething");
            Guid guid = Guid.NewGuid();
            await selfManagedGrain.SetLabel(guid.ToString());
            var label = await selfManagedGrain.GetLabel();

            if (string.IsNullOrEmpty(label))
            {
                throw new ArgumentException("Bad data: Null label returned");
            }
            return this._Data.ActivationId;
        }
    }
}