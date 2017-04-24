
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using UnitTests.GrainInterfaces;
using Orleans.Concurrency;
using Orleans.Transactions;
using Orleans.Runtime;

namespace UnitTests.Grains
{
    [Reentrant]
    public class TransactionTestSchedulerGrain : Grain, ITransactionRunner
    {
        public async Task Run(string id, string txName, ISchedulerGrain scheduler, ITransactionTestGrain[] grains)
        {
            await scheduler.Step(id, "TxStart");

            try
            {
                await Dispatch(txName, id, scheduler, grains);

                await scheduler.Step(id, "Success");
            }
            catch (Exception e)
            {
                await scheduler.Step(id, e.GetType().Name);
            }

            // final step where threads wait for end... not part of schedule
            await scheduler.Step(id, SchedulerStep.CompletionLabel);
        }

        private Task Dispatch(string txName, string id, ISchedulerGrain scheduler, ITransactionTestGrain[] grains)
        {
            switch (txName)
            {
                case nameof(Read_Add10):
                    return this.AsReference<ITransactionRunner>().Read_Add10(id, scheduler, grains);

                case nameof(Set10_Abort):
                    return this.AsReference<ITransactionRunner>().Set10_Abort(id, scheduler, grains);

                default:
                    throw new OrleansException($"no such transaction: {txName}");
            }
        }

        public async Task Read_Add10(string id, ISchedulerGrain scheduler, ITransactionTestGrain[] grains)
        {
            if (grains.Length != 2)
                throw new ArgumentException(nameof(grains), "invalid number of grains");

            await scheduler.Step(id, "Read");

            var r1 = await grains[0].Get();

            await scheduler.Step(id, "=" + r1);

            await scheduler.Step(id, "Add10");

            var r2 = await grains[1].Add(10);

            await scheduler.Step(id, "=" + r2);

            await scheduler.Step(id, "TxEnd");
        }

        public async Task Set10_Abort(string id, ISchedulerGrain scheduler, ITransactionTestGrain[] grains)
        {
            if (grains.Length != 1)
                throw new ArgumentException(nameof(grains), "invalid number of grains");

            await scheduler.Step(id, "Set10");

            await grains[0].Set(10);

            await scheduler.Step(id, "Abort");

            throw new UserExplicitAbortException();
        }
 
    }
}