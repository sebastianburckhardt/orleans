using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Test.TransactionsTests;
using UnitTests.GrainInterfaces;
using Xunit;
using Xunit.Abstractions;

namespace Tester.TransactionsTests
{
    public abstract class MultipleTransactionsTestRunner
    {
        private readonly IGrainFactory grainFactory;
        private readonly ITestOutputHelper output;

        protected MultipleTransactionsTestRunner(IGrainFactory grainFactory, ITestOutputHelper output)
        {
            this.output = output;
            this.grainFactory = grainFactory;
        }
        
        [SkippableFact]
        public virtual async Task ReadTx_then_ReadTx()
        {
            ITransactionTestGrain grain = grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());

            Assert.Equal(0, await grain.Get());
            Assert.Equal(0, await grain.Get());
        }

        [SkippableFact]
        public virtual async Task ReadTx_then_WriteTx()
        {
            var grain = grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());

            Assert.Equal(0, await grain.Get());
            Assert.Equal(10, await grain.Add(10));
        }

        [SkippableFact]
        public virtual async Task WriteTx_then_ReadTx()
        {
            var grain = grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());

            Assert.Equal(10, await grain.Add(10));
            Assert.Equal(10, await grain.Get());
        }

        [SkippableFact]
        public virtual async Task WriteTx_then_WriteTx()
        {
            var grain = grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());

            Assert.Equal(10, await grain.Add(10));
            Assert.Equal(20, await grain.Add(10));
        }


        private async Task RunTwoTransactions(
            string idA, string chosenTransactionA, ITransactionTestGrain[] grainsA,
            string idB, string chosenTransactionB, ITransactionTestGrain[] grainsB,
            IEnumerable<SchedulerStep> schedule)
        {
            var scheduler = grainFactory.GetGrain<ISchedulerGrain>(Guid.NewGuid());
            var runner = grainFactory.GetGrain<ITransactionRunner>(Guid.NewGuid());

            await scheduler.Initialize(schedule);

            var transactionA = runner.Run(idA, chosenTransactionA, scheduler, grainsA);
            var transactionB = runner.Run(idB, chosenTransactionB, scheduler, grainsB);

            await scheduler.Finish();
            await transactionA;
            await transactionB;
        }


        [SkippableFact]
        public virtual Task TwoSingleObjectReadWriteTransactions_Separate()
        {
            var x = grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());

            return RunTwoTransactions(

                "A", "Read_Add10", new ITransactionTestGrain[] { x, x },
                "B", "Read_Add10", new ITransactionTestGrain[] { x, x },

                new SchedulerStep[]
                {
                    new SchedulerStep("A:TxStart"),
                    new SchedulerStep("A:Read"),
                    new SchedulerStep("A:=0"),
                    new SchedulerStep("A:Add10"),
                    new SchedulerStep("A:=10"),
                    new SchedulerStep("A:TxEnd"),
                    new SchedulerStep("A:Success"),
                            new SchedulerStep("B:TxStart"),
                            new SchedulerStep("B:Read"),
                            new SchedulerStep("B:=10"),
                            new SchedulerStep("B:Add10"),
                            new SchedulerStep("B:=20"),
                            new SchedulerStep("B:TxEnd"),
                            new SchedulerStep("B:Success"),
                }
            );
        }

        [SkippableFact]
        public virtual Task TwoSingleObjectReadWriteTransactions_StartBoth()
        {
            var x = grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());

            return RunTwoTransactions(

                "A", "Read_Add10", new ITransactionTestGrain[] { x, x },
                "B", "Read_Add10", new ITransactionTestGrain[] { x, x },


                new SchedulerStep[]
                {
                    new SchedulerStep("A:TxStart"),
                            new SchedulerStep("B:TxStart"),
                    new SchedulerStep("A:Read"),
                    new SchedulerStep("A:=0"),
                    new SchedulerStep("A:Add10"),
                    new SchedulerStep("A:=10"),
                    new SchedulerStep("A:TxEnd"),
                    new SchedulerStep("A:Success"),
                            new SchedulerStep("B:Read"),
                            new SchedulerStep("B:=10"),
                            new SchedulerStep("B:Add10"),
                            new SchedulerStep("B:=20"),
                            new SchedulerStep("B:TxEnd"),
                            new SchedulerStep("B:Success"),
            });
        }

        [SkippableFact]
        public virtual Task TwoSingleObjectReadWriteTransactions_WriteOverWrittenVersion()
        {
            var x = grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());

            return RunTwoTransactions(

                "A", "Read_Add10", new ITransactionTestGrain[] { x, x },
                "B", "Read_Add10", new ITransactionTestGrain[] { x, x },


                new SchedulerStep[]
                {
                    new SchedulerStep("A:TxStart"),
                    new SchedulerStep("A:Read"),
                    new SchedulerStep("A:=0"),
                            new SchedulerStep("B:TxStart"),
                            new SchedulerStep("B:Read"),
                            new SchedulerStep("B:=0"),
                            new SchedulerStep("B:Add10"),
                            new SchedulerStep("B:=10"),
                            new SchedulerStep("B:TxEnd"),
                            new SchedulerStep("B:Success"),
                    new SchedulerStep("A:Add10"),
                    new SchedulerStep("A:OrleansTransactionWaitDieException"),
            });
        }



        [SkippableFact]
        public virtual Task TwoSingleObjectReadWriteTransactions_WriteOverReadVersion()
        {
            var x = grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());

            return RunTwoTransactions(

                "A", "Read_Add10", new ITransactionTestGrain[] { x, x },
                "B", "Read_Add10", new ITransactionTestGrain[] { x, x },


                new SchedulerStep[]
                {
                    new SchedulerStep("A:TxStart"),
                            new SchedulerStep("B:TxStart"),
                            new SchedulerStep("B:Read"),
                            new SchedulerStep("B:=0"),
                    new SchedulerStep("A:Read"),
                    new SchedulerStep("A:=0"),
                    new SchedulerStep("A:Add10"),
                    new SchedulerStep("A:OrleansTransactionWaitDieException"),
                            new SchedulerStep("B:Add10"),
                            new SchedulerStep("B:=10"),
                            new SchedulerStep("B:TxEnd"),
                            new SchedulerStep("B:Success"),
            });
        }

        [SkippableFact]
        public virtual Task TwoSingleObjectReadWriteTransactions_ReadOverwrittenVersion()
        {
            var x = grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());

            return RunTwoTransactions(

                "A", "Read_Add10", new ITransactionTestGrain[] { x, x },
                "B", "Read_Add10", new ITransactionTestGrain[] { x, x },


                new SchedulerStep[]
                {
                    new SchedulerStep("A:TxStart"),
                            new SchedulerStep("B:TxStart"),
                            new SchedulerStep("B:Read"),
                            new SchedulerStep("B:=0"),
                            new SchedulerStep("B:Add10"),
                            new SchedulerStep("B:=10"),
                            new SchedulerStep("B:TxEnd"),
                            new SchedulerStep("B:Success"),
                    new SchedulerStep("A:Read"),
                    new SchedulerStep("A:OrleansTransactionVersionDeletedException"),
            });
        }


    }
}
