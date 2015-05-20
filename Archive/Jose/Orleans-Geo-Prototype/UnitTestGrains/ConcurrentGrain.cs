using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans;

using System;

namespace UnitTestGrains
{
    public class ConcurrentGrain : GrainBase, IConcurrentGrain
    {
        private OrleansLogger logger;
        private List<IConcurrentGrain> children;
        private int index;
        private int callNumber;

        public async Task Initialize(int ind)
        {
            index = ind;
            logger = base.GetLogger("ConcurrentGrain-" + index);
            logger.Info("Initialize(" + index + ")");
            if (index == 0)
            {
                children = new List<IConcurrentGrain>();
                for (int i = 0; i < 1; i++)
                {
                    IConcurrentGrain grain = ConcurrentGrainFactory.GetGrain((new Random()).Next());
                    await grain.Initialize(i + 1);
                    children.Add(grain);
                }
            }
        }

        public async Task<int> A()
        {
            callNumber++;
            int call = callNumber;
            logger.Info("A() start callNumber " + call);
            int i = 1;
            foreach (IConcurrentGrain child in children)
            {
                logger.Info("Calling B(" + i + "," + call + ")");
                int ret = await child.B(call);
                logger.Info("Resolved the calling B(" + i + "," + call + ")");
                i++;
            }
            logger.Info("A() END callNumber " + call);
            return 1;
        }

        public Task<int> B(int number)
        {
            logger.Info("B(" + index + ") call " + number);
            Thread.Sleep(100);
            logger.Info("B(" + index + ") call " + number + " after sleep");
            return Task.FromResult(1);
        }

        private List<int> m_list = new List<int>();

        public Task<List<int>> ModifyReturnList_Test()
        {
            return AsyncValue<List<int>>.StartNew(() =>
            {
                // just do a lot of modifications of the list
                for (int i = 0; i < 10; i++)
                {
                    if (m_list.Count < 1000)
                        m_list.Add(i);
                }
                for (int i = 0; i < 5; i++)
                {
                    m_list.RemoveAt(0);
                }
                return m_list;
            }).AsTask();
        }

        public Task Initialize_2(int ind)
        {
            index = ind;
            logger = base.GetLogger("ConcurrentGrain-" + index);
            logger.Info("Initialize(" + index + ")");
            return TaskDone.Done;
        }

        // start a long tail call on the 1st grain by calling into the 2nd grain 
        public Task<int> TailCall_Caller(IConcurrentReentrantGrain another, bool doCW)
        {
            logger.Info("TailCall_Caller");
            if (doCW)
            {
                return AsyncValue.FromTask(another.TailCall_Called()).ContinueWith((int i) => i).AsTask();
            }
            else
                return another.TailCall_Called();
        }


        // calls into the 1st grain while the tail call (TailCall_Caller) is not resolved yet.
        // if tail call optimization is working, this call should go in (the grain should be considered not executing request).
        public Task<int> TailCall_Resolver(IConcurrentReentrantGrain another)
        {
            logger.Info("TailCall_Resolver");
            return another.TailCall_Resolve();
        }

    }

    [Orleans.Reentrant]
    public class ConcurrentReentrantGrain : GrainBase, IConcurrentReentrantGrain
    {
        private OrleansLogger logger;
        private int index;
        private AsyncValueResolver<int> resolver;

        public Task Initialize_2(int ind)
        {
            index = ind;
            logger = base.GetLogger("ConcurrentReentrantGrain-" + index);
            logger.Info("Initialize(" + index + ")");
            return TaskDone.Done;
        }

        public Task<int> TailCall_Called()
        {
            logger.Info("TailCall_Called");
            resolver = new AsyncValueResolver<int>();
            return resolver.AsyncValue.AsTask();
        }

        public Task<int> TailCall_Resolve()
        {
            logger.Info("TailCall_Resolve");
            resolver.Resolve(7);
            return Task.FromResult(8);
        }
    }
}
