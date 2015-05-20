using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans;


using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    public interface IReliabilityTestGrainState : IGrainState
    {
        //[Queryable]
        string Label { get; set; }
    }

    public class ReliabilityTestGrain : GrainBase<IReliabilityTestGrainState>, IReliabilityTestGrain
    {
        private OrleansLogger logger;

        public override Task ActivateAsync()
        {
            logger = Logger.GetLogger("ReliabilityTestGrain", Logger.LoggerType.Application);
            logger.Info("Activated grain {0} on silo {1}", Identity, this.RuntimeIdentity);
            return TaskDone.Done;
        }

        #region Implementation of IReliabilityTestGrain

        Task<string> IReliabilityTestGrain.Label { get { return Task.FromResult(State.Label); } }

        public  Task<IReliabilityTestGrain> Other { get; set; }

        
        public Task SetLabels(string label, int delay)
        {
            logger.Info("{0}: changing label from {1} to {2}", Identity, State.Label, label);
            State.Label = label;
            Thread.Sleep(delay);
            return Other == null ? TaskDone.Done : Other.Result.SetLabels(label, delay);
        }

        public Task SetLabel(string label)
        {
            logger.Info("{0}: changing label from {1} to {2}", Identity, State.Label, label);
            State.Label = label;
            return TaskDone.Done;
        }

        #endregion
    }
}
