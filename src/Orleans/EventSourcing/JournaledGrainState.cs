using Orleans.LogViews;
using System;

namespace Orleans.EventSourcing
{
    public class JournaledGrainState : LogViewType<object>, IJournaledGrainState
    {
        public int Version { get; private set; }

        public virtual void TransitionState<TEvent>(TEvent @event)
        {
            try
            {
                dynamic me = this;
                me.Apply(@event);
                ++this.Version;
            }
            catch (MissingMethodException)
            {
                OnMissingStateTransition(@event);
            }
        }

        protected override void OnMissingStateTransition(object @event)
        {
            // Log
        }

        public override void TransitionView(object logentry)
        {
            this.TransitionState(logentry);
        }
    }
}
