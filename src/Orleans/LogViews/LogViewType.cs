using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
   [Serializable]
    public class LogViewType<TLogEntry> : GrainState
    {
        public virtual void TransitionView(TLogEntry logentry)
        {
            try
            {
                dynamic me = this;
                me.Apply(logentry);
            }
            catch (MissingMethodException)
            {
                OnMissingStateTransition(logentry);
            }
        }

        protected virtual void OnMissingStateTransition(object @event)
        {
            // Log
        }
    }


  
   

 
}
