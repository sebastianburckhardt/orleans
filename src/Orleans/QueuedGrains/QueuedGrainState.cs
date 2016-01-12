using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.LogViews;

namespace Orleans.QueuedGrains
{
    /*
    [Serializable]
    public class QueuedGrainState<TGrainState> : LogViewType<IUpdateOperation<TGrainState>>
    {
        public override void TransitionView(IUpdateOperation<TGrainState> logentry)
        {
            // we need a ugly cast to let C# know what type we are
            var tthis = (TGrainState)(object)this;

            logentry.Update(tthis);
        }
    }
    */
}
