using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Orleans;

using SimpleGrain;

namespace UnitTestGrains
{
    //public class SimpleMainGrain : GrainBase, ISimpleMainGrain
    //{
    //    public Task Run()
    //    {
    //        var ref1 = SimpleGrainFactory.CreateGrain(A: 17);
    //        var ref2 = SimpleGrainFactory.CreateGrain(A: 22);
    //        GetLogger("SimpleMainGrain").Info("#1 should be 17: " + ref1.A.GetValue());
    //        GetLogger("SimpleMainGrain").Info("#2 should be 22: " + ref2.A.GetValue());

    //        return TaskDone.Done;
    //    }
    //}
}
