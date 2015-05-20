using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using TestGrainInterface;

namespace TestGrain
{
    public class TestGrain : Orleans.GrainBase, ITestGrainInterface
    {
        int counter = 0;

        public Task<int> SayHelloAsync()
        {
            counter += 1;
            return Task.FromResult(counter);
            /*
            if (name == "internal")
            {
                return reply;
            }
            else
            {
                // Talk to another grain on every odd numbered request received. Used to test internal silo-silo
                // communication.
                if (counter % 2 == 1)
                {
                    ITestGrainInterface grainRef = TestGrainInterfaceFactory.GetGrain(3000);
                    string reply2 = await grainRef.SayHelloAsync("internal");
                    reply += " " + reply2;
                }
                return reply;
            }
             * */
        }
    }
}
