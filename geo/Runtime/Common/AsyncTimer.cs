using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeoOrleans.Runtime.Common
{

    public class AsyncTimer : BackgroundWorker
    {
        public AsyncTimer(int delay_ms, int period_ms, Func<Task<bool>> periodically)
        {
            this.taskfactory = async () =>
            {
                lock (this)
                    count++;

                bool keepgoing = await periodically();

                if (keepgoing)
                {
                    var t1 = Next(period_ms);
                }
            };

            var t2 = Next(delay_ms);
        }

        private int count;


        private async Task Next(int delay_msec)
        {
            int currentcount = count;

            await Task.Delay(delay_msec);

            lock (this)
            {
                if (currentcount == count) // there have not been more executions since this timer started
                    Notify();
            }
        }
    }



}
