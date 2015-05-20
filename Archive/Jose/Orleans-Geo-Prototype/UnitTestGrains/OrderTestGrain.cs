using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orleans;
using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    public interface IOrderTestGrainState : IGrainState
    {
        string Name { get; set; }
        Dictionary<string, HashSet<int>> History { get; set; }
    }

    public class OrderTestGrain : GrainBase<IOrderTestGrainState>, IOrderTestGrain
    {
        #region Implementation of IOrderTestGrain

        public Task Next(string client, int number, List<int> previous, int delay)
        {
            HashSet<int> set;
            if (! State.History.TryGetValue(client, out set))
            {
                set = new HashSet<int>();
                State.History[client] = set;
            }
            if (! set.IsSupersetOf(previous))
            {
                throw new ArgumentException("Previous for " + client + " is missing",
                    previous.Except(set).ToStrings(i => i.ToString()));
            }
            set.Add(number);
            Thread.Sleep(delay);
            return TaskDone.Done;
        }

        #endregion

        public Task<string> Name
        {
            get { return Task.FromResult(State.Name); }
        }
    }
}

