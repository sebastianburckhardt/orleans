using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;
using System.Collections;

namespace Orleans
{
    // AsyncMonitor allows to monitor async activities: subscribe to an event handler that will be called upon completion (success or failure) of every watched activity
    // One can add activitites to be watched or remove them from the monitor.
    // It is user's responsibility to synchronize and prevent potential data races to the data accessed from within the event handlers.

    internal class AsyncMonitor
    {
        // all activities are organized in turns, one turn for every invocation of AddActivities() method
        private Dictionary<int, List<AsyncCompletion>> completionTurns;        // Distionary<turnId, List<AsyncCompletion>>. I do not currently use the turnId, it is meant for future usages.
        private onCompletion                            _completionEvents;
        private int                                     numDelegates = 0;

        // event
        internal delegate void onCompletion(AsyncCompletion activity);
        public event onCompletion CompletionEvents
        {
            add 
            {
                lock (completionTurns)
                {
                    _completionEvents += value;
                    numDelegates++;
                }
            }
            remove 
            {
                lock (completionTurns)
                {
                    _completionEvents -= value;
                    numDelegates--;
                }
            }
        }
        

        public AsyncMonitor() 
        {
            completionTurns = new Dictionary<int, List<AsyncCompletion>>(); // TODO: what should be the initial capacity?
        }


        public void AddActivity(AsyncCompletion activity)
        {
            AsyncCompletion[] acs = new AsyncCompletion[1];
            acs[0] = activity;
            AddActivities(acs);
        }

        public void AddActivities(AsyncCompletion[] activities)
        {
            lock (completionTurns)
            {
                if (numDelegates <= 0) throw new ArgumentException("No delegates are defined for this monitor");

                if (activities == null) throw new ArgumentNullException();

                // create a new turn and add all activities there.
                List<AsyncCompletion> turn = new List<AsyncCompletion>(activities.Length);
                foreach (AsyncCompletion activity in activities)
                {
                    if (activity == null)
                    {
                        throw new ArgumentException("Asked to add a null reference.");
                    }

                    // check this AsyncCompletion has a unique Key.
                    bool found = FindAndRemove(activity, false);
                    if (found)
                    {
                        throw new ArgumentException("This AsyncCompletion already exists in this monitor.");
                    }
                    turn.Add(activity);
                }
                // add new turn to a set of all completion turns
                int turnIndex = completionTurns.Count;
                completionTurns.Add(turnIndex, turn);

                //WatchActivities(turn);
            }
        }

        //private void WatchActivities(List<AsyncCompletion> turn)
        //{
        //    AsyncCompletion waitPromise = AsyncCompletion.StartNew(() =>
        //    {
        //        bool lockTaken = false;
        //        try{
        //            lockTaken = false;
        //            Monitor.Enter(turn, ref lockTaken);
        //            Debug.Assert(lockTaken);
        //            // iteratively wait for Any completion to finish (allowing concurrent removals), until this turn is all done
        //            while (turn.Count > 0)
        //            {
        //                AsyncCompletion[] array = turn.ToArray();
        //                Monitor.Exit(turn);
        //                lockTaken = false;

        //                Debug.Assert(array.Length > 0); // took turn snapshot under lock.
        //                int index = AsyncCompletion.WaitAny(array);

        //                Monitor.Enter(turn, ref lockTaken);
        //                Debug.Assert(lockTaken);
        //                Debug.WriteLine("*WatchActivities: WaitAny of activity=" + array[index] + " ended with index=" + index + " with Status=" + array[index].Status);
        //                // do Not call turn.RemoveAt(index), since this activity (or other activity) might have been removed from the turn and indices changed.
        //                bool found = FindAndRemoveFromTurn(array[index], true, turn);
        //                if (found)
        //                {
        //                    // fire an event only if this activity was not removed from monitor by now.
        //                    _completionEvents(array[index]); // fire an event upon finishing this activity
        //                }
        //            }
        //            Monitor.Exit(turn);
        //            lockTaken = false;
        //        }finally 
        //        {
        //            if (lockTaken)
        //            {
        //                Monitor.Exit(turn);
        //            }
        //        }
        //    });
        //    waitPromise.Ignore();
        //    // I don't care about the waitPromise. I don't need to wait for it.
        //}


        public void RemoveActivity(AsyncCompletion activity)
        {
            if (activity == null)
            {
                throw new ArgumentException("Asked to remove a null reference");
            }

            lock (completionTurns)
            {
                bool found = FindAndRemove(activity, true);
                if (!found)
                {
                    throw new ArgumentException("This AsyncCompletion does NOT exists in this monitor.");
                }
            }
        }


        #region Private members

        private bool FindAndRemove(AsyncCompletion activity, bool remove)
        {
            if (activity==null)
            {
                throw new ArgumentException("Asked to FindAndRemove a null reference.");
            }

            //Dictionary<int, List<AsyncCompletion>>.ValueCollection.Enumerator enumerator = completionTurns.Values.GetEnumerator();
            IEnumerator enumerator = completionTurns.Values.GetEnumerator();
            enumerator.Reset();
            while (enumerator.MoveNext())
            {
                List<AsyncCompletion> turn = (List<AsyncCompletion>)enumerator.Current;
                bool found = FindAndRemoveFromTurn(activity, remove, turn);
                if (found) return true;
            }
            return false;
        }

        private static bool FindAndRemoveFromTurn(AsyncCompletion activity, bool remove, List<AsyncCompletion> turn)
        {
            for (int j = 0; j < turn.Count; j++)
            {
                if (turn[j] == activity) // just use references to compare
                {
                    if (remove)
                    {
                        turn.RemoveAt(j);
                    }
                    return true;
                }
            }
            return false;
        }

        #endregion
    }
}
