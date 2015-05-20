using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Orleans
{
    internal class OrleansRuntimeQueue<T> : IDisposable
    {
        private BlockingCollection<T> queue;
        private List<T> list;
        private object lockable;
        public bool IsAddingCompleted { get; private set; }

        public OrleansRuntimeQueue()
        {
            if (Constants.USE_BLOCKING_COLLECTION)
            {
                queue = new BlockingCollection<T>();
                IsAddingCompleted = false;
            }
            else
            {
                lockable = new object();
                list = new List<T>();
                IsAddingCompleted = false;
            }
        }

        public void Add(T item)
        {
            if (Constants.USE_BLOCKING_COLLECTION)
            {
                queue.Add(item);
            }
            else
            {
                if(IsAddingCompleted)
                    throw new InvalidOperationException("IsAddingCompleted.");
                bool lockTaken = false;
                try
                {
                    Monitor.Enter(lockable, ref lockTaken);
                    if (IsAddingCompleted)
                        throw new InvalidOperationException("IsAddingCompleted.");    
                    list.Add(item);
                    Monitor.PulseAll(lockable);
                }
                finally
                {
                    if (lockTaken)
                        Monitor.Exit(lockable);
                }
            }
        }

        public bool TryTake(out T item)
        {
            if (Constants.USE_BLOCKING_COLLECTION)
            {
                return queue.TryTake(out item);
            }
            else
            {
                bool lockTaken = false;
                try
                {
                    Monitor.Enter(lockable, ref lockTaken);
                    {
                        if (list.Count > 0)
                        {
                            item = list[0];
                            list.RemoveAt(0);
                            return true;
                        }
                        else
                        {
                            item = default(T);
                            return false;
                        }
                    }
                }
                finally
                {
                    if (lockTaken)
                        Monitor.Exit(lockable);
                }
            }
        }

        public T Take()
        {
            if (Constants.USE_BLOCKING_COLLECTION)
            {
                return queue.Take();
            }
            else
            {
                while (true)
                {
                    bool lockTaken = false;
                    try
                    {
                        Monitor.Enter(lockable, ref lockTaken);
                        {
                            if (list.Count > 0)
                            {
                                T item = list[0];
                                list.RemoveAt(0);
                                return item;
                            }
                            else if (IsAddingCompleted)
                            {
                                throw new InvalidOperationException("IsAddingCompleted and the queue is empty.");
                            }else
                            {
                                Monitor.Wait(lockable);
                                continue; // loop and try again.
                            }
                        }
                    }
                    finally
                    {
                        if (lockTaken)
                            Monitor.Exit(lockable);
                    }
                }
            }
        }

        public T First()
        {
            if (Constants.USE_BLOCKING_COLLECTION)
            {
                return queue.First();
            }
            else
            {
                while (true)
                {
                    bool lockTaken = false;
                    try
                    {
                        Monitor.Enter(lockable, ref lockTaken);
                        {
                            if (list.Count > 0)
                            {
                                T item = list[0];
                                return item;
                            }
                            else if (IsAddingCompleted)
                            {
                                throw new InvalidOperationException("IsAddingCompleted and the queue is empty.");
                            }
                            else
                            {
                                Monitor.Wait(lockable);
                                continue; // loop and try again.
                            }
                        }
                    }
                    finally
                    {
                        if (lockTaken)
                            Monitor.Exit(lockable);
                    }
                }
            }
        }

        public void CompleteAdding()
        {
            if (Constants.USE_BLOCKING_COLLECTION)
            { 
                queue.CompleteAdding();
            }
            else
            {
                lock (lockable)
                {
                    IsAddingCompleted = true;
                }
            }
        }

        public int Count
        {
            get
            {
                if (Constants.USE_BLOCKING_COLLECTION)
                {
                    return queue.Count;    
                }
                else
                {
                    lock (lockable)
                    {
                        return list.Count;
                    }
                }
            }
        }

        public void Dispose()
        {
            if (Constants.USE_BLOCKING_COLLECTION)
            {
                queue.Dispose();
            }
            else
            {
                lock (lockable)
                {
                    IsAddingCompleted = true;
                    list = null;
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;

            if (Constants.USE_BLOCKING_COLLECTION)
            {
                if (queue != null)
                {
                    queue.Dispose();
                    queue = null;
                }
            }
            else
            {
                lock (lockable)
                {
                    IsAddingCompleted = true;
                    list = null;
                }
            }
        }
    }
}
