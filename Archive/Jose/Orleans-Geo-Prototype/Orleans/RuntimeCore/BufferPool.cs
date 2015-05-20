using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Orleans.Counters;

namespace Orleans
{
    internal class BufferPool
    {
        private readonly int byteBufferSize;
        private readonly BlockingCollection<byte[]> buffers;
        private readonly CounterStatistic allocatedBufferCounter;
        private readonly CounterStatistic checkedOutBufferCounter;
        private readonly CounterStatistic checkedInBufferCounter;
        private readonly CounterStatistic droppedBufferCounter;

        public static BufferPool GlobalPool;

        public int Size
        {
            get { return byteBufferSize; }
        }

        public int Count
        {
            get { return buffers.Count; }
        }

        public string Name
        {
            get;
            private set;
        }

        internal static void InitGlobalBufferPool(IMessagingConfiguration config)
        {
            GlobalPool = new BufferPool(config.BufferPoolBufferSize, config.BufferPoolMaxSize, config.BufferPoolPreallocationSize, "Global");
        }

        /// <summary>
        /// Creates a buffer pool.
        /// </summary>
        /// <param name="bufferSize">The size, in bytes, of each buffer.</param>
        /// <param name="maxBuffers">The maximum number of buffers to keep around, unused; by default, the number of unused buffers is unbounded.</param>
        private BufferPool(int bufferSize, int maxBuffers, int preallocationSize, string name)
        {
            Name = name;
            byteBufferSize = bufferSize;
            if (maxBuffers <= 0)
            {
                buffers = new BlockingCollection<byte[]>();
            }
            else
            {
                buffers = new BlockingCollection<byte[]>(maxBuffers);
            }

            //string statisticNamePrefix = "Serialization." + Name + "BufferPool.";
            var globalPoolSizeStat = IntValueStatistic.FindOrCreate(StatNames.STAT_SERIALIZATION_BUFFERPOOL_BUFFERS_INPOOL,
                                                                    () => Count);
            allocatedBufferCounter = CounterStatistic.FindOrCreate(StatNames.STAT_SERIALIZATION_BUFFERPOOL_ALLOCATED_BUFFERS);
            checkedOutBufferCounter = CounterStatistic.FindOrCreate(StatNames.STAT_SERIALIZATION_BUFFERPOOL_CHECKED_OUT_BUFFERS);
            checkedInBufferCounter = CounterStatistic.FindOrCreate(StatNames.STAT_SERIALIZATION_BUFFERPOOL_CHECKED_IN_BUFFERS);
            droppedBufferCounter = CounterStatistic.FindOrCreate(StatNames.STAT_SERIALIZATION_BUFFERPOOL_CHECKED_IN_DROPPED_BUFFERS);

            // Those 2 counters should be equal. If not, it means we don't release all buffers.
            var checkedOutAndNotCheckedIn_BufferCounter = IntValueStatistic.FindOrCreate(StatNames.STAT_SERIALIZATION_BUFFERPOOL_INUSE_CHECKED_OUT_NOT_CHECKED_IN_BUFFERS,
                                                                    () => checkedOutBufferCounter.GetCurrentValue()
                                                                        - checkedInBufferCounter.GetCurrentValue()
                                                                        - droppedBufferCounter.GetCurrentValue());

            var allocatedAndNotInPool_BufferCounter = IntValueStatistic.FindOrCreate(StatNames.STAT_SERIALIZATION_BUFFERPOOL_INUSE_ALLOCATED_NOT_INPOOL_BUFFERS,
                                                                    () => allocatedBufferCounter.GetCurrentValue()
                                                                        - globalPoolSizeStat.GetCurrentValue()
                                                                        - droppedBufferCounter.GetCurrentValue());
            if(preallocationSize > 0)
            {
                var dummy = GetMultiBuffer(preallocationSize * Size);
                Release(dummy);
            }
        }

        public byte[] GetBuffer()
        {
            byte[] buffer;
            if (!buffers.TryTake(out buffer))
            {
                buffer = new byte[byteBufferSize];
                allocatedBufferCounter.Increment();
            }
            checkedOutBufferCounter.Increment();

            return buffer;
        }

        public List<ArraySegment<byte>> GetMultiBuffer(int totalSize)
        {
            var list = new List<ArraySegment<byte>>();
            while (totalSize > 0)
            {
                var buff = GetBuffer();
                list.Add(new ArraySegment<byte>(buff, 0, Math.Min(byteBufferSize, totalSize)));
                totalSize -= byteBufferSize;
            }
            return list;
        }

        public void Release(byte[] buffer)
        {
            if (buffer.Length == byteBufferSize)
            {
                if (buffers.TryAdd(buffer))
                {
                    checkedInBufferCounter.Increment();
                }
                else
                {
                    droppedBufferCounter.Increment();
                }
            }
            else
            {
                droppedBufferCounter.Increment();
            }
        }

        public void Release(List<ArraySegment<byte>> list)
        {
            if (list != null)
            {
                foreach (var segment in list)
                {
                    Release(segment.Array);
                }
            }
        }
    }
}