using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;


namespace Orleans
{
#if !DISABLE_STREAMS 
    public interface IConsistentRingProviderForGrains
#else
    internal interface IConsistentRingProviderForGrains
#endif
    {
        /// <summary>
        /// Get the responsbility range of the current silo
        /// </summary>
        /// <returns></returns>
        IRingRange GetMyRange();

        /// <summary>
        /// Subscribe to receive range change notifications
        /// </summary>
        /// <param name="observer">An observer interface to receive range change notifications.</param>
        /// <returns>bool value indicating that subscription succeeded or not.</returns>
        bool SubscribeToRangeChangeEvents(IGrainRingRangeListener observer);

        /// <summary>
        /// Unsubscribe from receiving range change notifications
        /// </summary>
        /// <param name="observer">An observer interface to receive range change notifications.</param>
        /// <returns>bool value indicating that unsubscription succeeded or not</returns>
        bool UnSubscribeFromRangeChangeEvents(IGrainRingRangeListener observer);
    }

    // This has to be a separate interface, not polymorphic with IRingRangeListener,
    // since IRingRangeListener is implemented by SystemTarget and thus if it becomes grain interface 
    // it would need to be system target interface (with SiloAddress as first argument).
#if !DISABLE_STREAMS 
    public interface IGrainRingRangeListener : IGrain
#else
    internal interface IGrainRingRangeListener : IGrain
#endif
    {
        Task RangeChangeNotification(IRingRange old, IRingRange now, bool increased);
    }

#if !DISABLE_STREAMS 
    public interface IRingRange
#else
    internal interface IRingRange
#endif
    {
        /// <summary>
        /// Check if <paramref name="n"/> is our responsibility to serve
        /// </summary>
        /// <param name="id"></param>
        /// <returns>true if the reminder is in our responsibility range, false otherwise</returns>
        bool InRange(uint n);

        bool InRange(GrainReference grainReference);
    }

    [Serializable]
    internal class SingleRange : IRingRange
    {
        /// <summary>
        /// exclusive
        /// </summary>
        public uint Begin { get { return begin; } }

        /// <summary>
        /// inclusive
        /// </summary>
        public uint End { get { return end; } }

        private readonly uint begin;
        private readonly uint end;

        internal SingleRange(uint begin, uint end)
        {
            this.begin = begin;
            this.end = end;
        }

        public bool InRange(GrainReference grainReference)
        {
            return InRange(unchecked((uint)grainReference.GetUniformHashCode()));
        }

        /// <summary>
        /// checks if n is element of (Begin, End], while remembering that the ranges are on a ring
        /// </summary>
        /// <param name="n"></param>
        /// <returns>true if n is in (Begin, End], false otherwise</returns>
        public bool InRange(uint n)
        {
            uint num = n;
            if (begin < end)
            {
                if (num > begin && num <= end)
                {
                    return true;
                }
                return false;
            }
            // Begin > End
            if (num > begin || num <= end)
            {
                return true;
            }
            return false;
        }

        private uint RangeSize()
        {
            if (begin < end)
            {
                return end - begin;
            }
            else
            {
                return RangeFactory.RING_SIZE - (begin - end);
            }
        }

        private double RangePercentage()
        {
            return ((double)RangeSize() / (double)RangeFactory.RING_SIZE) * ((double)100.0);
        }

        public override string ToString()
        {
            if (begin == 0 && end == 0)
            {
                return String.Format("[(0 0], Total size=x{0,8:X8}, %Ring Space {1:0.000}%]", RangeSize(), RangePercentage());
            }
            return String.Format("[(x{0,8:X8} x{1,8:X8}], Total size=x{2,8:X8}, %Ring Space {3:0.000}%]", begin, end, RangeSize(), RangePercentage());
        }
    }

    internal static class RangeFactory
    {
        public static readonly uint RING_SIZE = uint.MaxValue;

        // has to be public for serializer, otherwise logically a private class.
        [Serializable]
        public class MultiRange : IRingRange
        {
            //private readonly List<SingleRange> ranges = new List<SingleRange>();
            //public ReadOnlyCollection<SingleRange> Ranges { get { return ranges.AsReadOnly(); } }
            //public IEnumerable<SingleRange> Ranges { get { return ranges; } }
            internal List<SingleRange> ranges { get; private set; }

            internal MultiRange(uint begin, uint end)
            {
                ranges = new List<SingleRange>(1);
                ranges.Add(new SingleRange(begin, end));
            }

            public override string ToString()
            {
                return Utils.IEnumerableToString(ranges);
            }

            /// <summary>
            /// Check if <paramref name="n"/> is our responsibility to serve
            /// </summary>
            /// <param name="id"></param>
            /// <returns>true if the reminder is in our responsibility range, false otherwise</returns>
            public bool InRange(uint n)
            {
                foreach (IRingRange s in ranges)
                {
                    if (s.InRange(n))
                    {
                        return true;
                    }
                }
                return false;
            }

            public bool InRange(GrainReference grainReference)
            {
                return InRange(unchecked((uint)grainReference.GetUniformHashCode()));
            }
        }

        public static IRingRange CreateFullRange()
        {
#if USE_MILTU_RANGE
            return new MultiRange(0, 0);
#else
            return new SingleRange(0, 0);
#endif
        }

        public static IRingRange CreateRange(uint begin, uint end)
        {
#if USE_MILTU_RANGE
            return new MultiRange(begin, end);
#else
            return new SingleRange(begin, end);
#endif
        }

        public static IEnumerable<SingleRange> GetSubRanges(IRingRange range)
        {
#if USE_MILTU_RANGE
            if (range is MultiRange)
            {
                return ((MultiRange)range).ranges;
            }
            else
            {
                return new SingleRange[] { (SingleRange)range };
            }
#else
            //return new SingleRange[] { (SingleRange)range };
            yield return (SingleRange)range;
#endif
        }
    }
}
