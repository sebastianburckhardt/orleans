using System;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Orleans
{
    /// <summary>
    /// </summary>
    internal static class IEnumerableExtensions
    {
        /// <summary>
        /// </summary>
        public static List<T> AsList<T>(this IEnumerable<T> sequence)
        {
            if (sequence == null)
                return null;
            if (sequence is List<T>)
                return sequence as List<T>;
            return sequence.ToList();
        }

        /// <summary>
        /// </summary>
        public static bool AreEqual<T>(this IEnumerable<T> first, IEnumerable<T> second, IEqualityComparer<T> comparer)
        {
            int count = first.Count();
            if (count != second.Count()) return false;
            return first.Intersect(second, comparer).Count() == count;
        }

        /// <summary>
        /// </summary>
        public static IEnumerable<List<T>> BatchIEnumerable<T>(this IEnumerable<T> sequence, int batchSize)
        {
            var batch = new List<T>(batchSize);
            foreach (var item in sequence)
            {
                batch.Add(item);
                // when we've accumulated enough in the batch, send it out  
                if (batch.Count >= batchSize)
                {
                    yield return batch; // batch.ToArray();
                    batch = new List<T>(batchSize);
                }
            }
            if (batch.Count > 0)
            {
                yield return batch; //batch.ToArray();
            }
        }
    }
}
