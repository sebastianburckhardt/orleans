// This file contains the source code given by Christian to use in correlation analysis.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CorrelationDetection
{

    //
    //  This encodes the different types of dependencies between counters we track
    //

    [Serializable]
    public enum DependencyType
    {

        Additive,           // Is used when a counter is the sum of a number of values, one of which is the dependent counter
        Default,            // Is used to encode all other types of dependency
        Linear,             // Same as 'Additive', with the difference that the additive dependency is (at most) an upper bound

    };

    //
    //  This encodes the different types of correlations between counters we track
    //

    [Serializable]
    public enum CorrelationType
    {

        Positive, Negative, Either

    };

    //
    //  Histogram-Explanations class that encodes the 
    //
    public class HistogramExplanation
    {
        public int NumberOfBuckets { get; private set; }            // The number of different bounaries the comain of the counter is partitioned in
        public List<int> Bucketboudaries { get; private set; }      // Records the offsets of the bucket boundaries
        public List<double> COI_Aggregate { get; private set; }     // Records the aggregate (sum/average) of the COI within each bucket
        public List<double> CE_Aggregate { get; private set; }      // Records the aggregate (sum/average) of the C_Explain within each bucket
        public List<List<object>> CE_Values { get; private set; }   // Contains the individual values for the CE in case it's categorical
        public double Score { get; private set; }                   // The score used for ranking
        public bool VarianceAnalysis { get; private set; }          // Does the order of the tuples reflect time or the size of the COI.                      
        public int NumberOfObservations { get; private set; }       // The total number of observations this histogram is built over
        public bool CE_is_Categorical { get; private set; }         // If this value is true, then the CE attribute is categorical and  
    
        public HistogramExplanation()
        {
            NumberOfBuckets = 0;
            Bucketboudaries = new List<int>();
            COI_Aggregate = new List<double>();
            CE_Aggregate = new List<double>();
            CE_Values = new List<List<object>>();
            Score = 0;
            CE_is_Categorical = false;
        }


        public void SetCategorical(bool IsCategorical)
        {
            CE_is_Categorical = IsCategorical;
        }


        public void SetAnalysisType(bool IsVariance)
        {
            VarianceAnalysis = IsVariance;
        }

        public void AddBucket(int Boundary, double Aggregate_COI, double Aggregate_CE)
        {
            NumberOfBuckets++;
            Bucketboudaries.Add(Boundary);
            COI_Aggregate.Add(Aggregate_COI);
            CE_Aggregate.Add(Aggregate_CE);
        }

        public void Add_CE_Values(List<object> Values)
        {
            CE_Values.Add(Values);
        }

        public void SetScore(double Scorein)
        {
            Score = Scorein;
        }

        public void SetNumberOfObservations(int NumObservations)
        {
            NumberOfObservations = NumObservations;
        }

        // Print function for debugging
        
        //
        //  If there is no warehouse defined, we simply print the counter IDs
        //
        public void Print()
        {
            System.Console.WriteLine("Histogram (MinVar) Explanation:");
            System.Console.WriteLine("------------------------------------------");
            System.Console.WriteLine("Score:                \t" + Score);
            if (VarianceAnalysis)
                System.Console.WriteLine("Variance-Explanation: Data re-ordereded by the value of the Counter-of-Interest");
            System.Console.WriteLine("------------------------------------------");
            for (int i = 0; i < NumberOfBuckets; i++)
            {
                System.Console.WriteLine("------------------------------------------");
                if (!CE_is_Categorical)
                    System.Console.WriteLine("Bucket " + i.ToString() + "[" + (Bucketboudaries[i].ToString()) + ".." + (i == NumberOfBuckets - 1 ? NumberOfObservations.ToString() : Bucketboudaries[i + 1].ToString()) + ")" + "\t COI Avg:" + COI_Aggregate[i].ToString() + "\t CE Avg:" + CE_Aggregate[i].ToString());
                else
                {
                    System.Console.Write("Bucket " + i.ToString() + "[" + (Bucketboudaries[i].ToString()) + ".." + (i == NumberOfBuckets - 1 ? NumberOfObservations.ToString() : Bucketboudaries[i + 1].ToString()) + ")" + "\t COI Avg:" + COI_Aggregate[i].ToString() + "\t CE Value: ");
                    foreach (object value in CE_Values[i])
                    {
                        System.Console.Write(value.ToString() + "  ");
                    }
                    System.Console.WriteLine();
                }
            }
            System.Console.WriteLine("------------------------------------------\n");
        }
    }

    public class StatisticalFunctions
    {
        //
        //  Compute Pearson's product-moment correlation coefficient 
        //  (see http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient for detailed explanation)
        //

        public double Pearsons(List<double> X, List<double> Y)
        {
            // Error-checking

            if (X.Count != Y.Count) // incorrect input
            {
                throw new System.ArgumentException("The length of the X and Y arrays is not identical", "X");
            }

            if (X.Count == 0) // incorrect input
            {
                throw new System.ArgumentException("Correlation computation for empty list ", "X");
            }

            // Compute the coefficient

            double SumX = 0;
            double SumY = 0;
            double SumXX = 0;
            double SumYY = 0;
            double SumXY = 0;

            for (int i = 0; i < X.Count; i++)
            {
                SumX += X[i];
                SumY += Y[i];
                SumXX += Math.Pow(X[i], 2);
                SumYY += Math.Pow(Y[i], 2);
                SumXY += X[i] * Y[i];
            }

            return ((X.Count * SumXY) - (SumX * SumY)) / (Math.Sqrt((Double)(X.Count * SumXX - SumX * SumX)) * Math.Sqrt((Double)(X.Count * SumYY - SumY * SumY)));

        }

        //
        //  Linear Regression function for explanation based on (linear) trends 
        //

        void LinearRegression(int n, double[] x, double[] y, ref double a, ref double b, ref double var)
        {
            double avgx = 0;
            double avgy = 0;
            for (int i = 0; i < n; i++)
            {
                avgx += x[i];
                avgy += y[i];
            }
            avgx /= n;
            avgy /= n;

            double Sxx = 0;
            double Sxy = 0;
            for (int i = 0; i < n; i++)
            {
                Sxx += (x[i] - avgx) * (x[i] - avgx);
                Sxy += (x[i] - avgx) * (y[i] - avgy);
            }

            b = Sxy / Sxx;
            a = avgy - b * avgx;
            var = 0;
            for (int i = 0; i < n; i++)
                var += (y[i] - a - b * x[i]) * (y[i] - a - b * x[i]);
        }


        //
        //  Compute Spearman's rank correlation coefficient 
        //  (see http://en.wikipedia.org/wiki/Spearman_rank_correlation for detailed explanation)
        //

        public struct ValueRankPair
        {
            public double X, Y;
            public double RankX, RankY;
        }

        public double Spearman(List<double> X, List<double> Y)
        {
            // Error-checking

            if (X.Count != Y.Count) // incorrect input
            {
                throw new System.ArgumentException("The length of the X and Y arrays is not identical", "X");
            }

            if (X.Count == 0) // incorrect input
            {
                throw new System.ArgumentException("Correlation computation for empty list ", "X");
            }

            List<ValueRankPair> RankedData = new List<ValueRankPair>(X.Count);
            for (int i = 0; i < X.Count; i++)
            {
                ValueRankPair temp = new ValueRankPair();
                temp.X = X[i];
                temp.Y = Y[i];
                RankedData.Add(temp);
            }

            RankedData.OrderBy(r => r.X);

            // Get the ranks of the X's right (accounting for ties)
            int start = -1;
            for (int i = 0; i < X.Count; i++)
            {
                ValueRankPair temp = RankedData[i];
                temp.RankX = i + 1;
                RankedData[i] = temp;

                // Cleaning up the average ranks in the presence of duplicates
                if (i > 0)
                {
                    if (RankedData[i].X == RankedData[i - 1].X)
                    {
                        if (start == -1) start = i - 1;
                    }
                    else
                    {
                        if (start != -1)
                        {
                            double RankAverage = (RankedData[i - 1].RankX + RankedData[start].RankX) / 2;
                            for (int j = start; j <= i - 1; j++)
                            {
                                ValueRankPair temp2 = RankedData[j];
                                temp2.RankX = RankAverage;
                                RankedData[j] = temp2;
                            }
                            start = -1;
                        }
                    }

                }
            }

            // Do the same thing for the Y-ranks

            RankedData.OrderBy(r => r.Y);

            start = -1;
            for (int i = 0; i < Y.Count; i++)
            {
                ValueRankPair temp = RankedData[i];
                temp.RankY = i + 1;
                RankedData[i] = temp;

                // Cleaning up the average ranks in the presence of duplicates
                if (i > 0)
                {
                    if (RankedData[i].Y == RankedData[i - 1].Y)
                    {
                        if (start == -1) start = i - 1;
                    }
                    else
                    {
                        if (start != -1)
                        {
                            double RankAverage = (RankedData[i - 1].RankY + RankedData[start].RankY) / 2;
                            for (int j = start; j <= i - 1; j++)
                            {
                                ValueRankPair temp2 = RankedData[j];
                                temp2.RankY = RankAverage;
                                RankedData[j] = temp2;
                            }
                            start = -1;
                        }
                    }

                }
            }

            List<double> X_temp = new List<double>();
            List<double> Y_temp = new List<double>();

            for (int i = 0; i < Y.Count; i++)
            {
                X_temp.Add(RankedData[i].RankX);
                Y_temp.Add(RankedData[i].RankY);
            }

            return Pearsons(X_temp, Y_temp);

        }

        // Helper-Structure for the aggregation
        struct ValueCount
        {
            public double Sum; public int Count;
        }


        //
        //  Functions that compute various types of Histogram Explanations
        //
        //  GenerateMinVarianceHistogram first does the necessary preparations, modifying the data depending on the underlying data types 
        //  and then computes the actual Histogram Explanation using GenerateMinVarianceHistogramNumeric (see below)
        //

        public HistogramExplanation GenerateMinVarianceHistogram(List<object> COI, List<object> C_Explain, int NumberOfBuckets, bool OrderByCOI, DependencyType Dependency, CorrelationType Correlation)
        {
            HashSet<int> ValidSplitPoints = new HashSet<int>(); // If this set is empty, all split points are valid

            if (COI.Count == 0 || C_Explain.Count == 0)
                throw new System.ArgumentException("Correlation computation for empty list ", "COI");

            if (!(COI[0] is double
                    || COI[0] is long
                    || COI[0] is ulong
                    || COI[0] is int
                    )
                )
                throw new System.ArgumentException("Counter of Interest must be numerical for this type of explanation", "COI");

            // First: the easy caseL COI and C_Explain are numeric
            if ((C_Explain[0] is double
                    || C_Explain[0] is long
                    || C_Explain[0] is ulong
                    || C_Explain[0] is int
                    )
                )
            {

                // Cast everything to double and continue

                List<double> COI_D = COI.ConvertAll(counter => System.Convert.ToDouble(counter));
                List<double> C_Explain_D = C_Explain.ConvertAll(counter => System.Convert.ToDouble(counter));

                return GenerateMinVarianceHistogramNumeric_fast(COI_D, C_Explain_D, NumberOfBuckets, OrderByCOI, ValidSplitPoints, Dependency, Correlation);
            }

            // Otherwise, we have to account for CE being categorical

            // The logic we follow now depends on whether we do a Variance Analysis or not

            if (OrderByCOI)
            {
                // Figure out the average COI for each category;


                Dictionary<object, ValueCount> Averages = new Dictionary<object, ValueCount>();

                if (COI.Count != C_Explain.Count)
                    throw new System.ArgumentException("Number of values in COI and CE do not match up");

                List<double> COI_Values = COI.Select(counter => System.Convert.ToDouble(counter)).ToList();


                for (int i = 0; i < COI.Count; i++)
                {
                    if (Averages.ContainsKey(C_Explain[i]))
                    {
                        ValueCount V = Averages[C_Explain[i]];
                        V.Count++; V.Sum += COI_Values[i];
                        Averages[C_Explain[i]] = V;
                    }
                    else
                    {
                        ValueCount V = new ValueCount();
                        V.Count = 1; V.Sum = COI_Values[i];
                        Averages[C_Explain[i]] = V;
                    }
                }

                double[] Avg_COI = new double[Averages.Count];
                object[] Keys = new object[Averages.Count];

                if (Averages.Count == 1) // There's only a single value, so no analysis can be done
                {
                    return null;

                }

                int c = 0;
                foreach (KeyValuePair<object, ValueCount> Entry in Averages)
                {
                    Avg_COI[c] = Entry.Value.Sum / Entry.Value.Count;
                    Keys[c] = Entry.Key;
                    c++;
                }

                Array.Sort(Avg_COI, Keys);

                List<double> ReOrderedCOI = new List<double>();
                List<object> ReOrderedCE = new List<object>();
                for (c = 0; c < Keys.Length; c++)
                {
                    for (int i = 0; i < COI.Count; i++)
                    {
                        if (Keys[c].Equals(C_Explain[i]))
                        {
                            ReOrderedCOI.Add(COI_Values[i]);
                            ReOrderedCE.Add(Keys[c]);
                        }
                    }
                    if (c != Keys.Length - 1)
                        ValidSplitPoints.Add(ReOrderedCOI.Count - 1);   // The end of the data is not a valid splitpoint
                }


                HistogramExplanation Explanation = GenerateMinVarianceHistogramNumeric_fast(ReOrderedCOI, ReOrderedCOI, NumberOfBuckets, OrderByCOI, ValidSplitPoints, Dependency, CorrelationType.Either);


                // Do clean-up and corrections

                Explanation.CE_Aggregate.Clear(); // These would be misleading

                int current_bucket = 0;
                while (current_bucket < Explanation.NumberOfBuckets)
                {
                    List<object> CE_Values = new List<object>();
                    int low = Explanation.Bucketboudaries[current_bucket];
                    int high = current_bucket + 1 < Explanation.NumberOfBuckets ? Explanation.Bucketboudaries[current_bucket + 1] : ReOrderedCE.Count;
                    CE_Values.Add(ReOrderedCE[low]);
                    for (int j = low + 1; j < high; j++)
                    {
                        if (CE_Values[CE_Values.Count - 1] != ReOrderedCE[j])   // Since the keys are consecutive
                            CE_Values.Add(ReOrderedCE[j]);
                    }
                    Explanation.Add_CE_Values(CE_Values);
                    current_bucket++;
                }

                Explanation.SetCategorical(true);

                return Explanation;
            }
            else
            {
                System.Diagnostics.Debug.WriteLine("Trend Analysis for categorical Explanations currently not implemented.");

                return null;
            }
        }

        //
        //  This function computes an explanation of the COI in terms of the changes in the 'counter' C_explain
        //  Both counters have to be numeric
        //  Basically, the algorithm computes a partitioning of the domain of the 'counter' C_explain that minimizes the (sum of the) underlying variances
        //  and then tests in how far this correlates with the changes in the COI
        //
        //HistogramExplanation GenerateMinVarianceHistogramNumeric(List<double> COI, List<double> C_Explain, int NumberOfBuckets, bool OrderByCOI, HashSet<int> ValidSplitPoints, DependencyType Dependency, CorrelationType Correlation)
        //{

        //    // Standard error-checking

        //    if (COI.Count != C_Explain.Count) // incorrect input
        //    {
        //        throw new System.ArgumentException("The length of the COI and C_Explain arrays is not identical", "COI");
        //    }

        //    if (COI.Count == 0) // incorrect input
        //    {
        //        throw new System.ArgumentException("Correlation computation for empty list ", "COI");
        //    }

        //    if (NumberOfBuckets < 2 || NumberOfBuckets >= 100) // incorrect input
        //    {
        //        throw new System.ArgumentException("Number of buckets out of bounds", "NumberOfBuckets");
        //    }

        //    // If there are only a limited number of valid split points, we can't force more buckets than these
        //    if (ValidSplitPoints.Count < NumberOfBuckets && ValidSplitPoints.Count > 0)
        //        NumberOfBuckets = ValidSplitPoints.Count + 1;

        //    double[] COI_Array = COI.ToArray();
        //    double[] C_Explain_Array = C_Explain.ToArray();

        //    if (OrderByCOI) // Reorder Everything
        //        Array.Sort(COI_Array, C_Explain_Array);

        //    //
        //    //  If there is only a single value in the c_explain or COI array, then we return the default histogram
        //    //

        //    bool identical_values = true;
        //    for (int i = 1; i < C_Explain_Array.Length; i++)
        //        if (C_Explain_Array[i] != C_Explain_Array[i - 1])
        //        {
        //            identical_values = false; break;
        //        }


        //    // Do the same test for COI
        //    if (!identical_values)
        //        identical_values = true;
        //    {

        //        for (int i = 1; i < C_Explain_Array.Length; i++)
        //            if (C_Explain_Array[i] != C_Explain_Array[i - 1])
        //            {
        //                identical_values = false; break;
        //            }
        //    }

        //    if (identical_values)
        //    {
        //        return null;
        //    }

        //    // Compute all relevant prefix sums

        //    List<double> SumX = new List<double>();
        //    List<double> SumY = new List<double>();
        //    List<double> SumXX = new List<double>();
        //    List<double> SumYY = new List<double>();

        //    double CumulativeSumX = 0;
        //    double CumulativeSumY = 0;
        //    double CumulativeSumXX = 0;
        //    double CumulativeSumYY = 0;

        //    Int32 count = C_Explain.Count;
        //    for (int i = 0; i < count; i++)
        //    {
        //        CumulativeSumX += C_Explain_Array[i];
        //        CumulativeSumY += COI_Array[i];
        //        CumulativeSumXX += C_Explain_Array[i] * C_Explain_Array[i];
        //        CumulativeSumYY += COI_Array[i] * COI_Array[i];
        //        SumX.Add(CumulativeSumX);
        //        SumY.Add(CumulativeSumY);
        //        SumXX.Add(CumulativeSumXX);
        //        SumYY.Add(CumulativeSumYY);

        //    }

        //    // Create the structures required for the dynamic programming

        //    int[,] OptSplit = new int[count, NumberOfBuckets];
        //    double[,] Variance = new double[count, NumberOfBuckets];

        //    // Initialize

        //    for (int endpoint = 0; endpoint < count; endpoint++)
        //    {
        //        OptSplit[endpoint, 0] = -1;   // Only one bucket, starting at the smallest index
        //        // All intervalls trivially start at 0.

        //        // Compute the  variance in [0,endpoint]
        //        double AverageX1 = (SumX[endpoint]) / (endpoint + 1);
        //        double AverageXX1 = (SumXX[endpoint]) / (endpoint + 1);
        //        Variance[endpoint, 0] = (AverageXX1 - Math.Pow(AverageX1, 2)) * (endpoint + 1);
        //    }

        //    // Now do the dynamic programming recursion for more than 1 bucket
        //    for (int buckets = 1; buckets < NumberOfBuckets; buckets++) // compute a multi_bucket solution
        //    {
        //        for (int endpoint = buckets /* Any single-value bucket has 0 variance */ ; endpoint < count; endpoint++)
        //        {
        //            double MinVariance = double.MaxValue;
        //            int MinSplit = -1;

        //            for (int split = buckets - 1 /* Any single-value bucket has 0 variance */ ; split < endpoint; split++)
        //            {
        //                // consider splitting the array [0,endpoint] into [0,split][split+1,endpoint]

        //                if (ValidSplitPoints.Count > 0 && !ValidSplitPoints.Contains(split))
        //                    continue;   // If this is not a valid split point, we're done here


        //                double Var1 = Variance[split, buckets - 1];

        //                double AverageX2 = (SumX[endpoint] - SumX[split]) / (endpoint - split);
        //                double AverageXX2 = (SumXX[endpoint] - SumXX[split]) / (endpoint - split);
        //                double Var2 = (AverageXX2 - Math.Pow(AverageX2, 2)) * (endpoint - split); ;

        //                double SplitVariance = Var1 + Var2;

        //                if (MinVariance > SplitVariance)
        //                {
        //                    MinSplit = split;
        //                    MinVariance = SplitVariance;
        //                }
        //            }

        //            // Assign the optimal split

        //            OptSplit[endpoint, buckets] = MinSplit;
        //            Variance[endpoint, buckets] = MinVariance;
        //        }

        //    }

        //    // Reconstruct the bucket boundaries

        //    List<int> BucketSplits = new List<int>();

        //    BucketSplits.Add(count - 1); // the end of the last bucket
        //    int BucketsRemaining = NumberOfBuckets - 1;

        //    while (BucketsRemaining > 0)
        //    {
        //        BucketSplits.Add(OptSplit[BucketSplits.Last(), BucketsRemaining]);
        //        BucketsRemaining--;
        //    }
        //    BucketSplits.Add(-1);

        //    double Explanation_Score = 0;

        //    HistogramExplanation Explanation = new HistogramExplanation();
        //    Explanation.SetAnalysisType(OrderByCOI);

        //    // Compute the Average COI (we need this to 'normalize' for some types of dependencies)

        //    double COI_average = 0;
        //    double CE_average = 0;
        //    for (int j = 0; j < COI_Array.Length; j++)
        //    {
        //        COI_average += COI_Array[j];
        //        CE_average += C_Explain_Array[j];
        //    }
        //    COI_average /= COI_Array.Length;
        //    CE_average /= COI_Array.Length;

        //    DateTime Start = DateTime.MinValue;
        //    DateTime End = DateTime.MaxValue;

        //    for (int i = BucketSplits.Count - 1; i > 0; i--)
        //    {
        //        // Generate the appropriate 'bucket' in the explanation

        //        double COI_aggregate = 0;
        //        double CE_aggregate = 0;
        //        for (int j = BucketSplits[i] + 1; j <= BucketSplits[i - 1]; j++)
        //        //for (int j = BucketSplits[i]; j < BucketSplits[i - 1]; j++)
        //        {
        //            COI_aggregate += COI_Array[j];
        //            CE_aggregate += C_Explain_Array[j];
        //        }
        //        COI_aggregate /= (BucketSplits[i - 1] - BucketSplits[i]);
        //        CE_aggregate /= (BucketSplits[i - 1] - BucketSplits[i]);

        //        // compute the score used in ranking

        //        if (Dependency == DependencyType.Default)
        //        {
        //            double Var = 0;
        //            for (int j = BucketSplits[i] + 1; j <= BucketSplits[i - 1]; j++)
        //                Var += Math.Pow(COI_aggregate - COI_Array[j], 2);

        //            Explanation_Score += Var;
        //        }
        //        else if (Dependency == DependencyType.Linear)
        //        {
        //            double Var = 0;
        //            double Basic_Var = 0;
        //            double Var_Adjusted = 0;
        //            for (int j = BucketSplits[i] + 1; j <= BucketSplits[i - 1]; j++)
        //            {
        //                Var += Math.Pow(COI_aggregate - COI_Array[j], 2);
        //                Basic_Var += Math.Pow(COI_average - COI_Array[j], 2);

        //                double deviation_from_average = CE_average - C_Explain_Array[j];
        //                Var_Adjusted += Math.Pow(((COI_average - COI_Array[j]) - deviation_from_average), 2);    // What's the largest reduction in Variance due to CE given that we know the dependency is at most additive ?           
        //            }

        //            if (Var < Var_Adjusted)
        //                Explanation_Score += Var_Adjusted;
        //            else Explanation_Score += Var;
        //        }

        //        Explanation.AddBucket(BucketSplits[i] + 1, COI_aggregate, CE_aggregate);
        //    }

        //    Explanation.SetNumberOfObservations(COI_Array.Length);
        //    Explanation.SetScore(Explanation_Score);

        //    //
        //    //  Now test if the correlation works out...
        //    //

        //    bool Obeys_Correlation_Constraint = true;
        //    bool small_differences = false;

        //    for (int l = 1; l < Explanation.CE_Aggregate.Count; l++)
        //    {
        //        if (Math.Abs(Explanation.CE_Aggregate[l - 1] - Explanation.CE_Aggregate[l]) / CE_average < 0.000001)    // test if there is a noticeable difference
        //            small_differences = true;
        //    }

        //    if (Correlation != CorrelationType.Either)
        //    {
        //        double[] COI_Bucket_Array = Explanation.COI_Aggregate.ToArray();
        //        double[] CE_Bucket_Array = Explanation.CE_Aggregate.ToArray();

        //        Array.Sort(COI_Bucket_Array, CE_Bucket_Array);
        //        if (Correlation == CorrelationType.Positive)
        //        {
        //            for (int l = 1; l < COI_Bucket_Array.Length; l++)
        //            {
        //                if (CE_Bucket_Array[l - 1] > CE_Bucket_Array[l])
        //                    Obeys_Correlation_Constraint = false;
        //            }
        //        }
        //        if (Correlation == CorrelationType.Negative)
        //        {
        //            for (int l = 1; l < COI_Bucket_Array.Length; l++)
        //                if (CE_Bucket_Array[l - 1] < CE_Bucket_Array[l])
        //                    Obeys_Correlation_Constraint = false;
        //        }
        //    }

        //    if (Obeys_Correlation_Constraint == false || small_differences)
        //    {
        //        return null;
        //    }

        //    return Explanation;
        //}


        //
        //  This function computes an explanation of the COI in terms of the changes in the 'counter' C_explain
        //  Both counters have to be numeric
        //  Basically, the algorithm computes a partitioning of the domain of the 'counter' C_explain that minimizes the (sum of the) underlying variances
        //  and then tests in how far this correlates with the changes in the COI
        //
        HistogramExplanation GenerateMinVarianceHistogramNumeric_fast(List<double> COI, List<double> C_Explain, int NumberOfBuckets, bool OrderByCOI, HashSet<int> ValidSplitPoints, DependencyType Dependency, CorrelationType Correlation)
        {

            // Standard error-checking

            if (COI.Count != C_Explain.Count) // incorrect input
            {
                throw new System.ArgumentException("The length of the COI and C_Explain arrays is not identical", "COI");
            }

            if (COI.Count == 0) // incorrect input
            {
                throw new System.ArgumentException("Correlation computation for empty list ", "COI");
            }

            if (NumberOfBuckets < 2 || NumberOfBuckets >= 100) // incorrect input
            {
                throw new System.ArgumentException("Number of buckets out of bounds", "NumberOfBuckets");
            }

            // If there are only a limited number of valid split points, we can't force more buckets than these
            if (ValidSplitPoints.Count < NumberOfBuckets && ValidSplitPoints.Count > 0)
                NumberOfBuckets = ValidSplitPoints.Count + 1;

            double[] COI_Array = COI.ToArray();
            double[] C_Explain_Array = C_Explain.ToArray();

            if (OrderByCOI) // Reorder Everything
                Array.Sort(COI_Array, C_Explain_Array);

            //
            //  If there is only a single value in the c_explain or COI array, then we return the default histogram
            //

            bool identical_values = true;
            for (int i = 1; i < C_Explain_Array.Length; i++)
                if (C_Explain_Array[i] != C_Explain_Array[i - 1])
                {
                    identical_values = false; break;
                }


            // Do the same test for COI
            if (!identical_values)
                identical_values = true;
            {

                for (int i = 1; i < C_Explain_Array.Length; i++)
                    if (C_Explain_Array[i] != C_Explain_Array[i - 1])
                    {
                        identical_values = false; break;
                    }
            }

            if (identical_values)
            {
                return null;
            }

            // Compute all relevant prefix sums

            List<double> SumX = new List<double>();
            List<double> SumY = new List<double>();
            List<double> SumXX = new List<double>();
            List<double> SumYY = new List<double>();

            double CumulativeSumX = 0;
            double CumulativeSumY = 0;
            double CumulativeSumXX = 0;
            double CumulativeSumYY = 0;

            Int32 count = C_Explain.Count;
            for (int i = 0; i < count; i++)
            {
                CumulativeSumX += C_Explain_Array[i];
                CumulativeSumY += COI_Array[i];
                CumulativeSumXX += C_Explain_Array[i] * C_Explain_Array[i];
                CumulativeSumYY += COI_Array[i] * COI_Array[i];
                SumX.Add(CumulativeSumX);
                SumY.Add(CumulativeSumY);
                SumXX.Add(CumulativeSumXX);
                SumYY.Add(CumulativeSumYY);

            }

            // Create the structures required for the dynamic programming

            int[,] OptSplit = new int[count, NumberOfBuckets];
            double[,] Variance = new double[count, NumberOfBuckets];

            // Initialize

         
            for (int endpoint = 0; endpoint < count; endpoint++)
            {
                OptSplit[endpoint, 0] = -1;   // Only one bucket, starting at the smallest index
                // All intervalls trivially start at 0.

                // Compute the  variance in [0,endpoint]
                double AverageX1 = (SumX[endpoint]) / (endpoint + 1);
                double AverageXX1 = (SumXX[endpoint]) / (endpoint + 1);
                Variance[endpoint, 0] = (AverageXX1 - Math.Pow(AverageX1, 2)) * (endpoint + 1);
            }

            // Now do the dynamic programming recursion for more than 1 bucket
            for (int buckets = 1; buckets < NumberOfBuckets; buckets++) // compute a multi_bucket solution
            {
                for (int endpoint = (buckets == NumberOfBuckets - 1) ? count - 1 : buckets /* Any single-value bucket has 0 variance */ ; endpoint < count; endpoint++)
                {
                    double MinVariance = double.MaxValue;
                    int MinSplit = -1;

                    for (int split = buckets - 1 /* Any single-value bucket has 0 variance */ ; split < endpoint; split++)
                    {
                        // consider splitting the array [0,endpoint] into [0,split][split+1,endpoint]

                        if (ValidSplitPoints.Count > 0 && !ValidSplitPoints.Contains(split))
                            continue;   // If this is not a valid split point, we're done here


                        double Var1 = Variance[split, buckets - 1];

                        double AverageX2 = (SumX[endpoint] - SumX[split]) / (endpoint - split);
                        double AverageXX2 = (SumXX[endpoint] - SumXX[split]) / (endpoint - split);
                        double Var2 = (AverageXX2 - Math.Pow(AverageX2, 2)) * (endpoint - split);

                        double SplitVariance = Var1 + Var2;

                        if (MinVariance > SplitVariance)
                        {
                            MinSplit = split;
                            MinVariance = SplitVariance;
                        }
                    }

                    // Assign the optimal split

                    OptSplit[endpoint, buckets] = MinSplit;
                    Variance[endpoint, buckets] = MinVariance;
                }

            }
            

            // 
            //  Additional Performance optimization for faster partitioning
            //
            //  - Currently not enabled

            //else
            //{

            //    // Compute an approximation of the minimum

            //    int no_intervalls = Math.Max(count,20);
            //    double Approx_Minimum = double.MaxValue;
            //    int end = count - 1;
            //    double AverageX1, AverageXX1, V1;
            //    double AverageX2, AverageXX2, V2;

            //    for (int l = 1; l < no_intervalls; l++)
            //    {
            //        int position = 0 + l * (count / no_intervalls);

            //        AverageX1 = (SumX[position]) / (position + 1);
            //        AverageXX1 = (SumXX[position]) / (position + 1);
            //        V1 = (AverageXX1 - Math.Pow(AverageX1, 2)) * (position + 1);
            //        AverageX2 = (SumX[end] - SumX[position]) / (end - position);
            //        AverageXX2 = (SumXX[end] - SumXX[position]) / (end - position);
            //        V2 = (AverageXX2 - Math.Pow(AverageX2, 2)) * (end - position);
            //        if (Approx_Minimum > V1+V2)
            //            Approx_Minimum = V1+V2;
            //    }

            //    //
            //    //  Compute S0 and the left boundary of the split location
            //    //

            //    int left = 0;
            //    int right = count-2;
            //    double S, S_last;
            //    int h, h_last;
            //    h_last =-1;
            //    S = Approx_Minimum;

                
                
            //    while (true)
            //    {

            //        while (left <= right)
            //        {
            //            int mid = (left + right) / 2;

            //            // Compute the corresponding error

            //            AverageX2 = (SumX[end] - SumX[mid - 1]) / (end - mid + 1);
            //            AverageXX2 = (SumXX[end] - SumXX[mid - 1]) / (end - mid + 1);
            //            V2 = (AverageXX2 - Math.Pow(AverageX2, 2)) * (end - mid + 1);

            //            if (V2 > S)
            //                left = mid + 1;
            //            else
            //                right = mid -1;
            //        }

            //        AverageX2 = (SumX[end] - SumX[left - 1]) / (end - left + 1);
            //        AverageXX2 = (SumXX[end] - SumXX[left - 1]) / (end - left + 1);
            //        V2 = (AverageXX2 - Math.Pow(AverageX2, 2)) * (end - left + 1);

            //        if (V2 <= S) h = right; else h = left;
            //        S_last = S;

            //        AverageX1 = (SumX[h]) / (h + 1);
            //        AverageXX1 = (SumXX[h]) / (h + 1);
            //        V1 = (AverageXX1 - Math.Pow(AverageX1, 2)) * (h + 1);

            //        S = Approx_Minimum - V1;
            //        if (h == h_last)
            //            break;
            //        h_last = h;
            //    }

            //    // Initialize

            //    for (int endpoint = 0; endpoint < count; endpoint++)
            //    {
            //        OptSplit[endpoint, 0] = -1;   // Only one bucket, starting at the smallest index
            //        // All intervalls trivially start at 0.

            //        // Compute the  variance in [0,endpoint]
            //        AverageX1 = (SumX[endpoint]) / (endpoint + 1);
            //        AverageXX1 = (SumXX[endpoint]) / (endpoint + 1);
            //        Variance[endpoint, 0] = (AverageXX1 - Math.Pow(AverageX1, 2)) * (endpoint + 1);
            //    }

            //    // Now do the dynamic programming recursion for more than 1 bucket
            //    for (int buckets = 1; buckets < NumberOfBuckets; buckets++) // compute a multi_bucket solution
            //    {
            //        for (int endpoint = (buckets == NumberOfBuckets - 1) ? count - 1 : buckets /* Any single-value bucket has 0 variance */ ; endpoint < count; endpoint++)
            //        {
            //            double MinVariance = double.MaxValue;
            //            int MinSplit = -1;

            //            for (int split = buckets - 1 /* Any single-value bucket has 0 variance */ ; split < endpoint; split++)
            //            {
            //                // consider splitting the array [0,endpoint] into [0,split][split+1,endpoint]

            //                if (ValidSplitPoints.Count > 0 && !ValidSplitPoints.Contains(split))
            //                    continue;   // If this is not a valid split point, we're done here


            //                double Var1 = Variance[split, buckets - 1];

            //                AverageX2 = (SumX[endpoint] - SumX[split]) / (endpoint - split);
            //                AverageXX2 = (SumXX[endpoint] - SumXX[split]) / (endpoint - split);
            //                double Var2 = (AverageXX2 - Math.Pow(AverageX2, 2)) * (endpoint - split);

            //                double SplitVariance = Var1 + Var2;

            //                if (MinVariance > SplitVariance)
            //                {
            //                    MinSplit = split;
            //                    MinVariance = SplitVariance;
            //                }
            //            }

            //            // Assign the optimal split

            //            OptSplit[endpoint, buckets] = MinSplit;
            //            Variance[endpoint, buckets] = MinVariance;
            //        }

            //    }
            //}

            // Reconstruct the bucket boundaries

            List<int> BucketSplits = new List<int>();

            BucketSplits.Add(count - 1); // the end of the last bucket
            int BucketsRemaining = NumberOfBuckets - 1;

            while (BucketsRemaining > 0)
            {
                BucketSplits.Add(OptSplit[BucketSplits.Last(), BucketsRemaining]);
                BucketsRemaining--;
            }
            BucketSplits.Add(-1);

            double Explanation_Score = 0;

            HistogramExplanation Explanation = new HistogramExplanation();
            Explanation.SetAnalysisType(OrderByCOI);

            // Compute the Average COI (we need this to 'normalize' for some types of dependencies)

            double COI_average = 0;
            double CE_average = 0;
            for (int j = 0; j < COI_Array.Length; j++)
            {
                COI_average += COI_Array[j];
                CE_average += C_Explain_Array[j];
            }
            COI_average /= COI_Array.Length;
            CE_average /= COI_Array.Length;

            DateTime Start = DateTime.MinValue;
            DateTime End = DateTime.MaxValue;

            for (int i = BucketSplits.Count - 1; i > 0; i--)
            {
                // Generate the appropriate 'bucket' in the explanation

                double COI_aggregate = 0;
                double CE_aggregate = 0;
                for (int j = BucketSplits[i] + 1; j <= BucketSplits[i - 1]; j++)
                //for (int j = BucketSplits[i]; j < BucketSplits[i - 1]; j++)
                {
                    COI_aggregate += COI_Array[j];
                    CE_aggregate += C_Explain_Array[j];
                }
                COI_aggregate /= (BucketSplits[i - 1] - BucketSplits[i]);
                CE_aggregate /= (BucketSplits[i - 1] - BucketSplits[i]);

                // compute the score used in ranking

                if (Dependency == DependencyType.Default)
                {
                    double Var = 0;
                    for (int j = BucketSplits[i] + 1; j <= BucketSplits[i - 1]; j++)
                        Var += Math.Pow(COI_aggregate - COI_Array[j], 2);

                    Explanation_Score += Var;
                }
                else if (Dependency == DependencyType.Linear)
                {
                    double Var = 0;
                    double Basic_Var = 0;
                    double Var_Adjusted = 0;
                    for (int j = BucketSplits[i] + 1; j <= BucketSplits[i - 1]; j++)
                    {
                        Var += Math.Pow(COI_aggregate - COI_Array[j], 2);
                        Basic_Var += Math.Pow(COI_average - COI_Array[j], 2);

                        double deviation_from_average = CE_average - C_Explain_Array[j];
                        Var_Adjusted += Math.Pow(((COI_average - COI_Array[j]) - deviation_from_average), 2);    // What's the largest reduction in Variance due to CE given that we know the dependency is at most additive ?           
                    }

                    if (Var < Var_Adjusted)
                        Explanation_Score += Var_Adjusted;
                    else Explanation_Score += Var;
                }

                Explanation.AddBucket(BucketSplits[i] + 1, COI_aggregate, CE_aggregate);
            }

            Explanation.SetNumberOfObservations(COI_Array.Length);
            Explanation.SetScore(Explanation_Score);

            //
            //  Now test if the correlation works out...
            //

            bool Obeys_Correlation_Constraint = true;
            bool small_differences = false;

            for (int l = 1; l < Explanation.CE_Aggregate.Count; l++)
            {
                if (Math.Abs(Explanation.CE_Aggregate[l - 1] - Explanation.CE_Aggregate[l]) / CE_average < 0.000001)    // test if there is a noticeable difference
                    small_differences = true;
            }

            if (Correlation != CorrelationType.Either)
            {
                double[] COI_Bucket_Array = Explanation.COI_Aggregate.ToArray();
                double[] CE_Bucket_Array = Explanation.CE_Aggregate.ToArray();

                Array.Sort(COI_Bucket_Array, CE_Bucket_Array);
                if (Correlation == CorrelationType.Positive)
                {
                    for (int l = 1; l < COI_Bucket_Array.Length; l++)
                    {
                        if (CE_Bucket_Array[l - 1] > CE_Bucket_Array[l])
                            Obeys_Correlation_Constraint = false;
                    }
                }
                if (Correlation == CorrelationType.Negative)
                {
                    for (int l = 1; l < COI_Bucket_Array.Length; l++)
                        if (CE_Bucket_Array[l - 1] < CE_Bucket_Array[l])
                            Obeys_Correlation_Constraint = false;
                }
            }

            if (Obeys_Correlation_Constraint == false || small_differences)
            {
                return null;
            }

            return Explanation;
        }

    }

}
