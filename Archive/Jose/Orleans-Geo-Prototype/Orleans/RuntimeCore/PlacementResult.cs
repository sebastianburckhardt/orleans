using System;


namespace Orleans
{
    internal class PlacementResult
    {
        public PlacementStrategy PlacementStrategy { get; private set; }
        public bool IsNewPlacement { get { return PlacementStrategy != null; } }
        public ActivationId Activation { get; private set; }
        public SiloAddress Silo { get; private set; }
        /// <summary>
        /// Some storage providers need to know the grain type in order to read the state.
        /// The PlacementResult is generated based on the target grain type's policy, so the type
        /// is known and will be passed in the message NewGrainType header.
        /// </summary>
        public string GrainType { get; private set; }

        private PlacementResult()
        { }

        public static PlacementResult IdentifySelection(ActivationAddress address)
        {
            return
                new PlacementResult
                    {
                        Activation = address.Activation,
                        Silo = address.Silo
                    };
        }

        public static PlacementResult
            SpecifyCreation(
                SiloAddress silo,
                PlacementStrategy placement,
                string grainType)
        {
            if (silo == null)
                throw new ArgumentNullException("silo");
            if (placement == null)
                throw new ArgumentNullException("placement");
            if (string.IsNullOrWhiteSpace(grainType))
                throw new ArgumentException("'grainType' must contain a valid typename.");

            return
                new PlacementResult
                    {
                        Activation = ActivationId.NewId(),
                        Silo = silo,
                        PlacementStrategy = placement,
                        GrainType = grainType
                    };
        }

        public ActivationAddress ToAddress(GrainId grainId)
        {
            return ActivationAddress.GetAddress(Silo, grainId, Activation);
        }

        public override string ToString()
        {
            var placementStr = IsNewPlacement ? PlacementStrategy.ToString() : "*not-new*";
            return String.Format("PlacementResult({0}, {1}, {2}, {3})",
                Silo, Activation, placementStr, GrainType);
        }
    }

    //public interface IPlacement
    //{
    //    SiloAddress GetSiloForNewGrain();
    //    SiloAddress GetSiloForNewGrain(int iSilo);
    //}

    //public interface IRouter : IPlacement
    //{
    //    void Start();
    //    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1716:IdentifiersShouldNotMatchKeywords", MessageId = "Stop")]
    //    void Stop();
    //    void Register(ActivationAddress address);
    //    void Unregister(ActivationAddress address);

    //    // Route() returns true if the message was successfully routed and may now be sent.
    //    // It returns false if the routing will happen asynchronously, in which case the
    //    // router will re-post the message when the routing completes.
    //    // If this method returns true, then the target silo must have been filled in inside
    //    // the message; if necessary, the NewPlacement flag should be set in the target grain address.
    //    bool Route(Message msg, out Message result);

    //    /// <summary>
    //    /// Lookup locally known directory information for a target grain
    //    /// </summary>
    //    /// <param name="grain"></param>
    //    /// <param name="addresses">Local addresses will always be complete, remote may be partial</param>
    //    /// <returns>True if remote addresses are complete within freshness constraint</returns>
    //    bool TryFullLookup(GrainId grain, out List<ActivationAddress> addresses);

    //    List<SiloAddress> GetMembership();
    //}

    //[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1032:ImplementStandardExceptionConstructors")]
    //[Serializable]
    //public class RoutingException : Exception
    //{
    //    public RoutingException() : base() { }

    //    public RoutingException(string msg) : base(msg) { }

    //    public RoutingException(string msg, Exception inner) : base(msg, inner) { }
    //}
}
