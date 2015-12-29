namespace Orleans.EventSourcing
{
    public interface IJournaledGrainState
    {
        int Version { get; }

        void TransitionState<TEvent>(TEvent @event);
    }
}
