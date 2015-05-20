using System;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Orleans
{
    /// <summary>
    /// The ObserverSubscriptionManager class is a helper class for grains that support observers.
    /// It provides methods for tracking subscribing observers and for sending notifications.
    /// </summary>
    /// <typeparam name="T">The observer interface type to be managed.</typeparam>
    [Serializable]
    public class ObserverSubscriptionManager<T>
        where T : IGrainObserver
    {
        /// <summary>
        /// Number of subscribers currently registered
        /// </summary>
        public int Count 
        {
            get { return _observers.Count; }
        }

        /// <summary>
        /// The set of currently-subscribed observers.
        /// This is implemented as a dictionary keyed by GrainID so that if the same obsever subscribes multiple times,
        /// it will still only get invoked once per notification.
        /// </summary>
        private readonly Dictionary<GrainId,T> _observers;

        /// <summary>
        /// Constructs an empty subscription manager.
        /// </summary>
        public ObserverSubscriptionManager()
        {
            _observers = new Dictionary<GrainId, T>();
        }

        /// <summary>
        /// Records a new subscribing observer.
        /// </summary>
        /// <param name="observer">The new subscriber.</param>
        /// <returns>A promise that resolves when the subscriber is added.
        /// <para>This promise will be broken if the observer is already a subscriber.
        /// In this case, the existing subscription is unaffected.</para></returns>
        public void Subscribe(T observer)
        {
            GrainId id = ((GrainReference)((IGrainObserver)observer)).GrainId; // for some reason can't cast directly to GrainReference
            
            if (_observers.ContainsKey(id))
                throw new OrleansException(String.Format("Cannot subscribe already subscribed observer {0}.", observer));

            _observers.Add(id, observer);
        }


        /// <summary>
        /// Removes a (former) subscriber.
        /// </summary>
        /// <param name="observer">The unsubscribing observer.</param>
        /// <returns>A promise that resolves when the subscriber is removed.
        /// This promise will be broken if the observer is not a subscriber.</returns>
        public void Unsubscribe(T observer)
        {
            GrainId id = ((GrainReference)((IGrainObserver)observer)).GrainId; // for some reason can't cast directly to GrainReference
            if (!_observers.ContainsKey(id))
                throw new OrleansException(String.Format("Observer {0} is not subscribed.", observer));

            _observers.Remove(id);
        }
        
        /// <summary>
        /// Removes all subscriptions.
        /// </summary>
        public void Clear()
        {
            _observers.Clear();
        }

        /// <summary>
        /// Sends a notification to all subscribers.
        /// </summary>
        /// <param name="notification">An action that sends the notification by invoking the proper method on the provided subscriber.
        /// This action is called once for each current subscriber.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void Notify(Action<T> notification)
        {
            var failed = new List<GrainId>();

            foreach (var pair in _observers)
            {
                try
                {
                    notification(pair.Value);
                }
                catch (Exception)
                {
                    failed.Add(pair.Key);
                }
            }
            foreach (var key in failed)
            {
                _observers.Remove(key);
            }
        }
    }
}