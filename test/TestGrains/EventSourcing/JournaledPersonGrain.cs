using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.EventSourcing;
using Orleans.Providers;
using TestGrainInterfaces;
using System.Collections.Generic;

namespace TestGrains
{
    [LogViewProvider(ProviderName = "TestEventStore")]
    public class JournaledPersonGrain : JournaledGrain<PersonState>, IJournaledPersonGrain, ICustomStreamName
    {

        public Task RegisterBirth(PersonAttributes props)
        {
            if (this.State.FirstName == null)
            {
                RaiseEvent(new PersonRegistered(props.FirstName, props.LastName, props.Gender));

                return WaitForConfirmation();
            }

            return TaskDone.Done;
        }

        public async Task Marry(IJournaledPersonGrain spouse)
        {
            if (State.IsMarried)
                throw new NotSupportedException(string.Format("{0} is already married.", State.LastName));

            var spouseData = await spouse.GetPersonalAttributes();

            var events = new List<object>();

            events.Add(new PersonMarried(spouse.GetPrimaryKey(), spouseData.FirstName, spouseData.LastName));

            if (State.LastName != spouseData.LastName)
            {
                events.Add(new PersonLastNameChanged(spouseData.LastName));
            }

            RaiseEvents(events);

            await WaitForConfirmation();
        }

        public Task ChangeLastName(string lastName)
        {
            RaiseEvent(new PersonLastNameChanged(lastName));

            return TaskDone.Done;
        }

        public Task SaveChanges()
        {
            return WaitForConfirmation();
        }

        public Task<PersonAttributes> GetPersonalAttributes()
        {
            return Task.FromResult(new PersonAttributes
            {
                FirstName = State.FirstName,
                LastName = State.LastName,
                Gender = State.Gender
            });
        }

        public Task<PersonAttributes> GetConfirmedPersonalAttributes()
        {
            return Task.FromResult(new PersonAttributes
            {
                FirstName = ConfirmedState.FirstName,
                LastName = ConfirmedState.LastName,
                Gender = ConfirmedState.Gender
            });
        }

        public Task<int> GetConfirmedVersion()
        {
            return Task.FromResult(ConfirmedVersion);
        }

        public Task<int> GetVersion()
        {
            return Task.FromResult(Version);
        }

        public string GetStreamName()
        {
            return string.Concat(this.GetType().Name, "-", this.GetPrimaryKey().ToString());
        }

        protected override void TransitionState(PersonState state, object @event)
        {
            dynamic e = @event;
            state.Apply(e);
        }
    }
}
