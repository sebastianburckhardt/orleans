using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.EventSourcing;
using Orleans.Providers;
using TestGrainInterfaces;

namespace TestGrains
{
    [JournaledStorageProvider(ProviderName = "GetEventStore")]
    public class JournaledPersonGrain : JournaledGrain<PersonState>, IJournaledPersonGrain
    {
        public Task RegisterBirth(PersonAttributes props)
        {
            if (this.State.FirstName == null)
            {
                RaiseStateEvent(new PersonRegistered(props.FirstName, props.LastName, props.Gender));

                return WaitForWriteCompletion();
            }

            return TaskDone.Done;
        }

        public async Task Marry(IJournaledPersonGrain spouse)
        {
            if (State.IsMarried)
                throw new NotSupportedException(string.Format("{0} is already married.", State.LastName));

            var spouseData = await spouse.GetPersonalAttributes();

            RaiseStateEvent(new PersonMarried(spouse.GetPrimaryKey(), spouseData.FirstName, spouseData.LastName));

            if (State.LastName != spouseData.LastName)
            {
                RaiseStateEvent(new PersonLastNameChanged(spouseData.LastName));
            }

            await WaitForWriteCompletion();
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
    }
}
