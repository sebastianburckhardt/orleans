using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.EventSourcing;
using Orleans.Providers;
using TestGrainInterfaces;

namespace TestGrains
{
    [JournaledStorageProvider(ProviderName = "GetEventStore")]
    //[JournaledStorageProvider(ProviderName = "MemoryStore")]
    public class JournaledPersonGrain : JournaledGrain<PersonState>, IJournaledPersonGrain, ICustomStreamName
    {
        public Task RegisterBirth(PersonAttributes props)
        {
            if (this.State.FirstName == null)
            {
                RaiseEvent(new PersonRegistered(props.FirstName, props.LastName, props.Gender));

                return Commit();
            }

            return TaskDone.Done;
        }

        public async Task Marry(IJournaledPersonGrain spouse)
        {
            if (State.IsMarried)
                throw new NotSupportedException(string.Format("{0} is already married.", State.LastName));

            var spouseData = await spouse.GetPersonalAttributes();

            RaiseEvent(new PersonMarried(spouse.GetPrimaryKey(), spouseData.FirstName, spouseData.LastName));

            if (State.LastName != spouseData.LastName)
            {
                RaiseEvent(new PersonLastNameChanged(spouseData.LastName));
            }

            await Commit();
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

        public string GetStreamName()
        {
            return string.Concat(this.GetType().Name, "-", this.GetPrimaryKey().ToString());
        }
    }
}
