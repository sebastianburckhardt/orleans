using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrains
{

    public interface IReminderTestGrain : IGrain
    {
        Task<IOrleansReminder> StartReminder(string reminderName, TimeSpan? period = null, bool validate = false);

        Task StopReminder(string reminderName);
        Task StopReminder(IOrleansReminder reminder);

        Task<TimeSpan> GetReminderPeriod(string reminderName);
        Task<long> GetCounter(string name);
        Task<IOrleansReminder> GetReminderObject(string reminderName);
        Task<List<IOrleansReminder>> GetRemindersList();

        Task EraseReminderTable();
    }

    // to test reminders for different grain types
    public interface IReminderTestCopyGrain : IGrain
    {
        Task<IOrleansReminder> StartReminder(string reminderName, TimeSpan? period = null, bool validate = false);
        Task StopReminder(string reminderName);

        Task<TimeSpan> GetReminderPeriod(string reminderName);
        Task<long> GetCounter(string name);
    }

    public interface IReminderGrainWrong : IGrain
    // since it doesnt implement IRemindable, we should get an error at run time
    // we need a way to let the user know at compile time if s/he doesn't implement IRemindable yet tries to register a reminder
    {
        Task<bool> StartReminder(string reminderName);
    }

    #region reproduce error with multiple interfaces

    //public interface IBaseGrain : IGrain //IRemindable
    //{
    //    Task SomeTestMethod();
    //}

    //public interface IDerivedGrain : IBaseGrain
    //{
    //}

    #endregion
}
