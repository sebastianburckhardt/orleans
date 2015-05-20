using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime.ReminderService;


#pragma warning disable 612,618
namespace UnitTestGrains
{
    // NOTE: if you make any changes here, copy them to ReminderTestCopyGrain
    public class ReminderTestGrain : GrainBase, IReminderTestGrain, IRemindable
    {
        Dictionary<string, IOrleansReminder> allReminders;
        Dictionary<string, long> sequence;
        private TimeSpan period;

        private static long ACCURACY = 50 * TimeSpan.TicksPerMillisecond; // when we use ticks to compute sequence numbers, we might get wrong results as timeouts don't happen with precision of ticks  ... we keep this as a leeway

        private OrleansLogger logger;
        private string myId; // used to distinguish during debugging between multiple activations of the same grain

        private string filePrefix;

        public override Task ActivateAsync()
        {
            myId = _Data.ActivationId.ToString();// new Random().Next();
            allReminders = new Dictionary<string, IOrleansReminder>();
            sequence = new Dictionary<string, long>();
            period = GetDefaultPeriod();
            logger = GetLogger(string.Format("ReminderGrain {0}_{1}", Identity.GetPrimaryKeyLong(), RuntimeIdentity.ToString()));
            logger.Info("{0} Activated {1}!", myId, Identity);
            filePrefix = "g" + Identity.GetPrimaryKeyLong() + "_";
            return GetMissingReminders();
        }

        public override Task DeactivateAsync()
        {
            logger.Info("{0} Deactivated {1}!", myId, Identity);
            return TaskDone.Done;
        }

        public async Task<IOrleansReminder> StartReminder(string reminderName, TimeSpan? p = null, bool validate = false)
        {
            TimeSpan usePeriod = p ?? period;
            logger.Info("Starting reminder {0} for {1}", reminderName, Identity);
            IOrleansReminder r = null;
            if (validate)
                r = await RegisterOrUpdateReminder(reminderName, /*TimeSpan.FromSeconds(3)*/usePeriod - TimeSpan.FromSeconds(2), usePeriod);
            else
                r = await GrainClient.InternalCurrent.RegisterOrUpdateReminder(reminderName, /*TimeSpan.FromSeconds(3)*/usePeriod - TimeSpan.FromSeconds(2), usePeriod).AsTask();

            allReminders[reminderName] = r;
            sequence[reminderName] = 0;

            File.Delete(GetFileName(reminderName)); // if successfully started, then remove any old data
            logger.Info("Started reminder {0} for {1}", reminderName, Identity);
            return r;
        }

        public Task ReceiveReminder(string reminderName, TickStatus status)
        {
            // it can happen that due to failure, when a new activation is created, 
            // it doesn't know which reminders were registered against the grain
            // hence, this activation may receive a reminder that it didn't register itself, but
            // the previous activation (incarnation of the grain) registered... so, play it safe
            if (!sequence.ContainsKey(reminderName))
            {
                // allReminders.Add(reminderName, r); // not using allReminders at the moment
                //counters.Add(reminderName, 0);
                sequence.Add(reminderName, 0); // we'll get upto date to the latest sequence number while processing this tick
            }

            // calculating tick sequence number

            // we do all arithmetics on DateTime by converting into long because we dont have divide operation on DateTime
            // using dateTime.Ticks is not accurate as between two invocations of ReceiveReminder(), there maybe < period.Ticks
            // if # of ticks between two consecutive ReceiveReminder() is larger than period.Ticks, everything is fine... the problem is when its less
            // thus, we reduce our accuracy by ACCURACY ... here, we are preparing all used variables for the given accuracy
            long now = status.CurrentTickTime.Ticks / ACCURACY; //DateTime.UtcNow.Ticks / ACCURACY;
            long first = status.FirstTickTime.Ticks / ACCURACY;
            long per = status.Period.Ticks / ACCURACY;
            long sequenceNumber = 1 + ((now - first) / per);

            // end of calculating tick sequence number

            // do switch-ing here
            if (sequenceNumber < sequence[reminderName])
            {
                logger.Info("{0} Incorrect tick {1} vs. {2} with status {3} for {4}", reminderName, sequence[reminderName], sequenceNumber, status, Identity);
                return TaskDone.Done;
            }

            sequence[reminderName] = sequenceNumber;
            logger.Info("{0}: {1} Sequence # {2} with status {3} for {4}", myId, reminderName, sequence[reminderName], status, Identity);

            File.WriteAllText(GetFileName(reminderName), sequence[reminderName].ToString());

            return TaskDone.Done;
        }

        public Task StopReminder(string reminderName)
        {
            logger.Info("{0}: Stoping reminder {1} for {2}", myId, reminderName, Identity);
            // we dont reset counter as we want the test methods to be able to read it even after stopping the reminder
            //return UnregisterReminder(allReminders[reminderName]);
            IOrleansReminder reminder = null;
            if (allReminders.TryGetValue(reminderName, out reminder))
            {
                return UnregisterReminder(reminder);
            }
            else
            {
                // during failures, there may be reminders registered by an earlier activation that we dont have cached locally
                // therefore, we need to update our local cache 
                return GetMissingReminders().ContinueWith((Task t) =>
                    UnregisterReminder(allReminders[reminderName]));
            }
        }

        private Task GetMissingReminders()
        {
            return base.GetReminders().ContinueWith((Task<List<IOrleansReminder>> reminders) =>
            {
                logger.Info("Got missing reminders {0}", Utils.IEnumerableToString(reminders.Result));
                foreach (IOrleansReminder l in reminders.Result)
                {
                    if (!allReminders.ContainsKey(l.ReminderName))
                    {
                        allReminders.Add(l.ReminderName, l);
                    }
                }
            });
        }


        public Task StopReminder(IOrleansReminder reminder)
        {
            logger.Info("Stoping reminder (using ref) {0} for {1}", reminder, Identity);
            // we dont reset counter as we want the test methods to be able to read it even after stopping the reminder
            return UnregisterReminder(reminder);
        }

        public Task<TimeSpan> GetReminderPeriod(string reminderName)
        {
            return Task.FromResult(period);
        }

        public Task<long> GetCounter(string name)
        {
            return Task.FromResult(long.Parse(File.ReadAllText(GetFileName(name))));
        }

        public Task<IOrleansReminder> GetReminderObject(string reminderName)
        {
            return base.GetReminder(reminderName);
        }
        public Task<List<IOrleansReminder>> GetRemindersList()
        {
            return base.GetReminders();
        }

        private string GetFileName(string reminderName)
        {
            return string.Format("{0}{1}", filePrefix, reminderName);
        }

        public static TimeSpan GetDefaultPeriod()
        {
            OrleansConfiguration config = new OrleansConfiguration();
            config.StandardLoad();
            if (config.Globals.LivenessType.Equals(GlobalConfiguration.LivenessProviderType.AzureTable))
            {
                return TimeSpan.FromSeconds(8); // azure operations take more time ... so we use a larger period
            }
            return TimeSpan.FromSeconds(6); //5);
        }

        public Task EraseReminderTable()
        {
            return ReminderTable.Clear();
        }
    }

    #region Copy of ReminderTestGrain ... to test reminders for different grain types

    // NOTE: do not make changes here ... this is a copy of ReminderTestGrain
    // changes to make when copying:
    //      1. rename logger to ReminderCopyGrain
    //      2. filePrefix should start with "gc", instead of "g"
    public class ReminderTestCopyGrain : GrainBase, IReminderTestCopyGrain, IRemindable
    {
        Dictionary<string, IOrleansReminder> allReminders;
        Dictionary<string, long> sequence;
        private TimeSpan period;

        private static long ACCURACY = 50 * TimeSpan.TicksPerMillisecond; // when we use ticks to compute sequence numbers, we might get wrong results as timeouts don't happen with precision of ticks  ... we keep this as a leeway

        private OrleansLogger logger;
        private long myId; // used to distinguish during debugging between multiple activations of the same grain

        private string filePrefix;

        public override Task ActivateAsync()
        {
            myId = new Random().Next();
            allReminders = new Dictionary<string, IOrleansReminder>();
            sequence = new Dictionary<string, long>();
            period = ReminderTestGrain.GetDefaultPeriod();
            logger = GetLogger(string.Format("ReminderCopyGrain {0}_{1}", myId, Identity.GetPrimaryKeyLong()));
            logger.Info("{0} Activated {1}!", myId, Identity);
            filePrefix = "gc" + Identity.GetPrimaryKeyLong() + "_";
            return GetMissingReminders();
        }

        public override Task DeactivateAsync()
        {
            logger.Info("{0} Deactivated {1}!", myId, Identity);
            return TaskDone.Done;
        }

        public async Task<IOrleansReminder> StartReminder(string reminderName, TimeSpan? p = null, bool validate = false)
        {
            TimeSpan usePeriod = p ?? period;
            logger.Info("Starting reminder {0} for {1}", reminderName, Identity);
            IOrleansReminder r = null;
            if (validate)
                r = await RegisterOrUpdateReminder(reminderName, /*TimeSpan.FromSeconds(3)*/usePeriod - TimeSpan.FromSeconds(2), usePeriod);
            else
                r = await GrainClient.InternalCurrent.RegisterOrUpdateReminder(reminderName, /*TimeSpan.FromSeconds(3)*/usePeriod - TimeSpan.FromSeconds(2), usePeriod).AsTask();
            if (allReminders.ContainsKey(reminderName))
            {
                allReminders[reminderName] = r;
                sequence[reminderName] = 0;
            }
            else
            {
                allReminders.Add(reminderName, r);
                sequence.Add(reminderName, 0);
            }

            File.Delete(GetFileName(reminderName)); // if successfully started, then remove any old data
            logger.Info("Started reminder {0} for {1}", reminderName, Identity);
            return r;
        }

        public Task ReceiveReminder(string reminderName, TickStatus status)
        {
            // it can happen that due to failure, when a new activation is created, 
            // it doesn't know which reminders were registered against the grain
            // hence, this activation may receive a reminder that it didn't register itself, but
            // the previous activation (incarnation of the grain) registered... so, play it safe
            if (!sequence.ContainsKey(reminderName))
            {
                // allReminders.Add(reminderName, r); // not using allReminders at the moment
                //counters.Add(reminderName, 0);
                sequence.Add(reminderName, 0); // we'll get upto date to the latest sequence number while processing this tick
            }

            // calculating tick sequence number

            // we do all arithmetics on DateTime by converting into long because we dont have divide operation on DateTime
            // using dateTime.Ticks is not accurate as between two invocations of ReceiveReminder(), there maybe < period.Ticks
            // if # of ticks between two consecutive ReceiveReminder() is larger than period.Ticks, everything is fine... the problem is when its less
            // thus, we reduce our accuracy by ACCURACY ... here, we are preparing all used variables for the given accuracy
            long now = status.CurrentTickTime.Ticks / ACCURACY; //DateTime.UtcNow.Ticks / ACCURACY;
            long first = status.FirstTickTime.Ticks / ACCURACY;
            long per = status.Period.Ticks / ACCURACY;
            long sequenceNumber = 1 + ((now - first) / per);

            // end of calculating tick sequence number

            // do switch-ing here
            if (sequenceNumber < sequence[reminderName])
            {
                logger.Info("{0} Incorrect tick {1} vs. {2} with status {3} for {4}", reminderName, sequence[reminderName], sequenceNumber, status, Identity);
                return TaskDone.Done;
            }

            sequence[reminderName] = sequenceNumber;
            logger.Info("{0}: {1} Sequence # {2} with status {3} for {4}", myId, reminderName, sequence[reminderName], status, Identity);

            File.WriteAllText(GetFileName(reminderName), sequence[reminderName].ToString());

            return TaskDone.Done;
        }

        public Task StopReminder(string reminderName)
        {
            logger.Info("Stoping reminder {0} for {1}", reminderName, Identity);
            // we dont reset counter as we want the test methods to be able to read it even after stopping the reminder
            //return UnregisterReminder(allReminders[reminderName]);
            IOrleansReminder reminder = null;
            if (allReminders.TryGetValue(reminderName, out reminder))
            {
                return UnregisterReminder(reminder);
            }
            else
            {
                // during failures, there may be reminders registered by an earlier activation that we dont have cached locally
                // therefore, we need to update our local cache 
                return GetMissingReminders().ContinueWith((Task t) =>
                    UnregisterReminder(allReminders[reminderName]));
            }
        }

        private Task GetMissingReminders()
        {
            return base.GetReminders().ContinueWith((Task<List<IOrleansReminder>> reminders) =>
            {
                foreach (IOrleansReminder l in reminders.Result)
                {
                    if (!allReminders.ContainsKey(l.ReminderName))
                    {
                        allReminders.Add(l.ReminderName, l);
                    }
                }
            });
        }

        public Task StopReminder(IOrleansReminder reminder)
        {
            logger.Info("Stoping reminder (using ref) {0} for {1}", reminder, Identity);
            // we dont reset counter as we want the test methods to be able to read it even after stopping the reminder
            return UnregisterReminder(reminder);
        }

        public Task<TimeSpan> GetReminderPeriod(string reminderName)
        {
            return Task.FromResult(period);
        }

        public Task<long> GetCounter(string name)
        {
            return Task.FromResult(long.Parse(File.ReadAllText(GetFileName(name))));
        }

        public Task<IOrleansReminder> GetReminderObject(string reminderName)
        {
            return base.GetReminder(reminderName);
        }
        public Task<List<IOrleansReminder>> GetRemindersList()
        {
            return base.GetReminders();
        }

        private string GetFileName(string reminderName)
        {
            return string.Format("{0}{1}", filePrefix, reminderName);
        }
    }

    #endregion

    #region The wrong reminder grain

    public class WrongReminderGrain : GrainBase, IReminderGrainWrong
    {
        private OrleansLogger logger;

        public override Task ActivateAsync()
        {
            logger = GetLogger("ReminderGrain");
            logger.Info("Activated {0}", Identity);
            return TaskDone.Done;
        }

        public async Task<bool> StartReminder(string reminderName)
        {
            logger.Info("Starting reminder {0} for {1}", reminderName, Identity);
            IOrleansReminder r = await RegisterOrUpdateReminder(reminderName, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3));
            logger.Info("Started reminder {0} for {1}. It shouldn't have succeeded!", reminderName, Identity);
            return true;
        }
    }
    #endregion

    #region reproduce error with multiple interfaces
    //public class MyDerivedGrain : GrainBase, IDerivedGrain
    //{
    //    public Task SomeTestMethod()
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public Task ReceiveReminder(string reminderName, ReminderStatus status, object state)
    //    {
    //        throw new NotImplementedException();
    //    }
    //}
    //public class MyBaseGrain : GrainBase, IBaseGrain
    //{
    //    public Task SomeTestMethod()
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public Task ReceiveReminder(string reminderName, ReminderStatus status, object state)
    //    {
    //        throw new NotImplementedException();
    //    }
    //}
    #endregion
}
#pragma warning restore 612,618