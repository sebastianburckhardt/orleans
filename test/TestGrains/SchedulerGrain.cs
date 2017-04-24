using System;
using System.Threading.Tasks;
using Orleans;
using UnitTests.GrainInterfaces;
using System.Collections.Generic;
using System.Linq;
using Orleans.Concurrency;
using Orleans.Runtime;
using System.Text;
using System.Diagnostics;

namespace UnitTests.Grains
{
    [Reentrant]
    public class SchedulerGrain : Grain, ISchedulerGrain
    {
        private IReadOnlyList<SchedulerStep> schedule;
        private Dictionary<string, ParticipantInfo> participants;
        private int nextStep;
        private int numberStarted;
        private TaskCompletionSource<int> endOfSchedulePromise;

        private class ParticipantInfo
        {
            public TaskCompletionSource<int> Promise;
            public string Label;
        }

        public Task Initialize(IEnumerable<SchedulerStep> schedule)
        {
            this.schedule = schedule.ToList();
            participants = new Dictionary<string, ParticipantInfo>();
            foreach (var p in schedule.Select(step => step.ParticipantId).Distinct())
            {
                participants.Add(p, new ParticipantInfo() {
                    Label = "(not started)"
                });
            }
            nextStep = 0;
            numberStarted = 0;
            endOfSchedulePromise = new TaskCompletionSource<int>();
            return TaskDone.Done;
        }

        public Task Step(string participantId, string label)
        {
            // we are no longer tracking steps once the schedule has completed 
            if (endOfSchedulePromise.Task.IsCompleted)
                return TaskDone.Done;

            // sanity check: this participant must appear in the schedule
            ParticipantInfo info;
            if (!participants.TryGetValue(participantId, out info))
            {
               throw new OrleansException($"Invalid step: unknown participant {participantId}");
            }
          
            // sanity check: same participant must not perform concurrent steps
            if (participants[participantId].Promise != null)
            {
               return Error($"Invalid scheduler reentrancy: participant {participantId} reached {label} while blocked at {participants[participantId].Label}", info);
            }

            // record the label reached
            info.Label = label;

            // check if the label matches the expected label
            var expected = schedule.Skip(nextStep).Where(step => step.ParticipantId == participantId)
                .FirstOrDefault()?.ExpectedLabel;
            if (expected != null && expected != label)
            {
                var msg = $"Participant {participantId} deviated from expected schedule: reached label {label}, not {expected}";
                info.Label = info.Label + $"(expected:{expected})";
                return Error(msg, info);
            }

            // in the beginning, we hold all but last-to-arrive participant
            if (++numberStarted < participants.Count)
            {
                return WaitForMyTurn(info);
            }

            // if we have reached the end of the schedule, signal and release
            if (nextStep >= schedule.Count)
            {
                SignalCompletionAsynchronously(endOfSchedulePromise);
                return WaitForMyTurn(info);
            }

            // pop the next to go from the schedule
            var nextToGo = participants[schedule[nextStep++].ParticipantId];

            if (info != nextToGo)
            {
                WakeUp(nextToGo);
                return WaitForMyTurn(info);
            }
            else
            {
                Resume(info);
                return TaskDone.Done;
            }
        }

        private Task Error(string message, ParticipantInfo info)
        {
            var exception = new OrleansException(message);

            SignalErrorAsynchronously(endOfSchedulePromise, exception);

            return WaitForMyTurn(info);
        }

        public async Task Finish()
        {
            var timeout = Task.Delay(Debugger.IsAttached ? TimeSpan.FromDays(1) : TimeSpan.FromSeconds(10));

            await Task.WhenAny(endOfSchedulePromise.Task, timeout);

            if (! timeout.IsCompleted)
                await endOfSchedulePromise.Task;

            // check if all participants are at completion label 
            var incomplete = participants
                .Where(kvp => kvp.Value.Label != SchedulerStep.CompletionLabel)
                .Select(kvp => $"{kvp.Key}:{kvp.Value.Label}")
                .ToList();

            if (incomplete.Count != 0)
            {
                throw new OrleansException($"final state: {string.Join(", ", incomplete)}");
            }

            foreach (var info in participants.Values)
            {
                WakeUp(info);
            }
        }

        private async Task WaitForMyTurn(ParticipantInfo info)
        {
            // create a promise used to wake us up
            var promise = new TaskCompletionSource<int>();
            info.Promise = promise;

            // wait until we are woken up
            await promise.Task;

            Resume(info);
        }

        private void WakeUp(ParticipantInfo info)
        {
            info.Label = info.Label + "(awoken)";
            SignalCompletionAsynchronously(info.Promise);
        }

        private void Resume(ParticipantInfo info)
        {
            info.Promise = null;
            info.Label = info.Label + "(running)";
        }


        private static void SignalCompletionAsynchronously(TaskCompletionSource<int> tcs)
        {
            // may possibly be improved under .NET 4.6
            Task.Factory.StartNew(() => tcs.TrySetResult(0));
        }

        private static void SignalErrorAsynchronously(TaskCompletionSource<int> tcs, Exception e)
        {
            // may possibly be improved under .NET 4.6
            Task.Factory.StartNew(() => tcs.TrySetException(e));
        }
    }
}
