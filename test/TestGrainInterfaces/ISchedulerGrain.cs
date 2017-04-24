using System.Threading.Tasks;
using Orleans;
using System.Collections.Generic;
using System;

namespace UnitTests.GrainInterfaces
{
    /// <summary>
    /// A reentrant grain that is used for sequencing events in tests.
    /// </summary>
    public interface ISchedulerGrain : IGrainWithGuidKey
    {
        /// <summary>
        /// set up a schedule that describes a controlled sequence of participants' steps
        /// </summary>
        /// <param name="schedule">The list of steps</param>
        Task Initialize(IEnumerable<SchedulerStep> schedule);

        /// <summary>
        /// called by participants; will block until it is time for them to take a step
        /// </summary>
        /// <param name="id">The calling participant id</param>
        /// <param name="label">The label the participant is at (names the next step)</param>
        Task Step(string id, string label);

        /// <summary>
        /// Wait for schedule to be over, and check that all participants have reached completion
        /// </summary>
        /// <returns></returns>
        Task Finish();
    }
 
    [Serializable]
    public class SchedulerStep
    {
        /// <summary>
        /// the Id of the participant taking this step
        /// </summary>
        public string ParticipantId { get; set; }

        /// <summary>
        /// The expected label for this participant when taking this step.
        /// </summary>
        public string ExpectedLabel { get; set; }

        /// <summary>
        /// The label reserved for indicating that a participant has completed
        /// </summary>
        public const string CompletionLabel = "Done";

        /// <summary>
        /// A utility constructor
        /// </summary>
        public SchedulerStep(string participantAndLabel)
        {
            var comps = participantAndLabel.Split(':');
            ParticipantId = comps[0];
            ExpectedLabel = comps[1];
        }

    }
}
