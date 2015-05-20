// This file is used by Code Analysis to maintain SuppressMessage 
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given 
// a specific target and scoped to a namespace, type, member, etc.
//
// To add a suppression to this file, right-click the message in the 
// Error List, point to "Suppress Message(s)", and click 
// "In Project Suppression File".
// You do not need to add suppressions to this file manually.

[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Environment.#Orleans.IMessageTarget.Activation")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Environment.#Orleans.IMessageTarget.Grain")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Environment.#Orleans.IMessageTarget.HandleNewRequest(Orleans.Message)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Environment.#Orleans.IMessageTarget.HandleResponse(Orleans.Message)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Environment.#Orleans.Tasks.IEnvironment.Commit(Orleans.SiloAddress,Orleans.TaskId)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Environment.#Orleans.Tasks.IEnvironment.Committed(Orleans.SiloAddress,System.Int32,System.Collections.Generic.HashSet`1<Orleans.TaskId>,System.Collections.Generic.Dictionary`2<Orleans.ActivationAddress,Orleans.GrainState>,System.Collections.Generic.HashSet`1<Orleans.SiloAddress>)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Environment.#Orleans.Tasks.IEnvironment.Release(Orleans.SiloAddress,Orleans.TaskId,Orleans.TaskHeader)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Environment.#Orleans.Tasks.IEnvironment.Serialize(Orleans.SiloAddress,Orleans.ActivationAddress,System.Boolean)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Manager.#Orleans.Tasks.IManager.Commit(Orleans.RequestId,Orleans.TaskId)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Manager.#Orleans.Tasks.IManager.Complete(Orleans.TaskId)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Manager.#Orleans.Tasks.IManager.CreateLocalActivation(Orleans.ActivationAddress,Orleans.GrainState,Orleans.TaskHeader)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Manager.#Orleans.Tasks.IManager.Deserialize(Orleans.ActivationAddress,Orleans.GrainState)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Manager.#Orleans.Tasks.IManager.Fail(Orleans.RequestId)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Manager.#Orleans.Tasks.IManager.GetEnvironment()")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Manager.#Orleans.Tasks.IManager.Released(Orleans.ActivationAddress)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Manager.#Orleans.Tasks.IManager.Serialize(Orleans.ActivationAddress)")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1033:InterfaceMethodsShouldBeCallableByChildTypes", Scope = "member", Target = "Orleans.Tasks.Manager.#Orleans.Tasks.IManager.UnregisterActivation(Orleans.ActivationAddress)")]
