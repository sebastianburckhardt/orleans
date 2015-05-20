# Send a PrepareAndStartQueryEvent to the currently configured EventBus to prepare the PerformanceCounterQuery. 

$eventChannel = $BD.EventChannel
$prepareAndStartQueryEvent = new-object ManagementFramework.Events.QueryManagement.PrepareAndStartQueryEvent
$prepareAndStartQueryEvent.QueryInstanceName = "TracerQueryInstance01"
$prepareAndStartQueryEvent.QueryFileName = "Orleans.Management.Agents.dll"
$prepareAndStartQueryEvent.QueryTypeName = "Orleans.Management.Agents.TracerQuery"
$prepareAndStartQueryEvent.QueryInstanceDescription = "FirstInstance"
$inputStreamDictionary = New-Object 'System.Collections.Generic.Dictionary[string, string]'
$inputStreamPerf = "FALSE:Orleans.Management.Agents.dll:Orleans.Management.Agents.TracerReportEvent"
$inputStreamDictionary.Add("0", $inputStreamPerf)
$prepareAndStartQueryEvent.EncodedInputStreams = $inputStreamDictionary
$prepareAndStartQueryEvent.OutputEventFileName = "Orleans.Management.Agents.dll"
$prepareAndStartQueryEvent.OutputEventTypeName = "Orleans.Management.Agents.TracerOutputEvent"
$eventChannel.Publish($prepareAndStartQueryEvent)
