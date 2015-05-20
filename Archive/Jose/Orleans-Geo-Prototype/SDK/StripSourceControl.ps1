#Strips the Source Control information from C# project files.
#requires -version 2.0

param([string]$sourceDirectory)

# Change the path to where we think it should be (see http://huddledmasses.org/powershell-power-user-tips-current-directory/).
[Environment]::CurrentDirectory=(Get-Location -PSProvider FileSystem).ProviderPath

Function Clear-XmlElementValue ($xml, $elementName)
{
	$element = ($xml | Where {$_.$elementName})

	if ($element) 
	{
		$element.$elementName = ""
	}
}

Echo "Removing Source Control information from SDK sample PROJECT files..."
$projectFiles = Get-ChildItem $sourceDirectory -Recurse -Include *.csproj, *.ccproj
foreach ($projectFile in $projectFiles) 
{
	$projectXml = New-Object XML

try 
	{
		$Error.Clear()
		$projectXml.Load($projectFile.FullName)
	}
	catch [System.Management.Automation.MethodInvocationException]
	{
		Echo ""
		Echo "Start Error  ************************"
		Echo "**** Could not load file $projectFile"
		$Error
		Echo "End Error  **************************"
		Echo ""
		continue
	}
	Clear-XmlElementValue $projectXml.Project.PropertyGroup "SccProjectName"
	Clear-XmlElementValue $projectXml.Project.PropertyGroup "SccLocalPath"
	Clear-XmlElementValue $projectXml.Project.PropertyGroup "SccAuxPath"
	Clear-XmlElementValue $projectXml.Project.PropertyGroup "SccProvider"
	$projectXml.Save($projectFile.FullName)
}

Echo "Removing Source Control information from SDK sample SOLUTION files..."
$solutionFiles = Get-ChildItem $sourceDirectory -Recurse -Filter "*.sln"
foreach ($solutionFile in $solutionFiles) 
{
	[string] $solutionContent = [System.IO.File]::ReadAllText($solutionFile.FullName)

	$tfsSolutionRegex = [regex]"(?s)\bGlobalSection\(TeamFoundationVersionControl\).*?EndGlobalSection"
	$revisedSolutionContent = $tfsSolutionRegex.Replace($solutionContent, "")	
	$revisedSolutionContent | Out-File $solutionFile.FullName
}

