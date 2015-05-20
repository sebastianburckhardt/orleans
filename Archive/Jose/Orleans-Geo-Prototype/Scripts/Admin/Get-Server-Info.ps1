function GetInfo {
		[CmdletBinding()]
	param(
		[Parameter(Position=0, Mandatory=$true)]
		[ValidateNotNullOrEmpty()]
		[System.String]
		$ServerName
		
	)		

		
			gwmi -computer $ServerName Win32_ComputerSystem | Format-List Name,Domain,Manufacturer,Model,SystemType
			gwmi -computer $ServerName Win32_Processor | Format-List Caption,Name,Manufacturer,ProcessorId,NumberOfCores,AddressWidth
			gwmi -computer $ServerName Win32_OperatingSystem | Format-List @{Expression={$_.Caption};Label="OS Name"},SerialNumber,OSArchitecture
}

#Example of using this function
#$Servers = "17xcg1601", "17xcg1602"
$Servers = Get-Content cluster17.txt

$Servers | Foreach {
	GetInfo $_
}
