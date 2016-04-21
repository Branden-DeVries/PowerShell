

function Get-PartitionAccessPath{
    [cmdletbinding()]
    [OutputType([PSCustomObject])]
    param(
        [switch]$ExcludeVolumeGuid
    )
    Get-Disk|%{
        Write-Verbose -Message "Getting Disk Info for $($_.Number)"
        $DiskNumber=$_.Number
        $FriendlyName=$_.FriendlyName
        $Guid=$_.Guid
        $Manufacturer=$_.Manufacturer
        $SerialNumber=$_.SerialNumber
        $UniqueID=$_.UniqueId
        Get-Partition -DiskNumber $_.Number|%{
            $PartitionNumber=$_.PartitionNumber


            $_.AccessPaths|%{
            Write-Verbose -Message "Getting Access Paths"
            if(-not [string]::IsNullOrEmpty($_)){
                if((-not $ExcludeVolumeGuid.IsPresent) -or ($_ -notlike "\\?\Volume*")){
                    $property= @{ DiskNumber=$DiskNumber;
                                  PartitionNumber=$PartitionNumber;
                                  AccessPath=$_;
                                  SerialNumber=$SerialNumber;
                                  Manufacturer=$Manufacturer;
                                  FriendlyName=$FriendlyName;
                                  Guid=$Guid
                                  UniqueID=$UniqueID
                                }
                    $obj=New-Object -TypeName psobject -Property $property
                    Write-Output $obj  
                } 
            } #If AccessPath NullOrEmpty
        }


        
    
        }#foreach partition
    } #foreach disk

}#Get-PartitionAccessPath


function Get-DBFileDiskMapping{
    [cmdletbinding()]
    [OutputType([PSCustomObject])]
    param(
        [parameter(Mandatory=$true)]
        [string]$SQLInstance='.\KP'
    
    )


    $GetDBVolumeMappingSQL=@"
SELECT b.name as 'DatabaseName',b.database_id,volume_mount_point, volume_id,logical_volume_name,count(*) as 'filecount' FROM sys.master_files a
inner join sys.databases b on (b.database_id=a.database_id)
CROSS APPLY sys.dm_os_volume_stats(b.database_id, a.file_id)
WHERE b.database_id >4
group by b.name,b.database_id,volume_mount_point, volume_id,logical_volume_name
order by databasename 
"@

    Try{
        if((Get-Module).Name -notcontains 'SQLPS'){
            Write-Verbose -Message "Loading SQLPS Module"
            $currentdir =(Get-Item -Path ".\").FullName #Since SQLPS Changes Directory when Importing Module
            Import-Module SQLPS -ErrorAction Stop -Verbose:$false -PassThru -WarningVariable warn |Out-Null
            Set-Location -Path $currentdir
        }

        $DiskMapping=Get-PartitionAccessPath|Sort DiskNumber
        Write-Verbose -Message "Executing SQL Query"
        $DBVolumeMapping=Invoke-Sqlcmd -ServerInstance $SQLInstance -Database 'master' -Query $GetDBVolumeMappingSQL -ErrorAction Stop

        $DBVolumeMapping|%{
                Write-Verbose -Message "Getting DBVolumeMapping $($_.volume_id)"
                $Volume_id = $_.volume_id
                $Disk = $DiskMapping.Where({$_.AccessPath -eq $Volume_id})
                Add-Member -InputObject $_ -MemberType NoteProperty -Name DiskNumber -Value $Disk.DiskNumber -PassThru|Out-Null
                Add-Member -InputObject $_ -MemberType NoteProperty -Name SerialNumber -Value $Disk.SerialNumber -PassThru|Out-Null
                Add-Member -InputObject $_ -MemberType NoteProperty -Name Manufacturer -Value $Disk.Manufacturer -PassThru|Out-Null
                Add-Member -InputObject $_ -MemberType NoteProperty -Name FriendlyName -Value $Disk.FriendlyName -PassThru|Out-Null
                Add-Member -InputObject $_ -MemberType NoteProperty -Name PartitionNumber -Value $Disk.PartitionNumber -PassThru|Out-Null
                Add-Member -InputObject $_ -MemberType NoteProperty -Name UniqueID -Value $Disk.UniqueID -PassThru|Out-Null
                $_.pstypenames.Insert(0,'Disk.DBFileMapping') 
                Write-Output $_|Select DiskNumber,PartitionNumber,DatabaseName,SerialNumber,UniqueID,volume_mount_point,volume_id
        }#ForEach DBVolumeMapping


    } catch {
        throw "Error getting Database Disk Mapping`n$Error[0]"
    
    }

} #Get-DBVolumeMapping

