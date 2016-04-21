

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


function AddItemProperties($item, $properties, $output)
{
    if($item -ne $null)
    {
        foreach($property in $properties)
        {
            $propertyHash =$property -as [hashtable]
            if($propertyHash -ne $null)
            {
                $hashName=$propertyHash[“name”] -as [string]
                if($hashName -eq $null)
                {
                    throw “there should be a string Name” 
                }
        
                $expression=$propertyHash[“expression”] -as [scriptblock]
                if($expression -eq $null)
                {
                    throw “there should be a ScriptBlock Expression” 
                }
        
                $_=$item
                $expressionValue=& $expression
        
                $output | add-member -MemberType “NoteProperty” -Name $hashName -Value $expressionValue
            }
            else
            {
                # .psobject.Properties allows you to list the properties of any object, also known as “reflection”
                foreach($itemProperty in $item.psobject.Properties)
                {
                    if ($itemProperty.Name -like $property)
                    {
                        $output | add-member -MemberType “NoteProperty” -Name $itemProperty.Name -Value $itemProperty.Value
                    }
                }
            }
        }
    }
}

   
function WriteJoinObjectOutput($leftItem, $rightItem, $leftProperties, $rightProperties, $Type)
{
    $output = new-object psobject

    if($Type -eq “AllInRight”)
    {
        # This mix of rightItem with LeftProperties and vice versa is due to
        # the switch of Left and Right arguments for AllInRight
        AddItemProperties $rightItem $leftProperties $output
        AddItemProperties $leftItem $rightProperties $output
    }
    else
    {
        AddItemProperties $leftItem $leftProperties $output
        AddItemProperties $rightItem $rightProperties $output
    }
    $output
}

<#
.Synopsis
   Joins two lists of objects
.DESCRIPTION
   Joins two lists of objects
.EXAMPLE
   Join-Object $a $b “Id” (“Name”,”Salary”)
#>
function Join-Object
{
    [CmdletBinding()]
    [OutputType([int])]
    Param
    (
        # List to join with $Right
        [Parameter(Mandatory=$true,
                   Position=0)]
        [object[]]
        $Left,

        # List to join with $Left
        [Parameter(Mandatory=$true,
                   Position=1)]
        [object[]]
        $Right,

        # Condition in which an item in the left matches an item in the right
        # typically something like: {$args[0].Id -eq $args[1].Id}
        [Parameter(Mandatory=$true,
                   Position=2)]
        [scriptblock]
        $Where,

        # Properties from $Left we want in the output.
        # Each property can:
        # – Be a plain property name like “Name”
        # – Contain wildcards like “*”
        # – Be a hashtable like @{Name=”Product Name”;Expression={$_.Name}}. Name is the output property name
        #   and Expression is the property value. The same syntax is available in select-object and it is
        #   important for join-object because joined lists could have a property with the same name
        [Parameter(Mandatory=$true,
                   Position=3)]
        [object[]]
        $LeftProperties,

        # Properties from $Right we want in the output.
        # Like LeftProperties, each can be a plain name, wildcard or hashtable. See the LeftProperties comments.
        [Parameter(Mandatory=$true,
                   Position=4)]
        [object[]]
        $RightProperties,

        # Type of join.
        #   AllInLeft will have all elements from Left at least once in the output, and might appear more than once
        # if the where clause is true for more than one element in right, Left elements with matches in Right are
        # preceded by elements with no matches. This is equivalent to an outer left join (or simply left join)
        # SQL statement.
        #  AllInRight is similar to AllInLeft.
        #  OnlyIfInBoth will cause all elements from Left to be placed in the output, only if there is at least one
        # match in Right. This is equivalent to a SQL inner join (or simply join) statement.
        #  AllInBoth will have all entries in right and left in the output. Specifically, it will have all entries
        # in right with at least one match in left, followed by all entries in Right with no matches in left,
        # followed by all entries in Left with no matches in Right.This is equivallent to a SQL full join.
        [Parameter(Mandatory=$false,
                   Position=5)]
        [ValidateSet(“AllInLeft”,”OnlyIfInBoth”,”AllInBoth”, “AllInRight”)]
        [string]
        $Type=”OnlyIfInBoth”
    )

    Begin
    {
        # a list of the matches in right for each object in left
        $leftMatchesInRight = new-object System.Collections.ArrayList

        # the count for all matches 
        $rightMatchesCount = New-Object “object[]” $Right.Count

        for($i=0;$i -lt $Right.Count;$i++)
        {
            $rightMatchesCount[$i]=0
        }
    }

    Process
    {
        if($Type -eq “AllInRight”)
        {
            # for AllInRight we just switch Left and Right
            $aux = $Left
            $Left = $Right
            $Right = $aux
        }

        # go over items in $Left and produce the list of matches
        foreach($leftItem in $Left)
        {
            $leftItemMatchesInRight = new-object System.Collections.ArrayList
            $null = $leftMatchesInRight.Add($leftItemMatchesInRight)

            for($i=0; $i -lt $right.Count;$i++)
            {
                $rightItem=$right[$i]

                if($Type -eq “AllInRight”)
                {
                    # For AllInRight, we want $args[0] to refer to the left and $args[1] to refer to right,
                    # but since we switched left and right, we have to switch the where arguments
                    $whereLeft = $rightItem
                    $whereRight = $leftItem
                }
                else
                {
                    $whereLeft = $leftItem
                    $whereRight = $rightItem
                }

                if(Invoke-Command -ScriptBlock $where -ArgumentList $whereLeft,$whereRight)
                {
                    $null = $leftItemMatchesInRight.Add($rightItem)
                    $rightMatchesCount[$i]++
                }
           
            }
        }

        # go over the list of matches and produce output
        for($i=0; $i -lt $left.Count;$i++)
        {
            $leftItemMatchesInRight=$leftMatchesInRight[$i]
            $leftItem=$left[$i]
                              
            if($leftItemMatchesInRight.Count -eq 0)
            {
                if($Type -ne “OnlyIfInBoth”)
                {
                    WriteJoinObjectOutput $leftItem  $null  $LeftProperties  $RightProperties $Type
                }

                continue
            }

            foreach($leftItemMatchInRight in $leftItemMatchesInRight)
            {
                WriteJoinObjectOutput $leftItem $leftItemMatchInRight  $LeftProperties  $RightProperties $Type
            }
        }
    }

    End
    {
        #produce final output for members of right with no matches for the AllInBoth option
        if($Type -eq “AllInBoth”)
        {
            for($i=0; $i -lt $right.Count;$i++)
            {
                $rightMatchCount=$rightMatchesCount[$i]
                if($rightMatchCount -eq 0)
                {
                    $rightItem=$Right[$i]
                    WriteJoinObjectOutput $null $rightItem $LeftProperties $RightProperties $Type
                }
            }
        }
    }
}



Export-ModuleMember -Function Join-Object
Export-ModuleMember -Function Get-PartitionAccessPath
Export-ModuleMember -Function Get-DBFileDiskMapping

<#
Find out if Other databases are on cloned volume
$x|%{
    #Write-Host $_.databasename -ForegroundColor Green
    if($databases.DatabaseName -contains $_.DatabaseName){
       "{0},{1}" -f $_.disknumber,$_.databasename  
        ($x|Where DiskNumber -eq $_.disknumber).DatabaseName|%{
            if($_ -notin $databases.databasename){
                Write-Warning "$_ is not in the list of databases to clone.`n`t`tIt may be sharing a disk with a cloned database.`n`t`tMove the database to another disk that is not being cloned"
            }
        }
               
    }

}



#>