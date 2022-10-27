 $TypeMappings = @{
    "keyword" = @("constant_keyword", "wildcard");
    "constant_keyword" = @("keyword", "wildcard");
    "wildcard" = @("constant_keyword", "keyword");
    "long" = @("keyword")
    "double" = @("keyword")
    "text" = @("constant_keyword", "keyword", "wildcard");
};

# THIS FUNCTION JUST MAKES IT EASY TO MAKE A PSCREDENTIALS OBJECT
function Get-ESCredentials{
    Param (
        [Parameter (Mandatory = $true )] [String]$User,
        [Parameter (Mandatory = $true )] [String]$Password
    )
    return New-Object System.Management.Automation.PSCredential($User, (ConvertTo-SecureString -String $Password -AsPlainText -Force))
}

# THIS FUNCTION FETCHES THE INDEX DEFINITION FROM ELASTICSEARCH
function Get-ESIndex{
    Param (
        [Parameter (Mandatory = $true )] [String]$ElasticsearchUri,
        [Parameter (Mandatory = $true )] [PSCredential]$Credential,
        [Parameter (Mandatory = $true )] [String]$IndexName
    )
    $response = Invoke-WebRequest -Method "GET" -Uri $ElasticsearchUri/$IndexName -Credential $Credential
    return ($response.Content | ConvertFrom-Json).$IndexName
}

# THIS FUNCTION EXAMINES AND ENHANCES THE EXISTING INDEX MAPPINGS WHILE GENERATING DATATABLE ROWS TO MAKE IT EASIER TO EXPLAIN TO THE USER WHAT WE DID
function Iterate-ESIndexMappings{
    Param (
        [Parameter (Mandatory = $true )] $Properties,
        [Parameter (Mandatory = $true )] [System.Data.DataTable]$ResultTable,
        [Parameter (Mandatory = $true )] [System.Collections.Specialized.OrderedDictionary]$FieldMap,
        [Parameter (Mandatory = $false)] [String]$Parent
    )
    # let's iterate through every property in the mappings!
    foreach ($property in $Properties) {
        $name = $Parent + $property.Name
        If ($property.Value.PSobject.Properties.Name.Contains("properties")) {
            # this isn't actually a field in its own right, it has children fields, so we shall RECURSE!!! mwa ha ha ha
            Iterate-ESIndexMappings -Properties ($property.Value.properties.PSObject.Properties) -ResultTable $ResultTable -Parent ($name + ".") -FieldMap $FieldMap > $null
        } Else {
            # This is an actual field, so we need a table row to show the user what we did with this field...
            $row = $ResultTable.NewRow()
            $row["Field Name"] = $name
            $row["Old Datatype"] = $property.Value.type
            If ($FieldMap.Contains($name)) {
                $row["New Datatype"] = $FieldMap[$name]
                If ($FieldMap[$name] -ne $property.Value.type) {
                    # OK, at this point we have some change. So lets flag that in the table...
                    $row["Status"] = "change"
                    # ...and change the mapping values in the index object...
                    $property.Value.type = $FieldMap[$name]
                    If ($FieldMap[$name] -ne "text") {
                        # and if we're not a text field we do not need these fields, they now serve no purpose
                        If ($property.Value.PSobject.Properties.Name.Contains("fields")) {
                            $property.Value.PSobject.Properties.Remove("fields")
                        }
                        If ($property.Value.PSobject.Properties.Name.Contains("analyzer")) {
                            $property.Value.PSobject.Properties.Remove("analyzer")
                        }
                    }
                } Else {
                    # nothing changes, so lets flag that in the table.
                    $row["Status"] = "no change"
                }
            } ElseIf ($TypeMappings.ContainsKey($property.Value.type)) {
                # uh oh! Somebody forgot to map this field, we'll flag that in the table and let it error later.
                $row["Status"] = "unmapped"
            } Else {
                # Oh, this is a datatype that we can't re-map, so that's cool. Keep it as is.
                $row["New Datatype"] = $property.Value.type
                $row["Status"] = "no change"
            }
            $ResultTable.Rows.Add($row) > $null
        }
    }
}

# THIS FUNCTION GETS THE NEW FIELDMAPPINGS FOR THE INDEX, BASED OFF THE EXISTING INDEX MAPPINGS AND THE MAPPINGS SPECIFIED IN THE CSV FILE
function Get-ESIndexMappings{
    Param (
        [Parameter (Mandatory = $true )] $Index,
        [Parameter (Mandatory = $true )] [System.Collections.Specialized.OrderedDictionary]$FieldMap
    )
    $resultTable = New-Object System.Data.DataTable
    $resultTable.Columns.Add("Field Name") > $null
    $resultTable.Columns.Add("Old Datatype") > $null
    $resultTable.Columns.Add("New Datatype") > $null
    $resultTable.Columns.Add("Status") > $null

    Iterate-ESIndexMappings -Properties ($Index.mappings.properties.psobject.Properties) -ResultTable $resultTable -FieldMap $FieldMap > $null
    return ,$resultTable
}

# THIS FUNCTION REPLACES THE EXISTING INDEX WITH THE ENHANCED ONE BY DELETING AND RECREATING
function Put-ESIndex {
    Param (
        [Parameter (Mandatory = $true )] [String]$ElasticsearchUri,
        [Parameter (Mandatory = $true )] [PSCredential]$Credential,
        [Parameter (Mandatory = $true )] [String]$IndexName,
        [Parameter (Mandatory = $true )] $IndexData
    )
    # Clean up some index settings that will disrupt the ability to create a new index... there is a more elegant way to implement this, but this works.
    If ($IndexData.settings.index.PSObject.Properties.Name.Contains("creation_date")) {
        $IndexData.settings.index.PSObject.Properties.Remove("creation_date")
    }
    If ($IndexData.settings.index.PSObject.Properties.Name.Contains("provided_name")) {
        $IndexData.settings.index.PSObject.Properties.Remove("provided_name")
    }
    If ($IndexData.settings.index.PSObject.Properties.Name.Contains("resize")) {
        $IndexData.settings.index.PSObject.Properties.Remove("resize")
    }
    If ($IndexData.settings.index.PSObject.Properties.Name.Contains("uuid")) {
        $IndexData.settings.index.PSObject.Properties.Remove("uuid")
    }
    If ($IndexData.settings.index.PSObject.Properties.Name.Contains("version")) {
        $IndexData.settings.index.PSObject.Properties.Remove("version")
    }
    If ($IndexData.settings.index.routing.allocation.PSObject.Properties.Name.Contains("initial_recovery")) {
        $IndexData.settings.index.routing.allocation.PSObject.Properties.Remove("initial_recovery")
    }
    # DELETE THE EXISTING INDEX - YES, THIS WILL LOSE ALL DATA
    If((Invoke-RestMethod -Method Delete -Uri $ElasticsearchUri/$IndexName -Credential $Credential).acknowledged) {
        Write-Host -ForegroundColor Green "Deleted Existing Index"
    }
    # CREATE A NEW INDEX WITH THE ENHANCED DATA TYPES
    If((Invoke-RestMethod -Method Put -Uri $ElasticsearchUri/$IndexName -Credential $Credential -ContentType "application/json" -Body (ConvertTo-Json -InputObject $IndexData -Depth 100)).acknowledged) {
        Write-Host -ForegroundColor Green "Created Enhanced Index"
    }
}

# THIS FUNCTION READS THE CSV FILE OF MAPPINGS ([field name],[new data type]) AND PUTS THEM INTO AN ORDERED DICTIONARY
function Read-MappingCSV {
    Param (
        [Parameter (Mandatory = $true )] [String]$CSVPath
    )
    $data = [ordered]@{}
    foreach ($line in (Get-Content -Path $CSVPath)) {
        If($line.IndexOf(',') -gt 0) {
            $name, $value = $line -split '\s*,\s*', 2
            $data[$name] = $value
        }
    }
    return $data
}

# THIS FUNCTION BRINGS IT ALL TOGETHER
function Enhance-ESIndex {
    Param (
        [Parameter (Mandatory = $true )] [String]$EsUri, #The URI of the Elasticsearch instance
        [Parameter (Mandatory = $true )] [String]$EsUser, #The Username of the Elasticsearch user to enhance the index
        [Parameter (Mandatory = $true )] [String]$EsPassword, #The Password of the Elasticsearch user to enhance the index
        [Parameter (Mandatory = $true )] [String]$EsIndex, #The name of the Index to enhance (typically ES_xx)
        [Parameter (Mandatory = $true )] [String]$EsMappingCSV #The path to the CSV file containing field mappings
    )
    $cred = Get-ESCredentials -User $EsUser -Password $EsPassword
    $index = Get-ESIndex -ElasticsearchUri $EsUri -Credential $EsCred -IndexName $EsIndex
    $fieldMap = Read-MappingCSV -CSVPath $EsMappingCSV
    $resultTable = Get-ESIndexMappings -Index $index -FieldMap $fieldMap
    Format-Table -InputObject $resultTable
    If($resultTable.Select("Status = 'unmapped'").Count -gt 0) {
        Write-Host -ForegroundColor Red "Unmapped fields in this index. Unable to proceed."
        Format-Table -InputObject $resultTable.Select("Status = 'unmapped'")
    } ElseIf ((Read-Host "Would you like to proceed? THIS WILL DELETE ALL DATA IN THE INDEX (Y/N)").ToUpper() -eq 'Y') {
        Put-ESIndex -ElasticsearchUri $EsUri -Credential $EsCred -IndexName $EsIndex -IndexData $index
        Write-Host -ForegroundColor Green "Success"
    } Else {
        Write-Host -ForegroundColor Yellow "Aborted"
    }
} 
