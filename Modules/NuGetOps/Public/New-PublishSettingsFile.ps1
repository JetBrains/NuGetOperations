function New-PublishSettingsFile {
    param(
        [string]$Name,
        [string]$HomeRealm,
        [string]$SubscriptionsFile)
    $MsftDomainNames = @("REDMOND","FAREAST","NORTHAMERICA","NTDEV")
    if(!$Name) {
        $Name = "Azure-$([Environment]::UserName)-on-$([Environment]::MachineName)-at-$([DateTime]::UtcNow.ToString("yyyy-MM-dd"))-utc"
    }
    $whr = ""
    $subscriptions = $null;
    if($HomeRealm) {
        $whr = "?whr=$HomeRealm"
    } elseif($MsftDomainNames -contains [Environment]::UserDomainName) {
        $whr = "?whr=microsoft.com"
        if(Test-Path "\\nuget\Environments\Subscriptions.xml") {
            $subscriptions = [xml](cat "\\nuget\Environments\Subscriptions.xml")
        }
    }

    # Make a cert
    Write-Host "Generating Certificate..."
    $FileName = "$Name.cer"
    if(Test-Path $FileName) {
        throw "There is already a cert at $FileName. Delete it or move it before running this command"
    }
    makecert -sky exchange -r -n "CN=$Name" -pe -a sha1 -len 2048 -ss My $FileName

    # Get the Thumbprint and find the private key in the store
    $FileName = (Convert-Path $FileName)
    Write-Host "Certificate created. Public Key is at $FileName"
    $cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 $FileName

    $PublishSettingsFileName = [IO.Path]::ChangeExtension($FileName, "publishsettings");
    $CertData = [Convert]::ToBase64String($cert.Export("Pkcs12", [String]::Empty));
  
    $subscriptionsXml = "<Subscription Id=`"-- ID HERE --`" Name=`"-- NAME HERE --`" />";  
    if($subscriptions) {
        $subscriptionsXml = "";
        $subscriptions.subscriptions.subscription | foreach {
            $subscriptionsXml += "<Subscription Id=`"$($_.id)`" Name=`"$($_.name)`" />" + [Environment]::NewLine;
        }
    }
    $xmlTemplate = @"
    <PublishData>
      <PublishProfile
        PublishMethod="AzureServiceManagementAPI"
        Url="https://management.core.windows.net/"
        ManagementCertificate="$CertData">
        $subscriptionsXml
      </PublishProfile>
    </PublishData>
"@
    
    $xmlTemplate | Out-File -FilePath $PublishSettingsFileName -Encoding UTF8

    Start-Process "https://manage.windowsazure.com/$whr#Workspaces/AdminTasks/ListManagementCertificates"
    Write-Host "Now: Go upload $FileName to the Azure Portal. I've just launched your browser for you :)."
    if($subscriptions) {
        Write-Host "Make sure you upload it once for EACH of these subscriptions: "
        $subscriptions.subscriptions.subscription | foreach {
            Write-Host "* $($_.name)"
        }
        Write-Host "Sometimes you need to refresh the browser to see the subscription list on the Upload certificate dialog."
        Write-Host "Once you've done that, type YES and press enter (press Ctrl-C to abort)"
        do {
            $result = Read-Host "Done? ";
        } while($result.ToLowerInvariant() -ne "yes");
        Import-AzurePublishSettingsFile $PublishSettingsFileName

        Write-Host "Awesome! I just imported it into the Azure PowerShell tools. Now either store your Publish Settings file SECURELY for later, or delete it"
        Write-Host "It's here: $PublishSettingsFileName"
        Write-Host "Same with your Certificate"
        Write-Host "It's here: $FileName"
    } else {
        Write-Host "I've written $PublishSettingsFileName but it's NOT READY YET!"
        Write-Host "Once you've uploaded the CER file for all the subscriptions you want to manage, open the publish settings file and find the Subscription element:"
        Write-Host
        Write-Host "<Subscription Id=`"--- ID HERE ---`" Name=`"--- NAME HERE ---`" />"
        Write-Host "Set the ID and Name as per the information in the portal. Feel free to add multiple copies with different ID/Name pairs. Just make sure you've uploaded the Cert to that subscription!"
        Write-Host "Then, save the publishsettings file and use it for Import-AzurePublishSettingsFile (or `"azure account import`" in the NodeJS azure cli)"
    }
}