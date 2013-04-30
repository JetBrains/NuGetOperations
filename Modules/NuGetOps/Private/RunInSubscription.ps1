function RunInSubscription($name, [scriptblock]$scriptblock) {
    $oldSub = Get-AzureSubscription | Where-Object { $_.IsDefault }
    if(!$oldSub -or ($oldSub.SubscriptionName -ne $name)) {
        Select-AzureSubscription $name
    }
    try {
        Invoke-Command $scriptblock
    } finally {
        if($oldSub -and ($oldSub.SubscriptionName -ne $name)) {
            Select-AzureSubscription $oldSub.SubscriptionName
        }
    }
}