$LogPath = '..\..\untracked\mpi_profile_housekeeping_logs'

$Ref = Get-ChildItem '\\uka-sr-p7syb-10\purge_logs' -Filter *.csv
$Ref += Get-ChildItem '\\uka-sr-p7syb-10\purge_logs' -Filter *.log | ? {
    (Get-Content $_.FullName) -Join ' ' -Match 'Finished'
  }
$Diff = Get-ChildItem $LogPath

Compare-Object -ReferenceObject $Ref -DifferenceObject $Diff -PassThru | % {
    Copy-Item -Destination $LogPath -Path $_.FullName
}
$result = @()
$id = 0
Get-ChildItem -Path $LogPath -Filter *.log | % {

    $file = @(Get-Content $_.FullName)
    $id += 1
    $recs = 0
    $file | % {
        if($_ -Match '(?<=Processing )\d+') {
            $recs += [Int]($matches[0])
        }
    }

    $s = ($file[0][0..18] -Join '')
    $e = ($file[-1][0..18] -Join '')

    $result += New-Object PSObject -Property @{
        Run=$id
        Day=(([datetime]$s).Date).ToString()
        Start=$s
        End=$e
        Duration=New-TimeSpan -Start $s -End $e
        'Records Purged'=$recs
    }
}


$result += $result | ForEach-Object `
  -Begin {$tot = 0; $tdr = New-TimeSpan} `
  -Process {$tot += $_.'Records Purged'; $tdr += $_.Duration} `
  -End {New-Object PSObject -Property @{
        Run='Tot'
        Day='Tot'
        Start=''
        End=''
        Duration=$tdr
        'Records Purged'=$tot
    }
  }

$report = ($result | Group-Object Day | ft `
    @{
        Label = 'Date'
        Expression = {Get-Date $_.Name -format d}
    }, `
    @{
        Label = 'Runs'
        Expression = {
            if($_.Name -ne 'Tot') {$_.Group.Count}
        }
    }, `
    @{
        Label = 'Duration'
        Expression = {
            New-Timespan -Seconds (($_.Group | % {$_.Duration.TotalSeconds}) | Measure-Object -Sum).Sum
        }
        Align = 'Right'
    }, `
    @{
        Label = 'Records Purged'
        Expression = {
            "{0:N0}" -f ($_.Group | Measure-object 'Records Purged' -Sum).Sum
        }
        Align = 'Right'
    }, `
    @{
        Label = 'Avg Processing Time (Secs)'
        Expression = {
            $rp = ($_.Group | Measure-object 'Records Purged' -Sum).Sum
            $s = (($_.Group | % {$_.Duration.TotalSeconds}) | Measure-Object -Sum).Sum
            "{0:N4}" -f ($s / $rp)
        }
        Align = 'Right'
    } -Autosize)

$report

#$result | ft -AutoSize

$head = @"
<style>
  td{
    border-width: 1px;
    padding: 0px;
    border-style: solid;
    border-color: black;
    background-color:palegoldenrod;
    whitespace: nowrap;
  }
</style>
"@
$postc =  @"
<style>
  td{
    background-color:white;
  }
</style>
"@

[string]$html = $report | ConvertTo-Html -Head $head #-PostContent $postc

$html = $report | ft -AutoSize | Out-String

$html = $report | Out-String

$html = @"
<pre>
$html
</pre>
"@

Send-MailMessage  `
    -SmtpServer 'APAMailRelay.michaelpage.local' `
    -Subject 'Purge Stats' `
    -To @('davesexton@michaelpage.com', 'davesexton1@gmail.com') `
    -Body $html `
    -From 'davesexton@michaelpage.com' `
    -BodyAsHtml
