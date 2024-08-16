$filename = "C:\develop\nwdm\data\SOCATv2024\SOCATv2024\data_from_SOCATv2024.txt"
$newfile = "C:\develop\nwdm\data\socat.txt"
$sw = new-object System.Diagnostics.Stopwatch
$sw.Start()
$reader = [io.file]::OpenText($filename)
$linecount = 0

$writer = [io.file]::CreateText($newfile)


try{
    try{
        while($reader.EndOfStream -ne $true) {
            "Reading"
            while( ($reader.EndOfStream -ne $true)){
                $thisline = $reader.ReadLine();
                if($thisline.StartsWith("//") -eq $false){
                    $writer.WriteLine($thisline);
                }
                $linecount++
            }
        }
    } finally {
        $writer.Dispose();
    }
} finally {
    $reader.Dispose();
}
$sw.Stop()


Write-Host "Complete in " $sw.Elapsed.TotalSeconds "seconds"
"ready."
