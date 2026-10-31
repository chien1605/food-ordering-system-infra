# Script to load local Docker images into Docker Desktop Kubernetes (containerd runtime)
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Loading Food Ordering System images into Kubernetes node..." -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$kubeletPid = (wsl -d docker-desktop pgrep kubelet).Trim()
if (-not $kubeletPid) {
    Write-Error "Could not find kubelet PID in docker-desktop WSL. Make sure Kubernetes is running in Docker Desktop."
    exit 1
}

$services = @("customer", "order", "payment", "restaurant")

foreach ($s in $services) {
    $img = "com.food.ordering.system/$s.service:1.0-SNAPSHOT"
    $tarPath = "$PSScriptRoot\$s.tar"
    $wslTarPath = "/mnt/host/c/" + ($PSScriptRoot -replace '^[a-zA-Z]:\\', '' -replace '\\', '/') + "/$s.tar"

    Write-Host "[1/3] Saving $img to $s.tar..." -ForegroundColor Yellow
    docker save $img -o $tarPath

    Write-Host "[2/3] Importing $s.tar into containerd k8s.io namespace..." -ForegroundColor Yellow
    wsl -d docker-desktop -e sh -c "cat '$wslTarPath' | nsenter -t $kubeletPid -m -p -- ctr -n k8s.io images import -"

    Write-Host "[3/3] Removing temporary $s.tar..." -ForegroundColor Yellow
    Remove-Item -Force $tarPath
}

Write-Host "`nAll images have been loaded into Docker Desktop Kubernetes successfully!" -ForegroundColor Green
Write-Host "Restarting pods to pick up images..." -ForegroundColor Cyan
kubectl delete pods -l app=customer-deployment,app=order-deployment,app=payment-deployment,app=restaurant-deployment --ignore-not-found=true
