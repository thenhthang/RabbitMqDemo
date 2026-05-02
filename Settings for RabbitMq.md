### Tạo user, user guest không the truy cập từ bên ngoài host, phải tạo user mới
```
# Create user (if not exists)
rabbitmqctl add_user "myuser" "mypassword"
# Set permissions for the "/" vhost
rabbitmqctl set_permissions -p "/" "myuser" ".*" ".*" ".*"
```

```
rabbitmqctl add_user admin password
rabbitmqctl set_user_tags admin administrator
rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"
```

### Cho phép ping
```
netsh advfirewall firewall add rule name="Allow Ping" protocol=icmpv4:8,any dir=in action=allow
```
### RabbitMq sử dụng các port để giao tiếp
## Run trong Powershell
```
# Danh sách các cổng cần mở theo tài liệu RabbitMQ của bạn
$ports = @(4369, 5672, 5671, 25672, 15672, 61613, 61614, 1883, 8883, 15674, 15675)

# Thêm dải cổng động 35672-35682
$dynamicPorts = "35672-35682"

Write-Host "Dang mo cac cong RabbitMQ tren Firewall..." -ForegroundColor Cyan

# Lệnh mở các cổng đơn lẻ
foreach ($port in $ports) {
    New-NetFirewallRule -DisplayName "RabbitMQ Port $port" -Direction Inbound -LocalPort $port -Protocol TCP -Action Allow -ErrorAction SilentlyContinue
}

# Lệnh mở dải cổng động
New-NetFirewallRule -DisplayName "RabbitMQ Dynamic Ports" -Direction Inbound -LocalPort $dynamicPorts -Protocol TCP -Action Allow -ErrorAction SilentlyContinue

Write-Host "Hoan thanh! Tat ca cac cong da duoc mo." -ForegroundColor Green

```
