using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using WorkerService1;

public abstract class BasePaymentWorker : BackgroundService
{
    //private readonly string _queueName;
    //private readonly string _routingKey;
    private IConnection? _connection;
    private IChannel? _channel;
    private readonly ILogger<BasePaymentWorker> _logger;
    private readonly RabbitMqSettings _rabbitMqSettings;
    // Tên của Exchange và Queue dành cho tin nhắn lỗi
    private const string DlxName = "dlx_payment_exchange";
    private const string DlqName = "dlq_payment_queue";

    private readonly SemaphoreSlim _lock = new(1, 1);
    protected BasePaymentWorker(
                                IOptions<RabbitMqSettings> rabbitMqSettings,
                                ILogger<BasePaymentWorker> logger)
    {
        //_queueName = queueName;
        //_routingKey = routingKey;
        _rabbitMqSettings = rabbitMqSettings.Value;
        _logger = logger;
        _logger.LogInformation($"BasePaymentWorker started for queue: {_rabbitMqSettings.QueueName}");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    
    {
        //var factory = new ConnectionFactory {HostName=_rabbitMqSettings.HostName, UserName=_rabbitMqSettings.UserName,Password=_rabbitMqSettings.Password, AutomaticRecoveryEnabled = true, ClientProvidedName=_rabbitMqSettings.ServiceName };
        //_connection = await factory.CreateConnectionAsync(stoppingToken);
        //_channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await EnsureConnectionAndChannelAsync(stoppingToken);
        // --- 1. KHAI BÁO CƠ CHẾ DEAD LETTER (DLX & DLQ) ---
        await _channel!.ExchangeDeclareAsync(exchange: DlxName, type: ExchangeType.Direct, durable: true, autoDelete:false, cancellationToken: stoppingToken);
        var dlqArgs = new Dictionary<string, object?> { { "x-queue-type", "quorum" } };
        await _channel!.QueueDeclareAsync(DlqName, durable: true, exclusive: false, autoDelete: false, arguments: dlqArgs, cancellationToken: stoppingToken);
        await _channel.QueueBindAsync(DlqName, DlxName, _rabbitMqSettings.RoutingKey, cancellationToken: stoppingToken);
        //********************************************

        // 1. Khai báo Exchange (phòng trường hợp API chưa chạy)
        await _channel.ExchangeDeclareAsync( exchange: _rabbitMqSettings.ExchangeName, type: ExchangeType.Direct, durable: true, autoDelete:false, cancellationToken: stoppingToken);

        // 2. Khai báo Queue (Sử dụng Quorum queue cho an toàn)
        var args = new Dictionary<string, object?> {
            { "x-queue-type", "quorum" } ,
            { "x-dead-letter-exchange", DlxName }, // Khi tin nhắn bị từ chối, gửi vào đây
            { "x-dead-letter-routing-key", _rabbitMqSettings.RoutingKey } // Giữ nguyên routing key cũ
        };
        await _channel.QueueDeclareAsync(_rabbitMqSettings.QueueName, durable: true, exclusive: false, autoDelete: false, arguments: args, cancellationToken: stoppingToken);

        // 3. Liên kết Queue với Exchange thông qua Routing Key
        await _channel.QueueBindAsync(_rabbitMqSettings.QueueName,_rabbitMqSettings.ExchangeName, _rabbitMqSettings.RoutingKey, cancellationToken: stoppingToken);
        // 4. kiểm soát số lượng tin nhắn mà consumer được phép nhận trước (prefetch) mà chưa ack (xác nhận).  
        await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false, cancellationToken: stoppingToken);
        // --- 3. ĐỊNH NGHĨA CHÍNH SÁCH RETRY VỚI POLLY ---
        // Chiến lược Exponential Backoff: Thử lại 3 lần. 
        // Thời gian chờ: Lần 1 (2 giây), Lần 2 (4 giây), Lần 3 (8 giây)
        var retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(
                retryCount: 3,
                sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                onRetry: (exception, timeSpan, attempt, context) =>
                {
                    _logger.LogWarning($"[CẢNH BÁO] {_rabbitMqSettings.QueueName} lỗi. Thử lại lần {attempt} sau {timeSpan.TotalSeconds}s. Lỗi: {exception.Message}");
                });
        // 4. Lắng nghe tin nhắn
        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            try
            {
                if (message != null)
                {
                    // Thực thi logic nghiệp vụ dưới sự bảo vệ của Polly
                    await retryPolicy.ExecuteAsync(async () =>
                    {
                        // Nếu hàm này ném lỗi, Polly sẽ tự động catch và retry theo thời gian đã định
                        //Ham xu ly nghiep vu chinh
                        await ProcessMessageAsync(message);
                    });
                }

                // Xác nhận (Ack) thanh cong, RabbitMQ xóa tin nhắn khỏi hàng đợi
                await _channel.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: stoppingToken);
                
            }
            catch (Exception ex) 
            {
                _logger.LogError($"[LỖI] Xử lý thất bại tại {_rabbitMqSettings.QueueName}. Lỗi: {ex.Message}. Đẩy vào DLQ...");

                // Thất bại: Từ chối tin nhắn (Nack) và KHÔNG đưa lại vào hàng đợi cũ (requeue: false).
                // Nack với requeue: false để đẩy vào DLX
                await _channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: stoppingToken);
            }
        };

        await _channel.BasicConsumeAsync(_rabbitMqSettings.QueueName, autoAck: false, consumer: consumer, cancellationToken: stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task EnsureConnectionAndChannelAsync(CancellationToken cancellationToken)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (_connection?.IsOpen == true && _channel?.IsOpen == true)
                return;

            var factory = new ConnectionFactory
            {
                HostName = _rabbitMqSettings.HostName,
                UserName = _rabbitMqSettings.UserName,
                Password = _rabbitMqSettings.Password,
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
                ClientProvidedName = _rabbitMqSettings.ServiceName
            };

            _connection = await factory.CreateConnectionAsync(cancellationToken);
            _channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    // Class con bắt buộc phải ghi đè hàm này
    protected abstract Task ProcessMessageAsync(string message);

    public override async Task StopAsync(CancellationToken cancellationToken)
    {

        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (_channel != null)
            {
                await _channel.CloseAsync(cancellationToken);
                await _channel.DisposeAsync();
                _channel = null;
            }

            if (_connection != null)
            {
                await _connection.CloseAsync(cancellationToken);
                await _connection.DisposeAsync();
                _connection = null;
            }
        }
        finally
        {
            _lock.Release();
            _lock.Dispose();
        }

        await base.StopAsync(cancellationToken);
    }
    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);
    }
}