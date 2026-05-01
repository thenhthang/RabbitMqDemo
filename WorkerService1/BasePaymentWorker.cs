using Microsoft.Extensions.Hosting;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using WorkerService1;

public abstract class BasePaymentWorker : BackgroundService
{
    private readonly string _queueName;
    private readonly string _routingKey;
    private IConnection? _connection;
    private IChannel? _channel;
    // Tên của Exchange và Queue dành cho tin nhắn lỗi
    private const string DlxName = "dlx_payment_exchange";
    private const string DlqName = "dlq_payment_queue";
    protected BasePaymentWorker(string queueName, string routingKey)
    {
        _queueName = queueName;
        _routingKey = routingKey;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory { HostName = "mac.lan", UserName="guest",Password="guest", AutomaticRecoveryEnabled = true };
        _connection = await factory.CreateConnectionAsync(stoppingToken);
        _channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);
        // --- 1. KHAI BÁO CƠ CHẾ DEAD LETTER (DLX & DLQ) ---
        await _channel.ExchangeDeclareAsync(DlxName, ExchangeType.Direct, durable: true, cancellationToken: stoppingToken);
        var dlqArgs = new Dictionary<string, object?> { { "x-queue-type", "quorum" } };
        await _channel.QueueDeclareAsync(DlqName, durable: true, exclusive: false, autoDelete: false, arguments: dlqArgs, cancellationToken: stoppingToken);
        await _channel.QueueBindAsync(DlqName, DlxName, _routingKey, cancellationToken: stoppingToken);
        //********************************************

        // 1. Khai báo Exchange (phòng trường hợp API chưa chạy)
        await _channel.ExchangeDeclareAsync("payment_exchange", ExchangeType.Direct, durable: true, cancellationToken: stoppingToken);

        // 2. Khai báo Queue (Sử dụng Quorum queue cho an toàn)
        var args = new Dictionary<string, object?> {
            { "x-queue-type", "quorum" } ,
            { "x-dead-letter-exchange", DlxName }, // Khi tin nhắn bị từ chối, gửi vào đây
            { "x-dead-letter-routing-key", _routingKey } // Giữ nguyên routing key cũ
        };
        await _channel.QueueDeclareAsync(_queueName, durable: true, exclusive: false, autoDelete: false, arguments: args, cancellationToken: stoppingToken);

        // 3. Liên kết Queue với Exchange thông qua Routing Key
        await _channel.QueueBindAsync(_queueName, "payment_exchange", _routingKey, cancellationToken: stoppingToken);
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
                    Console.WriteLine($"[CẢNH BÁO] {_queueName} lỗi. Thử lại lần {attempt} sau {timeSpan.TotalSeconds}s. Lỗi: {exception.Message}");
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
                Console.WriteLine($"[LỖI] Xử lý thất bại tại {_queueName}. Lỗi: {ex.Message}. Đẩy vào DLQ...");

                // Thất bại: Từ chối tin nhắn (Nack) và KHÔNG đưa lại vào hàng đợi cũ (requeue: false).
                // Nack với requeue: false để đẩy vào DLX
                await _channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: stoppingToken);
            }
        };

        await _channel.BasicConsumeAsync(_queueName, autoAck: false, consumer: consumer, cancellationToken: stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    // Class con bắt buộc phải ghi đè hàm này
    protected abstract Task ProcessMessageAsync(string message);

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_channel != null) await _channel.CloseAsync(cancellationToken);
        if (_connection != null) await _connection.CloseAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }
}