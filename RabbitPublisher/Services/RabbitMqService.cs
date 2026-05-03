
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitPublisher.Models;
using System.Text;
namespace RabbitPublisher.Services
{
    public class RabbitMqService : IRabbitMqService, IAsyncDisposable
    {
        private readonly RabbitMqSettings _rabbitMqSettings;
        private  IChannel? _channel;
        private  IConnection? _connection;
        private readonly SemaphoreSlim _lock = new(1, 1);

        public RabbitMqService(IOptions<RabbitMqSettings> rabbitMqSettings) 
        { 
            _rabbitMqSettings = rabbitMqSettings.Value;
        }
        // Phương thức khởi tạo kết nối bất đồng bộ
        private async Task EnsureChannelAsync()
        {
            if (_connection?.IsOpen == true && _channel?.IsOpen == true)
                return;

            await _lock.WaitAsync();
            try
            {
                if (_connection?.IsOpen == true && _channel?.IsOpen == true) return;
                // Close old resources nếu còn tồn tại
                if (_channel != null)
                {
                    await _channel.CloseAsync();
                    await _channel.DisposeAsync();
                    _channel = null;
                }
                // Tạo connection mới
                if (_connection != null)
                {
                    await _connection.CloseAsync();
                    await _connection.DisposeAsync();
                    _connection = null;
                }
                var factory = new ConnectionFactory
                {
                    HostName = _rabbitMqSettings.HostName,
                    UserName = _rabbitMqSettings.UserName,
                    Password = _rabbitMqSettings.Password,
                    ClientProvidedName = "Publisher",
                    AutomaticRecoveryEnabled = true,      // ← rất quan trọng
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
                };

                // Tạo kết nối và channel async
                _connection = await factory.CreateConnectionAsync();
                _channel = await _connection.CreateChannelAsync();

                // 2. Khai báo Exchange (đảm bảo Exchange tồn tại)
                await _channel.ExchangeDeclareAsync(
                    exchange: _rabbitMqSettings.ExchangeName,
                    type: ExchangeType.Direct,
                    durable: true,
                    autoDelete: false);
            }
            finally {  _lock.Release(); }
        }
        public async Task PublishAsync(string message, string routingKey)
        {
            await EnsureChannelAsync();
            var body = Encoding.UTF8.GetBytes(message);
            var properties = new BasicProperties { DeliveryMode = DeliveryModes.Persistent };
            // BasicPublishAsync trong bản 7.x
            await _channel!.BasicPublishAsync(
                exchange: _rabbitMqSettings.ExchangeName,
                routingKey: routingKey,
                mandatory: false,
                basicProperties: properties, // Tạo mới properties nếu cần
                body: body);
        }
        // Giải phóng tài nguyên đúng cách cho bản 7.x
        public async ValueTask DisposeAsync()
        {
            await _lock.WaitAsync(); // tránh race khi dispose trong lúc publish
            try
            {
                if (_channel != null)
                {
                    await _channel.CloseAsync();
                    await _channel.DisposeAsync();
                    _channel = null;
                }

                if (_connection != null)
                {
                    await _connection.CloseAsync();
                    await _connection.DisposeAsync();
                    _connection = null;
                }
            }
            finally
            {
                _lock.Release();
                _lock.Dispose();
            }
        }
    }
}
