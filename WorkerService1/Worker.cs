
using Microsoft.Extensions.Options;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace WorkerService1
{
    public class Worker : BaseWorker
    {
        private readonly ILogger<Worker> _logger;
        // Truyền tên Queue và Routing Key tương ứng vào Base
        public Worker(ILogger<Worker> loger,
            IOptions<RabbitMqSettings> rabbitMqSertting) : base(
            rabbitMqSettings: rabbitMqSertting,
            logger:loger)
        { 
            _logger = loger;
        }

        protected override Task ProcessMessageAsync(string message)
        {
            try
            {
                //_logger.LogInformation("Http {Method} Time {Time} Message {Msg}", "POST", DateTime.Now.TimeOfDay, message);
                var data = JsonSerializer.Deserialize<int>(message);
                Task.Delay(1000);          
                // Thêm logic ở đây
                return Task.CompletedTask;
            }
            catch (Exception) 
            {
                throw;
            }
        }
    }
}
