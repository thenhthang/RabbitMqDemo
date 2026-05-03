namespace WorkerService2
{
    public class Worker : BasePaymentWorker
    {
        private readonly ILogger<Worker> _logger;
        // Truyền tên Queue và Routing Key tương ứng vào Base
        public Worker(ILogger<Worker> loger) : base(
            queueName: "paymenttest",
            routingKey: "test2")
        {
            _logger = loger;
        }

        protected override Task ProcessMessageAsync(string message)
        {
            _logger.LogInformation($"Dang xy ly tin nhan {message}");
            // Thêm logic gọi Database, cập nhật tồn kho ở đây
            return Task.CompletedTask;
        }
    }
}
