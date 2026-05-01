
namespace WorkerService1
{
    public class Worker : BasePaymentWorker
    {
        // Truyền tên Queue và Routing Key tương ứng vào Base
        public Worker() : base(
            queueName: "paymenttest",
            routingKey: "test")
        { }

        protected override Task ProcessMessageAsync(string message)
        {
            Console.WriteLine($"Dang xy ly tin nhan {message}");
            // Thêm logic gọi Database, cập nhật tồn kho ở đây
            return Task.CompletedTask;
        }
    }
}
