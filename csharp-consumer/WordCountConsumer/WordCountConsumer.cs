using Confluent.Kafka;

class WordCountConsumer
{
    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            GroupId = "wordcount-consumer-group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, long>(config)
                             .SetValueDeserializer(Deserializers.Int64)
                             .Build();

        consumer.Subscribe("output-topic");

        // Teste de conexão com o Kafka
        try
        {
            var testConsume = consumer.Consume(TimeSpan.FromSeconds(10));
            if (testConsume == null)
            {
                Console.WriteLine("Nenhuma mensagem recebida durante o teste. Kafka pode não estar disponível.");
            }
            else
            {
                Console.WriteLine("Kafka está ativo e disponível.");
            }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Erro ao conectar ao Kafka: {e.Error.Reason}");
            return; // Sai do programa se não for possível conectar ao Kafka
        }

        // Inicia o consumo contínuo das mensagens
        try
        {
            while (true)
            {
                var consumeResult = consumer.Consume(CancellationToken.None);
                Console.WriteLine($"Word: {consumeResult.Message.Key}, Count: {consumeResult.Message.Value}");
            }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Erro ao consumir mensagens: {e.Error.Reason}");
        }
    }
}
