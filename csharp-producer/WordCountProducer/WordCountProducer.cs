using Bogus;

using Confluent.Kafka;

class WordCountProducer
{
    public static void Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        // Teste de conexão com Kafka
        try
        {
            using var producer = new ProducerBuilder<Null, string>(config).Build();
            var testMessage = new Message<Null, string> { Value = "Teste de conexão com Kafka" };
            producer.Produce("input-topic", testMessage);
            producer.Flush(TimeSpan.FromSeconds(10)); // Aguarda a confirmação do envio
            Console.WriteLine("Kafka está ativo e disponível.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao conectar ao Kafka: {ex.Message}");
            return; // Sai do programa se não for possível conectar ao Kafka
        }

        // Geração de frases aleatórias com a biblioteca Bogus
        var faker = new Faker();

        using var messageProducer = new ProducerBuilder<Null, string>(config).Build();

        while (true)
        {
            // Gera uma frase aleatória
            var randomSentence = faker.Lorem.Sentence();
            messageProducer.Produce("input-topic", new Message<Null, string> { Value = randomSentence });
            Console.WriteLine($"Frase enviada: {randomSentence}");

            Thread.Sleep(1000); // Espera 1 segundo antes de gerar a próxima frase
        }
    }
}
