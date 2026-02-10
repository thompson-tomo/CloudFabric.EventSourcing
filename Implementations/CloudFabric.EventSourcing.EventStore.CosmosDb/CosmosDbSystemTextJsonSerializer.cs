using System.Text.Json;
using Azure.Core.Serialization;
using Microsoft.Azure.Cosmos;

namespace CloudFabric.EventSourcing.EventStore.CosmosDb;

public class CosmosDbSystemTextJsonSerializer : CosmosSerializer
{
    private readonly JsonObjectSerializer _systemTextJsonSerializer;

    public CosmosDbSystemTextJsonSerializer(JsonSerializerOptions jsonSerializerOptions)
    {
        _systemTextJsonSerializer = new JsonObjectSerializer(jsonSerializerOptions);
    }

    public override T FromStream<T>(Stream stream)
    {
        if (typeof(Stream).IsAssignableFrom(typeof(T)))
        {
            return (T)(object)stream;
        }

        using (stream)
        {
            if (stream.CanSeek && stream.Length == 0)
            {
                return default;
            }

            return (T)_systemTextJsonSerializer.Deserialize(stream, typeof(T), default);
        }
    }

    public override Stream ToStream<T>(T input)
    {
        MemoryStream streamPayload = new MemoryStream();

        if (input is JsonDocument inputJsonDocument)
        {
            Utf8JsonWriter writer = new Utf8JsonWriter(streamPayload, new JsonWriterOptions { Indented = false });

            inputJsonDocument.WriteTo(writer);
            writer.Flush();
        }
        else
        {
            _systemTextJsonSerializer.Serialize(streamPayload, input, typeof(T), default);
        }

        streamPayload.Position = 0;
        return streamPayload;
    }
}
