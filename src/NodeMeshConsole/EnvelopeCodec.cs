using System;
using MessagePack;

namespace NodeMeshConsole
{
    internal static class EnvelopeCodec
    {
        public static byte[] Encode(TransportEnvelope envelope)
        {
            return MessagePackSerializer.Serialize(envelope);
        }

        public static TransportEnvelope Decode(byte[] bytes)
        {
            return MessagePackSerializer.Deserialize<TransportEnvelope>(bytes);
        }

        public static TransportEnvelope CreateData(string senderNode, string token, TransportPayload payload)
        {
            return new TransportEnvelope
            {
                MessageId = Guid.NewGuid().ToString("N"),
                SenderNode = senderNode,
                Token = token,
                Kind = EnvelopeKind.Data,
                PayloadBytes = PayloadCodec.Encode(payload),
                CreatedUtcTicks = DateTime.UtcNow.Ticks
            };
        }

        public static TransportEnvelope CreateAck(string senderNode, string acknowledgedMessageId)
        {
            return new TransportEnvelope
            {
                MessageId = Guid.NewGuid().ToString("N"),
                SenderNode = senderNode,
                Kind = EnvelopeKind.Ack,
                CreatedUtcTicks = DateTime.UtcNow.Ticks,
                AcknowledgedMessageId = acknowledgedMessageId
            };
        }
    }
}
