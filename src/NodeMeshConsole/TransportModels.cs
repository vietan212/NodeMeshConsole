using System;
using System.Collections.Generic;
using MessagePack;

namespace NodeMeshConsole
{
    internal enum PayloadKind
    {
        Double = 1,
        DoubleArray = 2,
        GeneratedBuffer = 3,
        KeyValueSet = 4
    }

    internal enum EnvelopeKind
    {
        Data = 1,
        Ack = 2
    }

    [MessagePackObject(AllowPrivate = true)]
    internal sealed class KeyValuePairValue
    {
        [Key(0)]
        public string Key { get; set; }

        [Key(1)]
        public double Value { get; set; }
    }

    [MessagePackObject(AllowPrivate = true)]
    internal sealed class TransportPayload
    {
        [Key(0)]
        public PayloadKind Kind { get; set; }

        [Key(1)]
        public double DoubleValue { get; set; }

        [Key(2)]
        public double[] DoubleArrayValue { get; set; }

        [Key(3)]
        public byte[] BufferValue { get; set; }

        [Key(4)]
        public KeyValuePairValue[] KeyValueSetValue { get; set; }
    }

    [MessagePackObject(AllowPrivate = true)]
    internal sealed class TransportEnvelope
    {
        [Key(0)]
        public string MessageId { get; set; }

        [Key(1)]
        public string SenderNode { get; set; }

        [Key(2)]
        public string Token { get; set; }

        [Key(3)]
        public EnvelopeKind Kind { get; set; }

        [Key(4)]
        public byte[] PayloadBytes { get; set; }

        [Key(5)]
        public long CreatedUtcTicks { get; set; }

        [Key(6)]
        public string AcknowledgedMessageId { get; set; }

        [Key(7)]
        public bool IsPayloadCompressed { get; set; }
    }

    [MessagePackObject(AllowPrivate = true)]
    internal sealed class BeaconTransportMessage
    {
        [Key(0)]
        public string NodeId { get; set; }

        [Key(1)]
        public string IpAddress { get; set; }

        [Key(2)]
        public int StreamPort { get; set; }

        [Key(3)]
        public int ReliablePort { get; set; }

        [Key(4)]
        public long SentUtcTicks { get; set; }
    }

    internal sealed class LatestStreamValue
    {
        public string Token { get; set; }

        public string SenderNode { get; set; }

        public DateTimeOffset ReceivedAt { get; set; }

        public TransportPayload Payload { get; set; }
    }

    internal sealed class ReliableTransportMessage
    {
        public string PeerNodeId { get; set; }

        public TransportEnvelope Envelope { get; set; }
    }

    internal sealed class PeerNode
    {
        public string NodeId { get; set; }

        public string IpAddress { get; set; }

        public int StreamPort { get; set; }

        public int ReliablePort { get; set; }

        public DateTimeOffset LastSeenUtc { get; set; }

        public override string ToString()
        {
            return string.Format(
                "{0} @ {1} (PUB {2}, ROUTER {3}, seen {4:O})",
                this.NodeId,
                this.IpAddress,
                this.StreamPort,
                this.ReliablePort,
                this.LastSeenUtc);
        }
    }
}
