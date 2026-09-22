using System;
using System.Collections.Generic;
using System.Linq;
using K4os.Compression.LZ4;
using MessagePack;

namespace NodeMeshConsole
{
    internal static class PayloadCodec
    {
        private const int CompressThreshold = 1024;

        public static byte[] Encode(TransportPayload payload)
        {
            return MessagePackSerializer.Serialize(payload);
        }

        public static TransportPayload Decode(byte[] bytes)
        {
            return MessagePackSerializer.Deserialize<TransportPayload>(bytes);
        }

        public static byte[] EncodeForTransport(TransportPayload payload, out bool isCompressed)
        {
            var payloadBytes = Encode(payload);
            if (payloadBytes.Length <= CompressThreshold)
            {
                isCompressed = false;
                return payloadBytes;
            }

            var compressedBytes = LZ4Pickler.Pickle(payloadBytes);
            if (compressedBytes.Length >= payloadBytes.Length)
            {
                isCompressed = false;
                return payloadBytes;
            }

            isCompressed = true;
            return compressedBytes;
        }

        public static TransportPayload DecodeTransportBytes(byte[] bytes, bool isCompressed)
        {
            return Decode(isCompressed ? LZ4Pickler.Unpickle(bytes) : bytes);
        }

        public static TransportPayload FromDouble(double value)
        {
            return new TransportPayload
            {
                Kind = PayloadKind.Double,
                DoubleValue = value
            };
        }

        public static TransportPayload FromDoubleArray(IEnumerable<double> values)
        {
            return new TransportPayload
            {
                Kind = PayloadKind.DoubleArray,
                DoubleArrayValue = values.ToArray()
            };
        }

        public static TransportPayload FromGeneratedBuffer(int length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException("length", "Buffer length must be non-negative.");
            }

            var buffer = new byte[length];
            for (var index = 0; index < buffer.Length; index++)
            {
                buffer[index] = (byte)(index % 256);
            }

            return new TransportPayload
            {
                Kind = PayloadKind.GeneratedBuffer,
                BufferValue = buffer
            };
        }

        public static TransportPayload FromKeyValueSet(IDictionary<string, double> values)
        {
            return new TransportPayload
            {
                Kind = PayloadKind.KeyValueSet,
                KeyValueSetValue = values
                    .OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase)
                    .Select(pair => new KeyValuePairValue
                    {
                        Key = pair.Key,
                        Value = pair.Value
                    })
                    .ToArray()
            };
        }

        public static string Describe(TransportPayload payload)
        {
            switch (payload.Kind)
            {
                case PayloadKind.Double:
                    return payload.DoubleValue.ToString("G");
                case PayloadKind.DoubleArray:
                    return "[" + string.Join(", ", payload.DoubleArrayValue ?? new double[0]) + "]";
                case PayloadKind.GeneratedBuffer:
                    return string.Format("byte[{0}]", payload.BufferValue == null ? 0 : payload.BufferValue.Length);
                case PayloadKind.KeyValueSet:
                    return payload.KeyValueSetValue == null
                        ? "{}"
                        : "{" + string.Join(", ", payload.KeyValueSetValue.Select(pair => pair.Key + "=" + pair.Value.ToString("G"))) + "}";
                default:
                    return "<unknown>";
            }
        }
    }
}
