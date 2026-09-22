using System;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace NodeMeshConsole
{
    internal static class NetworkAddressProvider
    {
        public static IPAddress GetActiveIPv4Address()
        {
            var candidates = NetworkInterface
                .GetAllNetworkInterfaces()
                .Where(networkInterface =>
                    networkInterface.OperationalStatus == OperationalStatus.Up &&
                    networkInterface.NetworkInterfaceType != NetworkInterfaceType.Loopback &&
                    networkInterface.NetworkInterfaceType != NetworkInterfaceType.Tunnel)
                .SelectMany(networkInterface => networkInterface.GetIPProperties().UnicastAddresses)
                .Where(address => address.Address.AddressFamily == AddressFamily.InterNetwork)
                .Select(address => address.Address)
                .Where(address => !IPAddress.IsLoopback(address))
                .OrderBy(address => IsLinkLocal(address) ? 1 : 0)
                .ToList();

            if (candidates.Count > 0)
            {
                return candidates[0];
            }

            var fallback = Dns.GetHostEntry(Dns.GetHostName())
                .AddressList
                .FirstOrDefault(address => address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(address));

            if (fallback != null)
            {
                return fallback;
            }

            return IPAddress.Loopback;
        }

        private static bool IsLinkLocal(IPAddress address)
        {
            var bytes = address.GetAddressBytes();
            return bytes.Length == 4 && bytes[0] == 169 && bytes[1] == 254;
        }
    }
}
