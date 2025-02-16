namespace Promul.Relay.Protocol
{
    public static class NetDataExtensions
    {
        public static RelayControlMessage ReadRelayControlMessage(this CompositeReader reader)
        {
            var rcm = new RelayControlMessage
            {
                Type = (RelayControlMessageType)reader.ReadByte(),
                AuthorClientId = reader.ReadUInt64(),
                JoinCode = reader.ReadString(),
                Data = reader.ReadRemainingBytes()
            };
            return rcm;
        }

        public static void Write(this CompositeWriter writer, RelayControlMessage rcm)
        {
            writer.Write((byte)rcm.Type);
            writer.Write(rcm.AuthorClientId);
            writer.Write(rcm.JoinCode ?? string.Empty);
            writer.Write(rcm.Data);
        }
    }
}