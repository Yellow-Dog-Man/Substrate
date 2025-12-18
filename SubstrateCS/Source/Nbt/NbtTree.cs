using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.IO.Compression;
using Substrate.Core;
using System.Linq;

namespace Substrate.Nbt
{
    public enum EndiannessType
    {
        /// <summary> Will read/write all integers and specifiers as big endian </summary>
        BigEndian,
        /// <summary> Will read/write all integers and specifiers as little endian </summary>
        LittleEndian,
    }

    public enum HeaderType
    {
        /// <summary> Will not attempt to read/write any header </summary>
        None,
        /// <summary> Will attempt to read/write the 8-byte level.dat header </summary>
        LevelHeader,
        /// <summary> Will attempt to read/write the 12-byte entity header </summary>
        EntityHeader,
    }

    /// <summary>
    /// Contains the root node of an NBT tree and handles IO of tree nodes.
    /// </summary>
    /// <remarks>
    /// NBT, or Named Byte Tag, is a tree-based data structure for storing most Minecraft data.
    /// NBT_Tree is more of a helper class for NBT trees that handles reading and writing nodes to data streams.
    /// Most of the API takes a TagValue or derived node as the root of the tree, rather than an NBT_Tree object itself.
    /// </remarks>
    public class NbtTree : ICopyable<NbtTree>
    {
        private Stream _stream = null;
        private TagNodeCompound _root = null;
        private string _rootName = "";
        public UInt32 versionSaved;
        public HeaderType headerType = HeaderType.None;
        public EndiannessType endiannessType = EndiannessType.BigEndian;
        
        private static TagNodeNull _nulltag = new TagNodeNull();

        /// <summary>
        /// Gets the root node of this tree.
        /// </summary>
        public TagNodeCompound Root
        {
            get { return _root; }
        }

        /// <summary>
        /// Gets or sets the name of the tree's root node.
        /// </summary>
        public string Name
        {
            get { return _rootName; }
            set { _rootName = value; }
        }

        /// <summary>
        /// Constructs a wrapper around a new NBT tree with an empty root node.
        /// </summary>
        public NbtTree ()
        {
            _root = new TagNodeCompound();
        }

        /// <summary>
        /// Constructs a wrapper around another NBT tree.
        /// </summary>
        /// <param name="tree">The root node of an NBT tree.</param>
        public NbtTree (TagNodeCompound tree)
        {
            _root = tree;
        }

        /// <summary>
        /// Constructs a wrapper around another NBT tree and gives it a name.
        /// </summary>
        /// <param name="tree">The root node of an NBT tree.</param>
        /// <param name="name">The name for the root node.</param>
        public NbtTree (TagNodeCompound tree, string name)
        {
            _root = tree;
            _rootName = name;
        }

        /// <summary>
        /// Constructs and wrapper around a new NBT tree parsed from a source data stream.
        /// </summary>
        /// <param name="s">An open, readable data stream containing NBT data.</param>
        public NbtTree (Stream s)
        {
            ReadFrom(s);
        }

        /// <summary>
        /// Rebuild the internal NBT tree from a source data stream.
        /// </summary>
        /// <param name="s">An open, readable data stream containing NBT data.</param>
        public void ReadFrom (Stream s)
        {
            ReadFrom(s, EndiannessType.BigEndian); /* Would autocorrect itself for drop-in */
        }

        /// <summary>
        /// Rebuild the internal NBT tree from a source data stream using the specified endianess type.
        /// </summary>
        /// <param name="s">An open, readable data stream containing NBT data.</param>
        /// <param name="endian">The endianness type to use for reading all the integer types.</param>
        public void ReadFrom(Stream s, EndiannessType endian)
        {
            if (s != null)
            {
                _stream = s;
                _root = ReadRoot(endian);
                _stream = null;
            }
        }

        /// <summary>
        /// Writes out the internal NBT tree to a destination data stream.
        /// </summary>
        /// <param name="s">An open, writable data stream.</param>
        public void WriteTo (Stream s)
        {
            WriteTo(s, endiannessType);
        }

        /// <summary>
        /// Writes out the internal NBT tree to a destination data stream.
        /// </summary>
        /// <param name="s">An open, writable data stream.</param>
        /// <param name="endian">The endianness type to use for writing all the integer types.</param>
        public void WriteTo (Stream s, EndiannessType endian)
        {
            if (s != null && _root != null) {
                _stream = s;
                long oldStreamLength = s.Length;
                WriteTag(_rootName, _root, endian);
                int length = (int)(s.Length - oldStreamLength);

                if (headerType != HeaderType.None)
                    _stream.Seek(oldStreamLength, SeekOrigin.Begin);
                switch(headerType)
                {
                    case HeaderType.EntityHeader:
                        byte[] header = new byte[] { (byte)'E', (byte)'N', (byte)'T', 0 };
                        _stream.Write(header, 0, 4);
                        goto case HeaderType.LevelHeader;
                    case HeaderType.LevelHeader:
                        byte[] version = BitConverter.GetBytes(versionSaved);
                        byte[] lengthBytes = BitConverter.GetBytes(length);
                        if (!BitConverter.IsLittleEndian)
                        {
                            Array.Reverse(version);
                            Array.Reverse(lengthBytes);
                        }
                        _stream.Write(version, 0, 4);
                        _stream.Write(lengthBytes, 0, 4);
                        WriteTag(_rootName, _root, endian);
                        break;
                }


                _stream = null;
            }
        }

        private TagNode ReadValue (TagType type, EndiannessType endian)
        {
            switch (type) {
                case TagType.TAG_END:
                    return null;

                case TagType.TAG_BYTE:
                    return ReadByte();

                case TagType.TAG_SHORT:
                    return ReadShort(endian);

                case TagType.TAG_INT:
                    return ReadInt(endian);

                case TagType.TAG_LONG:
                    return ReadLong(endian);

                case TagType.TAG_FLOAT:
                    return ReadFloat(endian);

                case TagType.TAG_DOUBLE:
                    return ReadDouble(endian);

                case TagType.TAG_BYTE_ARRAY:
                    return ReadByteArray(endian);

                case TagType.TAG_STRING:
                    return ReadString(endian);

                case TagType.TAG_LIST:
                    return ReadList(endian); /* elements need endian + length? */

                case TagType.TAG_COMPOUND:
                    return ReadCompound(endian);

                case TagType.TAG_INT_ARRAY:
                    return ReadIntArray(endian);
                    
                case TagType.TAG_LONG_ARRAY:
                    return ReadLongArray(endian);

                case TagType.TAG_SHORT_ARRAY:
                    return ReadShortArray(endian);
            }

            throw new Exception();
        }

        private bool EndiannessMatchesHost(EndiannessType endian)
        {
            return (endian == EndiannessType.LittleEndian && BitConverter.IsLittleEndian || endian == EndiannessType.BigEndian && !BitConverter.IsLittleEndian);
        }

        private TagNode ReadByte ()
        {
            int gzByte = _stream.ReadByte();
            if (gzByte == -1) {
                throw new NBTException(NBTException.MSG_GZIP_ENDOFSTREAM);
            }

            TagNodeByte val = new TagNodeByte((byte)gzByte);

            return val;
        }

        private TagNode ReadShort (EndiannessType endian)
        {
            byte[] gzBytes = new byte[2];
            _stream.Read(gzBytes, 0, 2);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(gzBytes);
            }

            TagNodeShort val = new TagNodeShort(BitConverter.ToInt16(gzBytes, 0));

            return val;
        }

        private TagNode ReadInt (EndiannessType endian)
        {
            byte[] gzBytes = new byte[4];
            _stream.Read(gzBytes, 0, 4);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(gzBytes);
            }

            TagNodeInt val = new TagNodeInt(BitConverter.ToInt32(gzBytes, 0));

            return val;
        }

        private TagNode ReadLong (EndiannessType endian)
        {
            byte[] gzBytes = new byte[8];
            _stream.Read(gzBytes, 0, 8);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(gzBytes);
            }

            TagNodeLong val = new TagNodeLong(BitConverter.ToInt64(gzBytes, 0));

            return val;
        }

        private TagNode ReadFloat (EndiannessType endian)
        {
            byte[] gzBytes = new byte[4];
            _stream.Read(gzBytes, 0, 4);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(gzBytes);
            }

            TagNodeFloat val = new TagNodeFloat(BitConverter.ToSingle(gzBytes, 0));

            return val;
        }

        private TagNode ReadDouble (EndiannessType endian)
        {
            byte[] gzBytes = new byte[8];
            _stream.Read(gzBytes, 0, 8);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(gzBytes);
            }

            TagNodeDouble val = new TagNodeDouble(BitConverter.ToDouble(gzBytes, 0));

            return val;
        }

        private TagNode ReadByteArray (EndiannessType endian)
        {
            byte[] lenBytes = new byte[4];
            _stream.Read(lenBytes, 0, 4);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            int length = BitConverter.ToInt32(lenBytes, 0);
            if (length < 0) {
                throw new NBTException(NBTException.MSG_READ_NEG);
            }

            byte[] data = new byte[length];
            _stream.Read(data, 0, length);

            TagNodeByteArray val = new TagNodeByteArray(data);

            return val;
        }

        private TagNode ReadString (EndiannessType endian)
        {
            byte[] lenBytes = new byte[2];
            _stream.Read(lenBytes, 0, 2);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            short len = BitConverter.ToInt16(lenBytes, 0);
            if (len < 0) {
                throw new NBTException(NBTException.MSG_READ_NEG);
            }

            byte[] strBytes = new byte[len];
            _stream.Read(strBytes, 0, len);

            System.Text.Encoding str = Encoding.UTF8;

            TagNodeString val = new TagNodeString(str.GetString(strBytes));

            return val;
        }

        private TagNode ReadList (EndiannessType endian)
        {
            int gzByte = _stream.ReadByte();
            if (gzByte == -1) {
                throw new NBTException(NBTException.MSG_GZIP_ENDOFSTREAM);
            }

            TagNodeList val = new TagNodeList((TagType)gzByte);
            if (val.ValueType > (TagType)Enum.GetValues(typeof(TagType)).GetUpperBound(0)) {
                throw new NBTException(NBTException.MSG_READ_TYPE);
            }

            byte[] lenBytes = new byte[4];
            _stream.Read(lenBytes, 0, 4);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            int length = BitConverter.ToInt32(lenBytes, 0);
            if (length < 0) {
                throw new NBTException(NBTException.MSG_READ_NEG);
            }

            if (val.ValueType == TagType.TAG_END)
                return new TagNodeList(TagType.TAG_BYTE);

            for (int i = 0; i < length; i++) {
                val.Add(ReadValue(val.ValueType, endian));
            }

            return val;
        }

        private TagNode ReadCompound (EndiannessType endian)
        {
            TagNodeCompound val = new TagNodeCompound();

            while (ReadTag(val, endian)) ;

            return val;
        }

        private TagNode ReadIntArray (EndiannessType endian)
        {
            byte[] lenBytes = new byte[4];
            _stream.Read(lenBytes, 0, 4);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            int length = BitConverter.ToInt32(lenBytes, 0);
            if (length < 0) {
                throw new NBTException(NBTException.MSG_READ_NEG);
            }

            int[] data = new int[length];
            byte[] buffer = new byte[4];
            for (int i = 0; i < length; i++) {
                _stream.Read(buffer, 0, 4);
                if (!EndiannessMatchesHost(endian)) {
                    Array.Reverse(buffer);
                }
                data[i] = BitConverter.ToInt32(buffer, 0);
            }

            TagNodeIntArray val = new TagNodeIntArray(data);

            return val;
        }

        private TagNode ReadLongArray (EndiannessType endian)
        {
            byte[] lenBytes = new byte[4];
            _stream.Read(lenBytes, 0, 4);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            int length = BitConverter.ToInt32(lenBytes, 0);
            if (length < 0) {
                throw new NBTException(NBTException.MSG_READ_NEG);
            }

            long[] data = new long[length];
            byte[] buffer = new byte[8];
            for (int i = 0; i < length; i++) {
                _stream.Read(buffer, 0, 8);
                if (!EndiannessMatchesHost(endian)) {
                    Array.Reverse(buffer);
                }
                data[i] = BitConverter.ToInt64(buffer, 0);
            }

            TagNodeLongArray val = new TagNodeLongArray(data);

            return val;
        }

        private TagNode ReadShortArray (EndiannessType endian)
        {
            byte[] lenBytes = new byte[4];
            _stream.Read(lenBytes, 0, 4);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            int length = BitConverter.ToInt32(lenBytes, 0);
            if (length < 0) {
                throw new NBTException(NBTException.MSG_READ_NEG);
            }

            short[] data = new short[length];
            byte[] buffer = new byte[2];
            for (int i = 0; i < length; i++) {
                _stream.Read(buffer, 0, 2);
                if (!EndiannessMatchesHost(endian)) {
                    Array.Reverse(buffer);
                }
                data[i] = BitConverter.ToInt16(buffer, 0);
            }

            TagNodeShortArray val = new TagNodeShortArray(data);

            return val;
        }

        private TagNodeCompound ReadRoot (EndiannessType endian) {
            TagType type = (TagType)_stream.ReadByte();
            if (type == TagType.TAG_COMPOUND)
                headerType = HeaderType.None;
            /* TODO: Ensure there is no format version 10, as this line can fail on it. This bug isn't possible with the entity header. */
            else
            {
                byte[] header = new byte[4];
                header[0] = (byte)type;
                _stream.Read(header, 1, 3);
                /* Entity header */
                if (System.Text.Encoding.ASCII.GetString(header, 0, 3).Equals("ENT") && header[3] == 0)
                {
                    _stream.Read(header, 0, 4);
                    headerType = HeaderType.EntityHeader;
                }
                else headerType = HeaderType.LevelHeader;
                if (!BitConverter.IsLittleEndian)
                    Array.Reverse(header);
                versionSaved = BitConverter.ToUInt32(header, 0); /* Save for future writing */
                /* Otherwise the header above was just the version header, since we don't know all version values, just check file length */
                _stream.Read(header, 0, 4);
                if (!BitConverter.IsLittleEndian)
                    Array.Reverse(header);
                if (_stream.Length - ((headerType == HeaderType.EntityHeader) ? 12 : 8) != System.BitConverter.ToUInt32(header, 0)) /* invalid length */
                    return null;
                type = (TagType)_stream.ReadByte();
                if (type != TagType.TAG_COMPOUND) /* Data after the header is invalid */
                    return null;
                endian = EndiannessType.LittleEndian; /* MCPE does not have big endian, so save with it by default. This also would work well for drop-in */
            }
            _rootName = ReadString(endian).ToTagString().Data; // name
            endiannessType = endian;
            return ReadValue(type, endian) as TagNodeCompound;
        }

        private bool ReadTag (TagNodeCompound parent, EndiannessType endian)
        {
            TagType type = (TagType)_stream.ReadByte();
            if (type != TagType.TAG_END) {
                string name = ReadString(endian).ToTagString().Data;
                parent[name] = ReadValue(type, endian);
                return true;
            }

            return false;
        }

        private void WriteValue (TagNode val, EndiannessType endian)
        {
            switch (val.GetTagType()) {
                case TagType.TAG_END:
                    break;

                case TagType.TAG_BYTE:
                    WriteByte(val.ToTagByte());
                    break;

                case TagType.TAG_SHORT:
                    WriteShort(val.ToTagShort(), endian);
                    break;

                case TagType.TAG_INT:
                    WriteInt(val.ToTagInt(), endian);
                    break;

                case TagType.TAG_LONG:
                    WriteLong(val.ToTagLong(), endian);
                    break;

                case TagType.TAG_FLOAT:
                    WriteFloat(val.ToTagFloat(), endian);
                    break;

                case TagType.TAG_DOUBLE:
                    WriteDouble(val.ToTagDouble(), endian);
                    break;

                case TagType.TAG_BYTE_ARRAY:
                    WriteByteArray(val.ToTagByteArray(), endian);
                    break;

                case TagType.TAG_STRING:
                    WriteString(val.ToTagString(), endian);
                    break;

                case TagType.TAG_LIST:
                    WriteList(val.ToTagList(), endian);
                    break;

                case TagType.TAG_COMPOUND:
                    WriteCompound(val.ToTagCompound(), endian);
                    break;

                case TagType.TAG_INT_ARRAY:
                    WriteIntArray(val.ToTagIntArray(), endian);
                    break;

                case TagType.TAG_LONG_ARRAY:
                    WriteLongArray(val.ToTagLongArray(), endian);
                    break;

                case TagType.TAG_SHORT_ARRAY:
                    WriteShortArray(val.ToTagShortArray(), endian);
                    break;
            }
        }

        private void WriteByte (TagNodeByte val)
        {
            _stream.WriteByte(val.Data);
        }

        private void WriteShort (TagNodeShort val, EndiannessType endian)
        {
            byte[] gzBytes = BitConverter.GetBytes(val.Data);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(gzBytes);
            }

            _stream.Write(gzBytes, 0, 2);
        }

        private void WriteInt (TagNodeInt val, EndiannessType endian)
        {
            byte[] gzBytes = BitConverter.GetBytes(val.Data);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(gzBytes);
            }

            _stream.Write(gzBytes, 0, 4);
        }

        private void WriteLong (TagNodeLong val, EndiannessType endian)
        {
            byte[] gzBytes = BitConverter.GetBytes(val.Data);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(gzBytes);
            }

            _stream.Write(gzBytes, 0, 8);
        }

        private void WriteFloat (TagNodeFloat val, EndiannessType endian)
        {
            byte[] gzBytes = BitConverter.GetBytes(val.Data);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(gzBytes);
            }

            _stream.Write(gzBytes, 0, 4);
        }

        private void WriteDouble (TagNodeDouble val, EndiannessType endian)
        {
            byte[] gzBytes = BitConverter.GetBytes(val.Data);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(gzBytes);
            }

            _stream.Write(gzBytes, 0, 8);
        }

        private void WriteByteArray (TagNodeByteArray val, EndiannessType endian)
        {
            byte[] lenBytes = BitConverter.GetBytes(val.Length);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            _stream.Write(lenBytes, 0, 4);
            _stream.Write(val.Data, 0, val.Length);
        }

        private void WriteString (TagNodeString val, EndiannessType endian)
        {
            System.Text.Encoding str = Encoding.UTF8;
            byte[] gzBytes = str.GetBytes(val.Data);

            byte[] lenBytes = BitConverter.GetBytes((short)gzBytes.Length);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            _stream.Write(lenBytes, 0, 2);

            _stream.Write(gzBytes, 0, gzBytes.Length);
        }

        private void WriteList (TagNodeList val, EndiannessType endian)
        {
            byte[] lenBytes = BitConverter.GetBytes(val.Count);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            _stream.WriteByte((byte)val.ValueType);
            _stream.Write(lenBytes, 0, 4);

            foreach (TagNode v in val) {
                WriteValue(v, endian);
            }
        }

        private void WriteCompound (TagNodeCompound val, EndiannessType endian)
        {
            foreach (KeyValuePair<string, TagNode> item in val) {
                WriteTag(item.Key, item.Value, endian);
            }

            WriteTag(null, _nulltag, endian);
        }

        private void WriteIntArray (TagNodeIntArray val, EndiannessType endian)
        {
            byte[] lenBytes = BitConverter.GetBytes(val.Length);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            _stream.Write(lenBytes, 0, 4);

            byte[] data = new byte[val.Length * 4];
            for (int i = 0; i < val.Length; i++) {
                byte[] buffer = BitConverter.GetBytes(val.Data[i]);
                if (!EndiannessMatchesHost(endian)) {
                    Array.Reverse(buffer);
                }
                Array.Copy(buffer, 0, data, i * 4, 4);
            }

            _stream.Write(data, 0, data.Length);
        }

        private void WriteLongArray (TagNodeLongArray val, EndiannessType endian)
        {
            byte[] lenBytes = BitConverter.GetBytes(val.Length);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            _stream.Write(lenBytes, 0, 4);

            byte[] data = new byte[val.Length * 8];
            for (int i = 0; i < val.Length; i++) {
                byte[] buffer = BitConverter.GetBytes(val.Data[i]);
                if (!EndiannessMatchesHost(endian)) {
                    Array.Reverse(buffer);
                }
                Array.Copy(buffer, 0, data, i * 8, 8);
            }

            _stream.Write(data, 0, data.Length);
        }

        private void WriteShortArray (TagNodeShortArray val, EndiannessType endian)
        {
            byte[] lenBytes = BitConverter.GetBytes(val.Length);

            if (!EndiannessMatchesHost(endian)) {
                Array.Reverse(lenBytes);
            }

            _stream.Write(lenBytes, 0, 4);

            byte[] data = new byte[val.Length * 2];
            for (int i = 0; i < val.Length; i++) {
                byte[] buffer = BitConverter.GetBytes(val.Data[i]);
                if (!EndiannessMatchesHost(endian)) {
                    Array.Reverse(buffer);
                }
                Array.Copy(buffer, 0, data, i * 2, 2);
            }

            _stream.Write(data, 0, data.Length);
        }

        private void WriteTag (string name, TagNode val, EndiannessType endian)
        {
            _stream.WriteByte((byte)val.GetTagType());

            if (val.GetTagType() != TagType.TAG_END) {
                WriteString(name, endian);
                WriteValue(val, endian);
            }
        }

        #region ICopyable<NBT_Tree> Members

        /// <summary>
        /// Creates a deep copy of the NBT_Tree and underlying nodes.
        /// </summary>
        /// <returns>A new NBT_tree.</returns>
        public NbtTree Copy ()
        {
            NbtTree tree = new NbtTree();
            tree._root = _root.Copy() as TagNodeCompound;

            return tree;
        }

        #endregion
    }

    // TODO: Revise exceptions?
    public class NBTException : Exception
    {
        public const String MSG_GZIP_ENDOFSTREAM = "Gzip Error: Unexpected end of stream";

        public const String MSG_READ_NEG = "Read Error: Negative length";
        public const String MSG_READ_TYPE = "Read Error: Invalid value type";

        public NBTException () { }

        public NBTException (String msg) : base(msg) { }

        public NBTException (String msg, Exception innerException) : base(msg, innerException) { }
    }

    public class InvalidNBTObjectException : Exception { }

    public class InvalidTagException : Exception { }

    public class InvalidValueException : Exception { }
}
