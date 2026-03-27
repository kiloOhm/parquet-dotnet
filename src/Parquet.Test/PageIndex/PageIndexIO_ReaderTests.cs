using System;
using System.IO;
using System.Linq;
using Parquet.Encryption;
using Parquet.File;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Xunit;

namespace Parquet.Test.PageIndex {
    public sealed class PageIndexIO_ReaderTests {
        private static readonly byte[] Key16 = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
        private static readonly byte[] Prefix = new byte[] { 0x50, 0x49, 0x58 };
        private static readonly byte[] Unique = new byte[] { 0x10, 0x20, 0x30, 0x40 };

        private static ThriftCompactProtocolWriter MakeWriter(Stream s) => new ThriftCompactProtocolWriter(s);
        private static ThriftCompactProtocolReader MakeReader(Stream s) => new ThriftCompactProtocolReader(s);
        private static ThriftCompactProtocolReader MakeReader(byte[] bytes) => new ThriftCompactProtocolReader(new MemoryStream(bytes));

        private static AES_GCM_V1_Encryption MakeGcm() => new AES_GCM_V1_Encryption {
            FooterEncryptionKey = Key16,
            AadPrefix = Prefix,
            AadFileUnique = Unique
        };

        private static OffsetIndex MakeOffsetIndex() {
            return new OffsetIndex {
                PageLocations = {
                    new PageLocation { Offset = 111, CompressedPageSize = 37, FirstRowIndex = 0 },
                    new PageLocation { Offset = 222, CompressedPageSize = 41, FirstRowIndex = 10 }
                },
                UnencodedByteArrayDataBytes = new System.Collections.Generic.List<long> { 5, 7 }
            };
        }

        private static ColumnIndex MakeColumnIndex() {
            return new ColumnIndex {
                NullPages = { false, true },
                MinValues = { new byte[] { 0x01 }, Array.Empty<byte>() },
                MaxValues = { new byte[] { 0x09 }, Array.Empty<byte>() },
                BoundaryOrder = BoundaryOrder.ASCENDING,
                NullCounts = new System.Collections.Generic.List<long> { 0, 10 }
            };
        }

        private static void WriteOffsetIndexAtOffset(MemoryStream ms, ColumnChunk chunk, OffsetIndex index) {
            ms.WriteByte(0xAB);
            long offset = ms.Position;
            index.Write(MakeWriter(ms));
            chunk.OffsetIndexOffset = offset;
            chunk.OffsetIndexLength = checked((int)(ms.Position - offset));
        }

        private static void WriteColumnIndexAtOffset(MemoryStream ms, ColumnChunk chunk, ColumnIndex index) {
            ms.WriteByte(0xCD);
            long offset = ms.Position;
            index.Write(MakeWriter(ms));
            chunk.ColumnIndexOffset = offset;
            chunk.ColumnIndexLength = checked((int)(ms.Position - offset));
        }

        private static void WriteEncryptedOffsetIndexAtOffset(
            MemoryStream ms,
            ColumnChunk chunk,
            OffsetIndex index,
            AES_GCM_V1_Encryption enc,
            short rowGroupOrdinal,
            short columnOrdinal) {
            using var plain = new MemoryStream();
            index.Write(MakeWriter(plain));

            ms.WriteByte(0x11);
            long offset = ms.Position;
            byte[] framed = enc.EncryptOffsetIndex(plain.ToArray(), rowGroupOrdinal, columnOrdinal);
            ms.Write(framed, 0, framed.Length);
            chunk.OffsetIndexOffset = offset;
            chunk.OffsetIndexLength = framed.Length;
        }

        private static void WriteEncryptedColumnIndexAtOffset(
            MemoryStream ms,
            ColumnChunk chunk,
            ColumnIndex index,
            AES_GCM_V1_Encryption enc,
            short rowGroupOrdinal,
            short columnOrdinal) {
            using var plain = new MemoryStream();
            index.Write(MakeWriter(plain));

            ms.WriteByte(0x22);
            long offset = ms.Position;
            byte[] framed = enc.EncryptColumnIndex(plain.ToArray(), rowGroupOrdinal, columnOrdinal);
            ms.Write(framed, 0, framed.Length);
            chunk.ColumnIndexOffset = offset;
            chunk.ColumnIndexLength = framed.Length;
        }

        [Fact]
        public void WriteOffsetIndex_Sets_Metadata_And_RoundTrips() {
            OffsetIndex expected = MakeOffsetIndex();
            ColumnChunk chunk = new ColumnChunk();

            using var ms = new MemoryStream();
            (long offset, int length) = PageIndexIO.WriteOffsetIndex(ms, expected, chunk, MakeWriter);
            ms.Seek(0, SeekOrigin.Begin);

            OffsetIndex actual = PageIndexIO.ReadOffsetIndex(ms, chunk, MakeReader);

            Assert.Equal(0, offset);
            Assert.Equal(length, chunk.OffsetIndexLength);
            Assert.Equal(offset, chunk.OffsetIndexOffset);
            Assert.Equal(new long[] { 5, 7 }, actual.UnencodedByteArrayDataBytes);
            Assert.Equal(10, actual.PageLocations[1].FirstRowIndex);
        }

        [Fact]
        public void WriteEncryptedColumnIndex_Sets_Metadata_And_RoundTrips() {
            const short rowGroupOrdinal = 5;
            const short columnOrdinal = 2;
            ColumnIndex expected = MakeColumnIndex();
            ColumnChunk chunk = new ColumnChunk();
            AES_GCM_V1_Encryption enc = MakeGcm();
            AES_GCM_V1_Encryption dec = MakeGcm();

            using var ms = new MemoryStream();
            (long offset, int length) = PageIndexIO.WriteColumnIndex(
                ms,
                expected,
                chunk,
                MakeWriter,
                enc,
                rowGroupOrdinal,
                columnOrdinal);
            ms.Seek(0, SeekOrigin.Begin);

            ColumnIndex actual = PageIndexIO.ReadEncryptedColumnIndex(
                ms,
                chunk,
                dec,
                rowGroupOrdinal,
                columnOrdinal,
                MakeReader);

            Assert.Equal(0, offset);
            Assert.Equal(length, chunk.ColumnIndexLength);
            Assert.Equal(offset, chunk.ColumnIndexOffset);
            Assert.Equal(new bool[] { false, true }, actual.NullPages);
            Assert.Equal(new long[] { 0, 10 }, actual.NullCounts);
        }

        [Fact]
        public void ReadOffsetIndex_RoundTrip_Works() {
            OffsetIndex expected = MakeOffsetIndex();
            ColumnChunk chunk = new ColumnChunk();

            using var ms = new MemoryStream();
            WriteOffsetIndexAtOffset(ms, chunk, expected);
            ms.Seek(0, SeekOrigin.Begin);

            OffsetIndex actual = PageIndexIO.ReadOffsetIndex(ms, chunk, MakeReader);

            Assert.Equal(2, actual.PageLocations.Count);
            Assert.Equal(111, actual.PageLocations[0].Offset);
            Assert.Equal(41, actual.PageLocations[1].CompressedPageSize);
            Assert.Equal(10, actual.PageLocations[1].FirstRowIndex);
            Assert.Equal(new long[] { 5, 7 }, actual.UnencodedByteArrayDataBytes);
        }

        [Fact]
        public void ReadColumnIndex_RoundTrip_Works() {
            ColumnIndex expected = MakeColumnIndex();
            ColumnChunk chunk = new ColumnChunk();

            using var ms = new MemoryStream();
            WriteColumnIndexAtOffset(ms, chunk, expected);
            ms.Seek(0, SeekOrigin.Begin);

            ColumnIndex actual = PageIndexIO.ReadColumnIndex(ms, chunk, MakeReader);

            Assert.Equal(new bool[] { false, true }, actual.NullPages);
            Assert.Equal(BoundaryOrder.ASCENDING, actual.BoundaryOrder);
            Assert.Equal(new byte[] { 0x01 }, actual.MinValues[0]);
            Assert.Equal(Array.Empty<byte>(), actual.MaxValues[1]);
            Assert.Equal(new long[] { 0, 10 }, actual.NullCounts);
        }

        [Fact]
        public void ReadOffsetIndex_Throws_When_No_Offset() {
            using var ms = new MemoryStream();
            ColumnChunk chunk = new ColumnChunk();

            Assert.Throws<InvalidOperationException>(() => PageIndexIO.ReadOffsetIndex(ms, chunk, MakeReader));
        }

        [Fact]
        public void ReadColumnIndex_Throws_When_No_Offset() {
            using var ms = new MemoryStream();
            ColumnChunk chunk = new ColumnChunk();

            Assert.Throws<InvalidOperationException>(() => PageIndexIO.ReadColumnIndex(ms, chunk, MakeReader));
        }

        [Fact]
        public void ReadEncryptedOffsetIndex_RoundTrip_Works() {
            const short rowGroupOrdinal = 2;
            const short columnOrdinal = 3;
            OffsetIndex expected = MakeOffsetIndex();
            ColumnChunk chunk = new ColumnChunk();
            AES_GCM_V1_Encryption enc = MakeGcm();
            AES_GCM_V1_Encryption dec = MakeGcm();

            using var ms = new MemoryStream();
            WriteEncryptedOffsetIndexAtOffset(ms, chunk, expected, enc, rowGroupOrdinal, columnOrdinal);
            ms.Seek(0, SeekOrigin.Begin);

            OffsetIndex actual = PageIndexIO.ReadEncryptedOffsetIndex(
                ms,
                chunk,
                dec,
                rowGroupOrdinal,
                columnOrdinal,
                MakeReader);

            Assert.Equal(2, actual.PageLocations.Count);
            Assert.Equal(222, actual.PageLocations[1].Offset);
            Assert.Equal(new long[] { 5, 7 }, actual.UnencodedByteArrayDataBytes);
        }

        [Fact]
        public void ReadEncryptedColumnIndex_RoundTrip_Works() {
            const short rowGroupOrdinal = 4;
            const short columnOrdinal = 1;
            ColumnIndex expected = MakeColumnIndex();
            ColumnChunk chunk = new ColumnChunk();
            AES_GCM_V1_Encryption enc = MakeGcm();
            AES_GCM_V1_Encryption dec = MakeGcm();

            using var ms = new MemoryStream();
            WriteEncryptedColumnIndexAtOffset(ms, chunk, expected, enc, rowGroupOrdinal, columnOrdinal);
            ms.Seek(0, SeekOrigin.Begin);

            ColumnIndex actual = PageIndexIO.ReadEncryptedColumnIndex(
                ms,
                chunk,
                dec,
                rowGroupOrdinal,
                columnOrdinal,
                MakeReader);

            Assert.Equal(new bool[] { false, true }, actual.NullPages);
            Assert.Equal(BoundaryOrder.ASCENDING, actual.BoundaryOrder);
            Assert.Equal(new long[] { 0, 10 }, actual.NullCounts);
        }
    }
}
