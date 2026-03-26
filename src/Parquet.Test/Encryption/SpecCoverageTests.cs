using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Bloom;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Parquet.Schema;
using Xunit;
using Encoding = System.Text.Encoding;

namespace Parquet.Test.Encryption {
    [Collection(nameof(ParquetEncryptionTestCollection))]
    public class SpecCoverageTests {
        private static FileCryptoMetaData ReadEncryptedFooterCryptoMeta(byte[] fileBytes) {
            int tailLen = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(fileBytes.Length - 8, 4));
            int tailStart = fileBytes.Length - 8 - tailLen;
            using var ms = new MemoryStream(fileBytes, tailStart, tailLen, writable: false);
            return FileCryptoMetaData.Read(new ThriftCompactProtocolReader(ms));
        }

        [Fact]
        public async Task EF_ColumnKey_Columns_Omit_Plaintext_MetaData_OnWire() {
            var schema = new ParquetSchema(
                new DataField<int>("id"),
                new DataField<string>("secret"));

            var opts = new ParquetOptions {
                FooterEncryptionKey = "01234567891234FK"
            };
            opts.ColumnKeys["secret"] = new ParquetOptions.ColumnKeySpec("01234567891234CK");

            byte[] bytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3 }));
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[1], new[] { "a", "b", "c" }));
                }
                bytes = ms.ToArray();
            }

            using var msRead = new MemoryStream(bytes, writable: false);
            using ParquetReader reader = await ParquetReader.CreateAsync(msRead, new ParquetOptions {
                FooterEncryptionKey = opts.FooterEncryptionKey
            });

            ColumnChunk plain = reader.Metadata!.RowGroups[0].Columns[0];
            ColumnChunk enc = reader.Metadata!.RowGroups[0].Columns[1];

            Assert.NotNull(plain.MetaData);
            Assert.NotNull(enc.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY);
            Assert.NotNull(enc.EncryptedColumnMetadata);
            Assert.Null(enc.MetaData);
        }

        [Fact]
        public async Task EF_Encrypted_Column_BloomFilter_Is_Encrypted_But_Reader_Can_Use_It() {
            var schema = new ParquetSchema(
                new DataField<int>("id"),
                new DataField<string>("secret"));

            var opts = new ParquetOptions {
                FooterEncryptionKey = "01234567891234FK"
            };
            opts.ColumnKeys["secret"] = new ParquetOptions.ColumnKeySpec("01234567891234CK");
            opts.BloomFilterOptionsByColumn["secret"] = new ParquetOptions.BloomFilterOptions {
                EnableBloomFilters = true,
                BloomFilterFpp = 0.0001f,
                BloomFilterBitsPerValueOverride = 32
            };

            byte[] bytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, opts)) {
                    using ParquetRowGroupWriter rowGroupWriter = w.CreateRowGroup();
                    await rowGroupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3, 4 }));
                    await rowGroupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[1], new[] { "alpha", "beta", "gamma", "delta" }));
                }
                bytes = ms.ToArray();
            }

            using var msRead = new MemoryStream(bytes, writable: false);
            using ParquetReader reader = await ParquetReader.CreateAsync(msRead, new ParquetOptions {
                FooterEncryptionKey = opts.FooterEncryptionKey,
                ColumnKeyResolver = (path, keyMetadata) => string.Join(".", path) == "secret" ? "01234567891234CK" : null
            });
            using ParquetRowGroupReader rg = reader.OpenRowGroupReader(0);
            DataField secretField = (DataField)reader.Schema.Fields[1];
            ColumnChunk secretMeta = rg.GetMetadata(secretField)!;

            using var raw = new MemoryStream(bytes, writable: false);
            Assert.ThrowsAny<Exception>(() =>
                BloomFilterIO.ReadFromStream(raw, secretMeta.MetaData!, s => new ThriftCompactProtocolReader(s)));

            Assert.True(rg.MightMatchEquals(secretField, "beta"));
            Assert.False(rg.MightMatchEquals(secretField, "not-present"));
        }

        [Fact]
        public async Task NonAscii_AadPrefix_Stored_As_Utf8_And_Supply_Mode_RoundTrips() {
            const string aadPrefix = "kunden/überblick/東京";
            var schema = new ParquetSchema(new DataField<int>("id"));

            byte[] storedBytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, new ParquetOptions {
                    FooterEncryptionKey = "01234567891234FK",
                    AADPrefix = aadPrefix,
                    SupplyAadPrefix = false
                })) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1, 2, 3 }));
                }
                storedBytes = ms.ToArray();
            }

            FileCryptoMetaData storedMeta = ReadEncryptedFooterCryptoMeta(storedBytes);
            Assert.Equal(Encoding.UTF8.GetBytes(aadPrefix), storedMeta.EncryptionAlgorithm.AESGCMV1!.AadPrefix);

            byte[] supplyBytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, new ParquetOptions {
                    FooterEncryptionKey = "01234567891234FK",
                    AADPrefix = aadPrefix,
                    SupplyAadPrefix = true
                })) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 4, 5, 6 }));
                }
                supplyBytes = ms.ToArray();
            }

            using var msRead = new MemoryStream(supplyBytes, writable: false);
            using ParquetReader reader = await ParquetReader.CreateAsync(msRead, new ParquetOptions {
                FooterEncryptionKey = "01234567891234FK",
                AADPrefix = aadPrefix
            });
            using ParquetRowGroupReader rgRead = reader.OpenRowGroupReader(0);
            DataColumn id = await rgRead.ReadColumnAsync((DataField)reader.Schema.Fields[0]);
            Assert.Equal(new[] { 4, 5, 6 }, id.AsSpan<int>().ToArray());
        }

        [Fact]
        public async Task Footer_And_Signing_Key_Metadata_Are_Written() {
            byte[] footerKeyMetadata = Encoding.UTF8.GetBytes("kms:footer-key-id");
            byte[] signingKeyMetadata = Encoding.UTF8.GetBytes("kms:signing-key-id");
            var schema = new ParquetSchema(new DataField<int>("id"));

            byte[] efBytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, new ParquetOptions {
                    FooterEncryptionKey = "01234567891234FK",
                    FooterEncryptionKeyMetadata = footerKeyMetadata
                })) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 1 }));
                }
                efBytes = ms.ToArray();
            }

            FileCryptoMetaData efMeta = ReadEncryptedFooterCryptoMeta(efBytes);
            Assert.Equal(footerKeyMetadata, efMeta.KeyMetadata);

            byte[] pfBytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter w = await ParquetWriter.CreateAsync(schema, ms, new ParquetOptions {
                    UsePlaintextFooter = true,
                    FooterSigningKey = "01234567891234SK",
                    FooterSigningKeyMetadata = signingKeyMetadata
                })) {
                    using ParquetRowGroupWriter rg = w.CreateRowGroup();
                    await rg.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], new[] { 2 }));
                }
                pfBytes = ms.ToArray();
            }

            using var msRead = new MemoryStream(pfBytes, writable: false);
            using ParquetReader reader = await ParquetReader.CreateAsync(msRead, new ParquetOptions {
                FooterSigningKey = "01234567891234SK"
            });
            Assert.Equal(signingKeyMetadata, reader.Metadata!.FooterSigningKeyMetadata);
        }
    }
}
