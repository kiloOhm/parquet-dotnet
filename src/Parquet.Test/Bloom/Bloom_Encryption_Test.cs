using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Bloom {
    public sealed class Bloom_Encryption_Test {
        [Fact]
        public async Task FooterKey_Encrypted_Bloom_Prunes_And_RoundTrips() {
            var field = new DataField<string>("id");
            var schema = new ParquetSchema(field);
            string[] values = { "user-001", "user-002", "user-003" };

            var opts = new ParquetOptions {
                FooterEncryptionKey = Convert.ToBase64String(Enumerable.Range(1, 16).Select(i => (byte)i).ToArray()),
                BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions> {
                    [field.Name] = new() { EnableBloomFilters = true }
                }
            };

            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, formatOptions: opts)) {
                using ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup();
                await rowGroupWriter.WriteColumnAsync(new DataColumn(field, values));
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms, new ParquetOptions {
                FooterEncryptionKey = opts.FooterEncryptionKey,
                BloomFilterOptionsByColumn = opts.BloomFilterOptionsByColumn
            });

            Assert.True(reader.IsEncryptedFile);

            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
            Assert.True(rowGroupReader.MightMatchEquals(field, "user-002"));
            Assert.False(rowGroupReader.MightMatchEquals(field, "user-999"));

            DataColumn col = await rowGroupReader.ReadColumnAsync(field);
            Assert.Equal(values, (string[])col.Data);
        }

        [Fact]
        public async Task ColumnKey_Encrypted_Bloom_Prunes_With_Resolver() {
            var id = new DataField<int>("id");
            var secret = new DataField<string>("secret");
            var schema = new ParquetSchema(id, secret);

            const string footerKey = "01234567891234FK";
            const string columnKey = "01234567891234CK";

            var writeOptions = new ParquetOptions {
                FooterEncryptionKey = footerKey,
                BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions> {
                    [secret.Name] = new() { EnableBloomFilters = true }
                }
            };
            writeOptions.ColumnKeys[secret.Name] = new ParquetOptions.ColumnKeySpec(columnKey);

            byte[] file;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, formatOptions: writeOptions)) {
                    using ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup();
                    await rowGroupWriter.WriteColumnAsync(new DataColumn(id, new[] { 1, 2, 3 }));
                    await rowGroupWriter.WriteColumnAsync(new DataColumn(secret, new[] { "alpha", "beta", "gamma" }));
                }

                file = ms.ToArray();
            }

            using(ParquetReader reader = await ParquetReader.CreateAsync(new MemoryStream(file, writable: false), new ParquetOptions {
                FooterEncryptionKey = footerKey,
                ColumnKeyResolver = (path, _) => string.Join(".", path) == secret.Name ? columnKey : null,
                BloomFilterOptionsByColumn = new Dictionary<string, ParquetOptions.BloomFilterOptions> {
                    [secret.Name] = new() { EnableBloomFilters = true }
                }
            }))
            using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                Assert.True(rg.MightMatchEquals(secret, "beta"));
                Assert.False(rg.MightMatchEquals(secret, "does-not-exist"));

                DataColumn secretColumn = await rg.ReadColumnAsync(secret);
                Assert.Equal(new[] { "alpha", "beta", "gamma" }, (string[])secretColumn.Data);
            }
        }
    }
}
