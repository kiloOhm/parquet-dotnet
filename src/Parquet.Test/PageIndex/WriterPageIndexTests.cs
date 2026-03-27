using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.File;
using Parquet.Meta;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.PageIndex {
    public sealed class WriterPageIndexTests : TestBase {
        private static async Task<byte[]> RemovePageIndexesFromFooterAsync(byte[] fileBytes) {
            using var input = new MemoryStream(fileBytes, writable: false);
            using ParquetReader reader = await ParquetReader.CreateAsync(input);

            FileMetaData metadata = reader.Metadata!;
            foreach(RowGroup rowGroup in metadata.RowGroups) {
                foreach(ColumnChunk columnChunk in rowGroup.Columns) {
                    columnChunk.OffsetIndexOffset = null;
                    columnChunk.OffsetIndexLength = null;
                    columnChunk.ColumnIndexOffset = null;
                    columnChunk.ColumnIndexLength = null;
                }
            }

            int originalFooterLength = BitConverter.ToInt32(fileBytes, fileBytes.Length - 8);
            int footerStart = fileBytes.Length - 8 - originalFooterLength;

            using var output = new MemoryStream();
            output.Write(fileBytes, 0, footerStart);

            var footer = new ThriftFooter(metadata);
            long newFooterLength = footer.Write(output);
            output.WriteInt32((int)newFooterLength);
            byte[] magic = System.Text.Encoding.ASCII.GetBytes("PAR1");
            output.Write(magic, 0, magic.Length);

            return output.ToArray();
        }

        [Fact]
        public async Task Write_With_PageIndexes_Then_Read_Them_Back() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            var field = (DataField)schema.Fields[0];
            var column = new DataColumn(field, new[] { 1, 2, 3, 4 });

            using var ms = new System.IO.MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(column);
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            ColumnChunk chunk = reader.Metadata!.RowGroups[0].Columns[0];
            Assert.NotNull(chunk.OffsetIndexOffset);
            Assert.NotNull(chunk.OffsetIndexLength);
            Assert.NotNull(chunk.ColumnIndexOffset);
            Assert.NotNull(chunk.ColumnIndexLength);
            Assert.NotNull(reader.Metadata.ColumnOrders);
            Assert.Single(reader.Metadata.ColumnOrders!);
            Assert.NotNull(reader.Metadata.ColumnOrders![0].TYPEORDER);

            DataField readField = reader.Schema.GetDataFields().Single();
            OffsetIndex? offsetIndex = rowGroupReader.GetOffsetIndex(readField);
            ColumnIndex? columnIndex = rowGroupReader.GetColumnIndex(readField);

            Assert.NotNull(offsetIndex);
            Assert.NotNull(columnIndex);
            Assert.Single(offsetIndex!.PageLocations);
            Assert.Equal(0, offsetIndex.PageLocations[0].FirstRowIndex);
            Assert.True(offsetIndex.PageLocations[0].CompressedPageSize > 0);
            Assert.True(chunk.ColumnIndexOffset >= offsetIndex.PageLocations[0].Offset + offsetIndex.PageLocations[0].CompressedPageSize);
            Assert.True(chunk.OffsetIndexOffset >= chunk.ColumnIndexOffset + chunk.ColumnIndexLength);
            Assert.Equal(BoundaryOrder.UNORDERED, columnIndex!.BoundaryOrder);
            Assert.Equal(new bool[] { false }, columnIndex.NullPages);
            Assert.Single(columnIndex.MinValues);
            Assert.Single(columnIndex.MaxValues);
        }

        [Fact]
        public async Task Write_With_PageIndexes_Then_Read_Them_Back_After_Async_Dispose() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            DataField field = schema.GetDataFields().Single();
            var column = new DataColumn(field, new[] { 1, 2, 3, 4, 5, 6 });
            var options = new ParquetOptions {
                DataPageRowCountLimit = 2
            };

            using var ms = new MemoryStream();
            await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(column);
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            DataField readField = reader.Schema.GetDataFields().Single();
            OffsetIndex? offsetIndex = rowGroupReader.GetOffsetIndex(readField);
            ColumnIndex? columnIndex = rowGroupReader.GetColumnIndex(readField);

            Assert.NotNull(offsetIndex);
            Assert.NotNull(columnIndex);
            Assert.Equal(new long[] { 0, 2, 4 }, offsetIndex!.PageLocations.Select(pl => pl.FirstRowIndex).ToArray());
            Assert.Equal(3, columnIndex!.NullPages.Count);
        }

        [Fact]
        public async Task Write_With_Invalid_RowCountLimit_Throws() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            DataField field = schema.GetDataFields().Single();
            var options = new ParquetOptions {
                DataPageRowCountLimit = 0
            };

            using var ms = new MemoryStream();
            using ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options);
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => {
                await rowGroup.WriteColumnAsync(new DataColumn(field, new[] { 1, 2, 3 }));
            });
        }

        [Fact]
        public async Task Write_BoolColumn_Writes_OffsetIndex_But_Skips_ColumnIndex() {
            var schema = new ParquetSchema(new DataField<bool>("flag"));
            var field = (DataField)schema.Fields[0];
            var column = new DataColumn(field, new[] { true, false, true });

            using var ms = new System.IO.MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(column);
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            ColumnChunk chunk = reader.Metadata!.RowGroups[0].Columns[0];
            Assert.NotNull(chunk.OffsetIndexOffset);
            Assert.NotNull(chunk.OffsetIndexLength);
            Assert.Null(chunk.ColumnIndexOffset);
            Assert.Null(chunk.ColumnIndexLength);

            DataField readField = reader.Schema.GetDataFields().Single();
            OffsetIndex? offsetIndex = rowGroupReader.GetOffsetIndex(readField);
            Assert.NotNull(offsetIndex);
            Assert.True(chunk.OffsetIndexOffset >= offsetIndex!.PageLocations[0].Offset + offsetIndex.PageLocations[0].CompressedPageSize);
            Assert.Null(rowGroupReader.GetColumnIndex(readField));
        }

        [Fact]
        public async Task Write_ColumnKeyEncryptedColumn_Writes_ColumnKeyEncrypted_PageIndexes() {
            var schema = new ParquetSchema(new DataField<int>("secret"));
            DataField field = schema.GetDataFields().Single();
            var options = new ParquetOptions {
                FooterEncryptionKey = "footerKey-16byte"
            };
            options.ColumnKeys["secret"] = new ParquetOptions.ColumnKeySpec("columnKey-16byte");

            using var ms = new MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(new DataColumn(field, new[] { 10, 20, 30, 40 }));
            }

            ms.Position = 0;
            var readOptions = new ParquetOptions {
                FooterEncryptionKey = options.FooterEncryptionKey,
                ColumnKeyResolver = (path, _) => string.Join(".", path) == "secret" ? "columnKey-16byte" : null
            };

            using ParquetReader reader = await ParquetReader.CreateAsync(ms, readOptions);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            ColumnChunk chunk = reader.Metadata!.RowGroups[0].Columns[0];
            Assert.NotNull(chunk.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY);

            DataField readField = reader.Schema.GetDataFields().Single();
            OffsetIndex? offsetIndex = rowGroupReader.GetOffsetIndex(readField);
            ColumnIndex? columnIndex = rowGroupReader.GetColumnIndex(readField);
            ParquetColumnPageReader pageReader = await rowGroupReader.OpenColumnPageReaderAsync(readField);
            ParquetDataPage page = await pageReader.ReadPageAsync(0);

            Assert.NotNull(offsetIndex);
            Assert.NotNull(columnIndex);
            Assert.Single(offsetIndex!.PageLocations);
            Assert.Equal(new[] { 10, 20, 30, 40 }, (int[])page.Column.Data);
        }

        [Fact]
        public async Task Write_With_RowCountLimit_Splits_Into_Multiple_DataPages() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            var field = (DataField)schema.Fields[0];
            var column = new DataColumn(field, Enumerable.Range(1, 5).ToArray());
            var options = new ParquetOptions {
                DataPageRowCountLimit = 2
            };

            using var ms = new System.IO.MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(column);
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            DataField readField = reader.Schema.GetDataFields().Single();
            OffsetIndex? offsetIndex = rowGroupReader.GetOffsetIndex(readField);
            ColumnIndex? columnIndex = rowGroupReader.GetColumnIndex(readField);

            Assert.NotNull(offsetIndex);
            Assert.NotNull(columnIndex);
            Assert.Equal(3, offsetIndex!.PageLocations.Count);
            Assert.Equal(new long[] { 0, 2, 4 }, offsetIndex.PageLocations.Select(pl => pl.FirstRowIndex).ToArray());
            Assert.Equal(new bool[] { false, false, false }, columnIndex!.NullPages);
            Assert.Equal(3, columnIndex.MinValues.Count);
            Assert.Equal(3, columnIndex.MaxValues.Count);
        }

        [Fact]
        public async Task Write_RepeatedColumn_Splits_Only_On_RowBoundaries() {
            var schema = new ParquetSchema(new DataField<System.Collections.Generic.IEnumerable<int>>("items"));
            var field = schema.GetDataFields().Single();
            var column = new DataColumn(field,
                new[] { 1, 2, 3, 4, 5 },
                new[] { 0, 1, 1, 0, 1 });
            var options = new ParquetOptions {
                DataPageRowCountLimit = 1
            };

            using var ms = new System.IO.MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(column);
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            DataField readField = reader.Schema.GetDataFields().Single();
            OffsetIndex? offsetIndex = rowGroupReader.GetOffsetIndex(readField);
            DataColumn roundTrip = await rowGroupReader.ReadColumnAsync(readField);

            Assert.NotNull(offsetIndex);
            Assert.Equal(2, offsetIndex!.PageLocations.Count);
            Assert.Equal(new long[] { 0, 1 }, offsetIndex.PageLocations.Select(pl => pl.FirstRowIndex).ToArray());
            Assert.Equal(new int?[] { 1, 2, 3, 4, 5 }, roundTrip.Data.Cast<int?>().ToArray());
            Assert.Equal(new int[] { 0, 1, 1, 0, 1 }, roundTrip.RepetitionLevels);
        }

        [Fact]
        public async Task ReadColumnPagesAsync_Reads_Only_Selected_PlainPages() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            var field = (DataField)schema.Fields[0];
            var column = new DataColumn(field, Enumerable.Range(1, 6).ToArray());
            var options = new ParquetOptions {
                DataPageRowCountLimit = 2
            };

            using var ms = new System.IO.MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(column);
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            DataField readField = reader.Schema.GetDataFields().Single();
            DataColumn selected = await rowGroupReader.ReadColumnPagesAsync(readField, new[] { 1, 2 });

            Assert.Equal(new[] { 3, 4, 5, 6 }, (int[])selected.Data);
        }

        [Fact]
        public async Task ReadColumnPagesAsync_Reads_Only_Selected_DictionaryPages() {
            var schema = new ParquetSchema(new DataField<string>("name"));
            var field = (DataField)schema.Fields[0];
            var column = new DataColumn(field, new[] { "a", "a", "b", "b", "c", "c" });
            var options = new ParquetOptions {
                DataPageRowCountLimit = 2,
                UseDictionaryEncoding = true
            };

            using var ms = new System.IO.MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(column);
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            DataField readField = reader.Schema.GetDataFields().Single();
            DataColumn selected = await rowGroupReader.ReadColumnPagesAsync(readField, new[] { 2 });

            Assert.Equal(new[] { "c", "c" }, (string[])selected.Data);
        }

        [Fact]
        public async Task ReadColumnPagesAsync_Reads_Only_Selected_RepeatedPages() {
            var schema = new ParquetSchema(new DataField<System.Collections.Generic.IEnumerable<int>>("items"));
            var field = schema.GetDataFields().Single();
            var column = new DataColumn(field,
                new[] { 1, 2, 3, 4, 5 },
                new[] { 0, 1, 1, 0, 1 });
            var options = new ParquetOptions {
                DataPageRowCountLimit = 1
            };

            using var ms = new System.IO.MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(column);
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            DataField readField = reader.Schema.GetDataFields().Single();
            DataColumn selected = await rowGroupReader.ReadColumnPagesAsync(readField, new[] { 1 });

            Assert.Equal(new int?[] { 4, 5 }, selected.Data.Cast<int?>().ToArray());
            Assert.Equal(new[] { 0, 1 }, selected.RepetitionLevels);
        }

        [Fact]
        public async Task OpenColumnPageReader_Reads_Single_Page_With_Metadata() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            var field = (DataField)schema.Fields[0];
            var column = new DataColumn(field, Enumerable.Range(1, 6).ToArray());
            var options = new ParquetOptions {
                DataPageRowCountLimit = 2
            };

            using var ms = new System.IO.MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(column);
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            ParquetColumnPageReader pageReader = rowGroupReader.OpenColumnPageReader(reader.Schema.GetDataFields().Single());
            ParquetDataPage page = await pageReader.ReadPageAsync(1);

            Assert.Equal(3, pageReader.PageCount);
            Assert.NotNull(pageReader.ColumnIndex);
            Assert.Equal(1, page.Ordinal);
            Assert.Equal(2, page.Location.FirstRowIndex);
            Assert.Equal(2, page.RowCount);
            Assert.Equal(new[] { 3, 4 }, (int[])page.Column.Data);
        }

        [Fact]
        public async Task OpenColumnPageReader_Falls_Back_To_Scanning_When_File_Has_No_PageIndexes() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            var field = (DataField)schema.Fields[0];
            var column = new DataColumn(field, Enumerable.Range(1, 6).ToArray());
            var options = new ParquetOptions {
                DataPageRowCountLimit = 2
            };

            byte[] fileBytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                    using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteColumnAsync(column);
                }

                fileBytes = await RemovePageIndexesFromFooterAsync(ms.ToArray());
            }

            using var stripped = new MemoryStream(fileBytes, writable: false);
            using ParquetReader reader = await ParquetReader.CreateAsync(stripped);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            DataField readField = reader.Schema.GetDataFields().Single();
            Assert.Null(rowGroupReader.GetOffsetIndex(readField));
            Assert.Null(rowGroupReader.GetColumnIndex(readField));

            ParquetColumnPageReader pageReader = await rowGroupReader.OpenColumnPageReaderAsync(readField);
            ParquetDataPage page = await pageReader.ReadPageAsync(2);

            Assert.Equal(3, pageReader.PageCount);
            Assert.Null(pageReader.ColumnIndex);
            Assert.Equal(4, page.Location.FirstRowIndex);
            Assert.Equal(new[] { 5, 6 }, (int[])page.Column.Data);
        }

        [Fact]
        public async Task OpenColumnPageReader_Computes_ColumnIndex_When_File_Has_No_PageIndexes() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            var field = (DataField)schema.Fields[0];
            var column = new DataColumn(field, new[] { 9, 1, 7, 3, 5, 11 });
            var options = new ParquetOptions {
                DataPageRowCountLimit = 2
            };

            byte[] fileBytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                    using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteColumnAsync(column);
                }

                fileBytes = await RemovePageIndexesFromFooterAsync(ms.ToArray());
            }

            using var stripped = new MemoryStream(fileBytes, writable: false);
            using ParquetReader reader = await ParquetReader.CreateAsync(stripped);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            DataField readField = reader.Schema.GetDataFields().Single();
            ParquetColumnPageReader pageReader = await rowGroupReader.OpenColumnPageReaderAsync(readField);

            ColumnIndex? computedColumnIndex = await pageReader.GetColumnIndexAsync();

            Assert.NotNull(computedColumnIndex);
            Assert.Same(computedColumnIndex, pageReader.ColumnIndex);
            Assert.Equal(3, computedColumnIndex!.NullPages.Count);
            Assert.Equal(new bool[] { false, false, false }, computedColumnIndex.NullPages);
            Assert.Equal(new long[] { 0, 0, 0 }, computedColumnIndex.NullCounts!.ToArray());
            Assert.Equal(new[] { 1, 3, 5 }, computedColumnIndex.MinValues.Select(value => BitConverter.ToInt32(value, 0)).ToArray());
            Assert.Equal(new[] { 9, 7, 11 }, computedColumnIndex.MaxValues.Select(value => BitConverter.ToInt32(value, 0)).ToArray());

            ColumnIndex? cachedColumnIndex = await rowGroupReader.GetOrCreateColumnIndexAsync(readField);
            Assert.Same(computedColumnIndex, cachedColumnIndex);
        }

        [Fact]
        public async Task OpenColumnPageReader_Leaves_ColumnIndex_Null_For_Unsupported_Fallback_Type() {
            var schema = new ParquetSchema(new DataField<bool>("flag"));
            var field = (DataField)schema.Fields[0];
            var column = new DataColumn(field, new[] { true, false, true, false });
            var options = new ParquetOptions {
                DataPageRowCountLimit = 2
            };

            byte[] fileBytes;
            using(var ms = new MemoryStream()) {
                using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                    using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteColumnAsync(column);
                }

                fileBytes = await RemovePageIndexesFromFooterAsync(ms.ToArray());
            }

            using var stripped = new MemoryStream(fileBytes, writable: false);
            using ParquetReader reader = await ParquetReader.CreateAsync(stripped);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            DataField readField = reader.Schema.GetDataFields().Single();
            ParquetColumnPageReader pageReader = await rowGroupReader.OpenColumnPageReaderAsync(readField);

            Assert.Null(await pageReader.GetColumnIndexAsync());
            Assert.Null(pageReader.ColumnIndex);
            Assert.Null(await rowGroupReader.GetOrCreateColumnIndexAsync(readField));
        }

        [Fact]
        public async Task Write_EncryptedColumn_Writes_Encrypted_PageIndexes() {
            var schema = new ParquetSchema(new DataField<int>("id"));
            var field = (DataField)schema.Fields[0];
            var column = new DataColumn(field, new[] { 10, 20, 30, 40 });
            var options = new ParquetOptions {
                FooterEncryptionKey = "footerKey-16byte"
            };

            using var ms = new System.IO.MemoryStream();
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms, options)) {
                using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteColumnAsync(column);
            }

            ms.Position = 0;
            using ParquetReader reader = await ParquetReader.CreateAsync(ms, options);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);

            ColumnChunk chunk = reader.Metadata!.RowGroups[0].Columns[0];
            Assert.NotNull(chunk.CryptoMetadata);
            Assert.NotNull(chunk.OffsetIndexOffset);
            Assert.NotNull(chunk.ColumnIndexOffset);

            DataField readField = reader.Schema.GetDataFields().Single();
            OffsetIndex? offsetIndex = rowGroupReader.GetOffsetIndex(readField);
            ColumnIndex? columnIndex = rowGroupReader.GetColumnIndex(readField);

            Assert.NotNull(offsetIndex);
            Assert.NotNull(columnIndex);
            Assert.Single(offsetIndex!.PageLocations);
            Assert.Equal(new long[] { 0 }, columnIndex!.NullCounts!);
        }
    }
}
