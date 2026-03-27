using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class ParquetRowGroupReaderTest : TestBase {
        private static OffsetIndex MakeOffsetIndex() {
            return new OffsetIndex {
                PageLocations = {
                    new PageLocation { Offset = 10, CompressedPageSize = 12, FirstRowIndex = 0 },
                    new PageLocation { Offset = 30, CompressedPageSize = 14, FirstRowIndex = 100 }
                }
            };
        }

        private static ColumnIndex MakeColumnIndex() {
            return new ColumnIndex {
                NullPages = { false, false },
                MinValues = { new byte[] { 1 }, new byte[] { 2 } },
                MaxValues = { new byte[] { 9 }, new byte[] { 10 } },
                BoundaryOrder = BoundaryOrder.ASCENDING,
                NullCounts = new System.Collections.Generic.List<long> { 0, 0 }
            };
        }

        private static void AppendOffsetIndex(MemoryStream stream, ColumnChunk chunk, OffsetIndex index) {
            stream.Position = stream.Length;
            long offset = stream.Position;
            index.Write(new ThriftCompactProtocolWriter(stream));
            chunk.OffsetIndexOffset = offset;
            chunk.OffsetIndexLength = checked((int)(stream.Position - offset));
        }

        private static void AppendColumnIndex(MemoryStream stream, ColumnChunk chunk, ColumnIndex index) {
            stream.Position = stream.Length;
            long offset = stream.Position;
            index.Write(new ThriftCompactProtocolWriter(stream));
            chunk.ColumnIndexOffset = offset;
            chunk.ColumnIndexLength = checked((int)(stream.Position - offset));
        }

        [Theory]
        [InlineData("multi.page.parquet")]
        [InlineData("multi.page.v2.parquet")]
        public async Task GetColumnStatistics_ShouldNotBeEmpty(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                for(int gidx = 0; gidx < reader.RowGroupCount; gidx++) {
                    using(ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0)) {

                        foreach(DataField df in reader.Schema.DataFields) {
                            DataColumnStatistics? stats = rowGroupReader.GetStatistics(df);

                            Assert.NotNull(stats);
                        }
                    }
                }
            }

        }

        [Theory]
        [InlineData("multi.page.parquet")]
        [InlineData("multi.page.v2.parquet")]
        public async Task GetColumnReader_MustFailOnInvalidField(string parquetFile) {
            using(ParquetReader reader = await ParquetReader.CreateAsync(OpenTestFile(parquetFile), leaveStreamOpen: false)) {
                using(ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0)) {

                    Assert.Throws<ArgumentNullException>(() => rowGroupReader.GetStatistics(null!));
                    DataField nonExistingField = new DataField("non_existing_field7862425", typeof(int));
                    Assert.Throws<ParquetException>(() => rowGroupReader.GetStatistics(nonExistingField));
                }
            }
        }

        [Fact]
        public async Task GetPageIndexes_ReturnsIndexes_And_Preserves_Stream_Position() {
            ParquetSchema schema = new ParquetSchema(new DataField<int>("id"));
            DataField<int> field = (DataField<int>)schema.GetDataFields().Single();
            DataColumn column = new DataColumn(field, new int[] { 1, 2, 3, 4 });
            using var ms = new MemoryStream();
            await ms.WriteSingleRowGroupParquetFileAsync(schema, column);
            ms.Position = 0;

            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            ColumnChunk chunk = reader.Metadata!.RowGroups[0].Columns[0];
            AppendOffsetIndex(ms, chunk, MakeOffsetIndex());
            AppendColumnIndex(ms, chunk, MakeColumnIndex());

            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
            DataField rowGroupField = reader.Schema.GetDataFields().Single();

            ms.Position = 7;
            OffsetIndex? offsetIndex = rowGroupReader.GetOffsetIndex(rowGroupField);
            Assert.Equal(7, ms.Position);

            ms.Position = 11;
            ColumnIndex? columnIndex = rowGroupReader.GetColumnIndex(rowGroupField);
            Assert.Equal(11, ms.Position);

            Assert.NotNull(offsetIndex);
            Assert.NotNull(columnIndex);
            Assert.Equal(2, offsetIndex!.PageLocations.Count);
            Assert.Equal(100, offsetIndex.PageLocations[1].FirstRowIndex);
            Assert.Equal(BoundaryOrder.ASCENDING, columnIndex!.BoundaryOrder);
            Assert.Equal(new byte[] { 9 }, columnIndex.MaxValues[0]);
        }

        [Fact]
        public async Task GetPageIndexes_Extensions_Work_For_Interface_Readers() {
            ParquetSchema schema = new ParquetSchema(new DataField<int>("id"));
            DataField<int> field = (DataField<int>)schema.GetDataFields().Single();
            DataColumn column = new DataColumn(field, new int[] { 1, 2, 3, 4 });
            using var ms = new MemoryStream();
            await ms.WriteSingleRowGroupParquetFileAsync(schema, column);
            ms.Position = 0;

            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            ColumnChunk chunk = reader.Metadata!.RowGroups[0].Columns[0];
            AppendOffsetIndex(ms, chunk, MakeOffsetIndex());
            AppendColumnIndex(ms, chunk, MakeColumnIndex());

            IParquetRowGroupReader rowGroupReader = reader.RowGroups.Single();
            DataField rowGroupField = reader.Schema.GetDataFields().Single();

            OffsetIndex? offsetIndex = rowGroupReader.GetOffsetIndex(rowGroupField);
            ColumnIndex? columnIndex = rowGroupReader.GetColumnIndex(rowGroupField);

            Assert.NotNull(offsetIndex);
            Assert.NotNull(columnIndex);
            Assert.Equal(10, offsetIndex!.PageLocations[0].Offset);
            Assert.Equal(new byte[] { 2 }, columnIndex!.MinValues[1]);
        }

        [Fact]
        public async Task GetPageIndexes_Returns_Null_When_Not_Present() {
            ParquetSchema schema = new ParquetSchema(new DataField<int>("id"));
            DataField<int> field = (DataField<int>)schema.GetDataFields().Single();
            DataColumn column = new DataColumn(field, new int[] { 1, 2, 3, 4 });
            using var ms = new MemoryStream();
            await ms.WriteSingleRowGroupParquetFileAsync(schema, column);
            ms.Position = 0;

            using ParquetReader reader = await ParquetReader.CreateAsync(ms);
            ColumnChunk chunk = reader.Metadata!.RowGroups[0].Columns[0];
            chunk.OffsetIndexOffset = null;
            chunk.OffsetIndexLength = null;
            chunk.ColumnIndexOffset = null;
            chunk.ColumnIndexLength = null;
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
            DataField rowGroupField = reader.Schema.GetDataFields().Single();

            Assert.Null(rowGroupReader.GetOffsetIndex(rowGroupField));
            Assert.Null(rowGroupReader.GetColumnIndex(rowGroupField));
        }
    }
}
