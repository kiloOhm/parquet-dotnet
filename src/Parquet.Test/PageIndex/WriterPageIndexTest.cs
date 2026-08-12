using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Parquet.Meta;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.PageIndex;

public class WriterPageIndexTest {
    private static readonly byte[] Key = Enumerable.Range(1, 16).Select(value => (byte)value).ToArray();
    private static readonly byte[] ColumnKey = Enumerable.Range(17, 16).Select(value => (byte)value).ToArray();

    [Fact]
    public async Task WritesPageLocationsAndColumnStatistics() {
        var field = new DataField<int>("value");
        using MemoryStream stream = await WriteAsync(field, [10, 20, 30, 40, 50], new ParquetOptions {
            DataPageRowCountLimit = 2
        });

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream);
        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        OffsetIndex offsetIndex = Assert.IsType<OffsetIndex>(rowGroup.GetOffsetIndex(field));
        ColumnIndex columnIndex = Assert.IsType<ColumnIndex>(rowGroup.GetColumnIndex(field));

        Assert.Equal([0L, 2L, 4L], offsetIndex.PageLocations.Select(page => page.FirstRowIndex));
        Assert.All(offsetIndex.PageLocations, page => Assert.True(page.CompressedPageSize > 0));
        Assert.True(offsetIndex.PageLocations.Zip(offsetIndex.PageLocations.Skip(1),
            (left, right) => left.Offset < right.Offset).All(value => value));
        Assert.Equal(3, columnIndex.MinValues.Count);
        Assert.Equal(3, columnIndex.MaxValues.Count);
        Assert.Equal([0L, 0L, 0L], columnIndex.NullCounts);

        int[] values = new int[5];
        await rowGroup.ReadAsync<int>(field, values);
        Assert.Equal([10, 20, 30, 40, 50], values);
    }

    [Fact]
    public async Task SplitsNullableValuesOnRowBoundaries() {
        var field = new DataField<int?>("value");
        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
            new ParquetSchema(field),
            stream,
            new ParquetOptions { DataPageRowCountLimit = 2 })) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, new int?[] { 10, null, 30, null, 50 });
            rowGroup.CompleteValidate();
        }
        stream.Position = 0;

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream);
        using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
        OffsetIndex offsetIndex = Assert.IsType<OffsetIndex>(rowGroupReader.GetOffsetIndex(field));
        ColumnIndex columnIndex = Assert.IsType<ColumnIndex>(rowGroupReader.GetColumnIndex(field));

        Assert.Equal([0L, 2L, 4L], offsetIndex.PageLocations.Select(page => page.FirstRowIndex));
        Assert.Equal([1L, 1L, 0L], columnIndex.NullCounts);
    }

    [Fact]
    public async Task ScansPageIndexesWhenFooterReferencesAreMissing() {
        var field = new DataField<int>("value");
        using MemoryStream stream = await WriteAsync(field, [10, 20, 30, 40, 50], new ParquetOptions {
            DataPageRowCountLimit = 2
        });

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream);
        ColumnChunk columnChunk = reader.Metadata!.RowGroups[0].Columns[0];
        columnChunk.OffsetIndexOffset = null;
        columnChunk.OffsetIndexLength = null;
        columnChunk.ColumnIndexOffset = null;
        columnChunk.ColumnIndexLength = null;

        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        Assert.Null(rowGroup.GetOffsetIndex(field));
        Assert.Null(rowGroup.GetColumnIndex(field));
        stream.Position = 7;

        OffsetIndex offsetIndex = await rowGroup.GetOrCreateOffsetIndexAsync(field);
        ColumnIndex columnIndex = Assert.IsType<ColumnIndex>(
            await rowGroup.GetOrCreateColumnIndexAsync(field));

        Assert.Equal(7, stream.Position);
        Assert.Equal([0L, 2L, 4L], offsetIndex.PageLocations.Select(page => page.FirstRowIndex));
        Assert.Equal(3, columnIndex.MinValues.Count);
        Assert.Equal(3, columnIndex.MaxValues.Count);
        Assert.Equal([0L, 0L, 0L], columnIndex.NullCounts);
    }

    [Fact]
    public async Task ScansRepeatedPageRowBoundaries() {
        var schema = new ParquetSchema(new DataField<IEnumerable<int>>("value"));
        DataField field = schema.DataFields[0];
        using var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(
            schema,
            stream,
            new ParquetOptions { DataPageRowCountLimit = 1 })) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(
                field,
                new int[] { 10, 20, 30, 40, 50 },
                new int[] { 0, 1, 1, 0, 1 });
            rowGroup.CompleteValidate();
        }
        stream.Position = 0;

        await using ParquetReader reader = await ParquetReader.CreateAsync(stream);
        ColumnChunk columnChunk = reader.Metadata!.RowGroups[0].Columns[0];
        columnChunk.OffsetIndexOffset = null;
        columnChunk.OffsetIndexLength = null;

        using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
        OffsetIndex offsetIndex = await rowGroupReader.GetOrCreateOffsetIndexAsync(field);

        Assert.Equal([0L, 1L], offsetIndex.PageLocations.Select(page => page.FirstRowIndex));
    }

    [Fact]
    public async Task EncryptsPageIndexesWithTheColumnContext() {
        var field = new DataField<int>("value");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(Key)) {
            EncryptFooter = false,
            EncryptAllColumns = false
        };
        encryption.ColumnKeys[field.Path.ToString()] = new ParquetKey(ColumnKey);
        var options = new ParquetOptions { DataPageRowCountLimit = 2, Encryption = encryption };
        using MemoryStream stream = await WriteAsync(field, [1, 2, 3, 4], options);

        var readOptions = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = Key }
        };
        readOptions.Decryption.ColumnKeys[field.Path.ToString()] = ColumnKey;
        await using(ParquetReader reader = await ParquetReader.CreateAsync(stream, readOptions)) {
            using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
            Assert.Equal(2, Assert.IsType<OffsetIndex>(rowGroup.GetOffsetIndex(field)).PageLocations.Count);
            Assert.Equal(2, Assert.IsType<ColumnIndex>(rowGroup.GetColumnIndex(field)).MinValues.Count);
        }

        stream.Position = 0;
        var wrongOptions = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = Key }
        };
        wrongOptions.Decryption.ColumnKeys[field.Path.ToString()] = new byte[16];
        await using ParquetReader wrongReader = await ParquetReader.CreateAsync(stream, wrongOptions);
        using ParquetRowGroupReader wrongRowGroup = wrongReader.OpenRowGroupReader(0);
        Assert.Throws<AuthenticationTagMismatchException>(() => wrongRowGroup.GetOffsetIndex(field));
    }

    [Fact]
    public async Task ScansEncryptedPageIndexesWhenFooterReferencesAreMissing() {
        var field = new DataField<int>("value");
        var encryption = new ParquetEncryptionOptions(new ParquetKey(Key)) {
            EncryptFooter = false,
            EncryptAllColumns = false
        };
        encryption.ColumnKeys[field.Path.ToString()] = new ParquetKey(ColumnKey);
        using MemoryStream stream = await WriteAsync(
            field,
            [1, 2, 3, 4],
            new ParquetOptions { DataPageRowCountLimit = 2, Encryption = encryption });

        var readOptions = new ParquetOptions {
            Decryption = new ParquetDecryptionOptions { FooterKey = Key }
        };
        readOptions.Decryption.ColumnKeys[field.Path.ToString()] = ColumnKey;
        await using ParquetReader reader = await ParquetReader.CreateAsync(stream, readOptions);
        ColumnChunk columnChunk = reader.Metadata!.RowGroups[0].Columns[0];
        columnChunk.OffsetIndexOffset = null;
        columnChunk.OffsetIndexLength = null;
        columnChunk.ColumnIndexOffset = null;
        columnChunk.ColumnIndexLength = null;

        using ParquetRowGroupReader rowGroup = reader.OpenRowGroupReader(0);
        OffsetIndex offsetIndex = await rowGroup.GetOrCreateOffsetIndexAsync(field);
        ColumnIndex columnIndex = Assert.IsType<ColumnIndex>(
            await rowGroup.GetOrCreateColumnIndexAsync(field));

        Assert.Equal([0L, 2L], offsetIndex.PageLocations.Select(page => page.FirstRowIndex));
        Assert.Equal(2, columnIndex.MinValues.Count);
    }

    private static async Task<MemoryStream> WriteAsync(
        DataField<int> field,
        int[] values,
        ParquetOptions options) {
        var stream = new MemoryStream();
        await using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), stream, options)) {
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<int>(field, values.AsMemory());
            rowGroup.CompleteValidate();
        }
        stream.Position = 0;
        return stream;
    }
}
