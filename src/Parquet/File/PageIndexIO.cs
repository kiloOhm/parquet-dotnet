using System;
using System.IO;
using Parquet.Encryption;
using Parquet.Meta;
using Parquet.Meta.Proto;

namespace Parquet.File;

internal static class PageIndexIO {
    public static OffsetIndex ReadOffsetIndex(
        Stream input,
        ColumnChunk columnChunk,
        Func<Stream, ThriftCompactProtocolReader> readerFactory) {
        ValidateRead(input, columnChunk, columnChunk.OffsetIndexOffset, nameof(columnChunk.OffsetIndexOffset));
        input.Seek(columnChunk.OffsetIndexOffset!.Value, SeekOrigin.Begin);
        return OffsetIndex.Read(readerFactory(input));
    }

    public static ColumnIndex ReadColumnIndex(
        Stream input,
        ColumnChunk columnChunk,
        Func<Stream, ThriftCompactProtocolReader> readerFactory) {
        ValidateRead(input, columnChunk, columnChunk.ColumnIndexOffset, nameof(columnChunk.ColumnIndexOffset));
        input.Seek(columnChunk.ColumnIndexOffset!.Value, SeekOrigin.Begin);
        return ColumnIndex.Read(readerFactory(input));
    }

    public static OffsetIndex ReadEncryptedOffsetIndex(
        Stream input,
        ColumnChunk columnChunk,
        ParquetCryptoContext cryptoContext,
        short rowGroupOrdinal,
        short columnOrdinal) {
        ValidateRead(input, columnChunk, columnChunk.OffsetIndexOffset, nameof(columnChunk.OffsetIndexOffset));
        input.Seek(columnChunk.OffsetIndexOffset!.Value, SeekOrigin.Begin);
        byte[] plain = cryptoContext.Decrypt(
            input,
            ParquetModuleType.OffsetIndex,
            rowGroupOrdinal,
            columnOrdinal);
        using var stream = new MemoryStream(plain, writable: false);
        OffsetIndex index = OffsetIndex.Read(new ThriftCompactProtocolReader(stream));
        ParquetCryptoContext.ValidateTrailingPadding(plain, stream.Position, "offset index");
        return index;
    }

    public static ColumnIndex ReadEncryptedColumnIndex(
        Stream input,
        ColumnChunk columnChunk,
        ParquetCryptoContext cryptoContext,
        short rowGroupOrdinal,
        short columnOrdinal) {
        ValidateRead(input, columnChunk, columnChunk.ColumnIndexOffset, nameof(columnChunk.ColumnIndexOffset));
        input.Seek(columnChunk.ColumnIndexOffset!.Value, SeekOrigin.Begin);
        byte[] plain = cryptoContext.Decrypt(
            input,
            ParquetModuleType.ColumnIndex,
            rowGroupOrdinal,
            columnOrdinal);
        using var stream = new MemoryStream(plain, writable: false);
        ColumnIndex index = ColumnIndex.Read(new ThriftCompactProtocolReader(stream));
        ParquetCryptoContext.ValidateTrailingPadding(plain, stream.Position, "column index");
        return index;
    }

    private static void ValidateRead(
        Stream input,
        ColumnChunk columnChunk,
        long? offset,
        string offsetName) {
        if(input == null)
            throw new ArgumentNullException(nameof(input));
        if(columnChunk == null)
            throw new ArgumentNullException(nameof(columnChunk));
        if(!input.CanRead)
            throw new InvalidOperationException("Input stream is not readable.");
        if(!input.CanSeek)
            throw new InvalidOperationException("Input stream must be seekable to read page indexes.");
        if(!offset.HasValue)
            throw new InvalidOperationException($"Column chunk does not contain {offsetName}.");
    }
}
