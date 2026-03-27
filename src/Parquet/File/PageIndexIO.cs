using System;
using System.IO;
using Parquet.Encryption;
using Parquet.Meta;
using Parquet.Meta.Proto;

namespace Parquet.File {
    /// <summary>
    /// Utilities to read page indexes using <see cref="ColumnChunk.OffsetIndexOffset"/>
    /// and <see cref="ColumnChunk.ColumnIndexOffset"/>.
    /// </summary>
    internal static class PageIndexIO {
        public static (long Offset, int Length) WriteOffsetIndex(
            Stream output,
            OffsetIndex offsetIndex,
            ColumnChunk columnChunk,
            Func<Stream, ThriftCompactProtocolWriter> writerFactory,
            EncryptionBase? encrypter = null,
            short rowGroupOrdinal = 0,
            short columnOrdinal = 0) {
            if(output == null)
                throw new ArgumentNullException(nameof(output));
            if(offsetIndex == null)
                throw new ArgumentNullException(nameof(offsetIndex));
            if(columnChunk == null)
                throw new ArgumentNullException(nameof(columnChunk));
            if(writerFactory == null)
                throw new ArgumentNullException(nameof(writerFactory));
            if(!output.CanWrite)
                throw new InvalidOperationException("Output stream not writable.");

            byte[] bytes;
            using(var ms = new MemoryStream()) {
                offsetIndex.Write(writerFactory(ms));
                bytes = ms.ToArray();
            }

            long offset = output.Position;
            WriteModule(output, bytes, encrypter == null ? null : encrypter.EncryptOffsetIndex(bytes, rowGroupOrdinal, columnOrdinal));

            int length = checked((int)(output.Position - offset));
            columnChunk.OffsetIndexOffset = offset;
            columnChunk.OffsetIndexLength = length;
            return (offset, length);
        }

        public static (long Offset, int Length) WriteColumnIndex(
            Stream output,
            ColumnIndex columnIndex,
            ColumnChunk columnChunk,
            Func<Stream, ThriftCompactProtocolWriter> writerFactory,
            EncryptionBase? encrypter = null,
            short rowGroupOrdinal = 0,
            short columnOrdinal = 0) {
            if(output == null)
                throw new ArgumentNullException(nameof(output));
            if(columnIndex == null)
                throw new ArgumentNullException(nameof(columnIndex));
            if(columnChunk == null)
                throw new ArgumentNullException(nameof(columnChunk));
            if(writerFactory == null)
                throw new ArgumentNullException(nameof(writerFactory));
            if(!output.CanWrite)
                throw new InvalidOperationException("Output stream not writable.");

            byte[] bytes;
            using(var ms = new MemoryStream()) {
                columnIndex.Write(writerFactory(ms));
                bytes = ms.ToArray();
            }

            long offset = output.Position;
            WriteModule(output, bytes, encrypter == null ? null : encrypter.EncryptColumnIndex(bytes, rowGroupOrdinal, columnOrdinal));

            int length = checked((int)(output.Position - offset));
            columnChunk.ColumnIndexOffset = offset;
            columnChunk.ColumnIndexLength = length;
            return (offset, length);
        }

        public static OffsetIndex ReadOffsetIndex(
            Stream input,
            ColumnChunk columnChunk,
            Func<Stream, ThriftCompactProtocolReader> readerFactory) {
            if(input == null)
                throw new ArgumentNullException(nameof(input));
            if(columnChunk == null)
                throw new ArgumentNullException(nameof(columnChunk));
            if(readerFactory == null)
                throw new ArgumentNullException(nameof(readerFactory));
            if(!input.CanRead)
                throw new InvalidOperationException("Input stream not readable.");
            if(!input.CanSeek)
                throw new InvalidOperationException("Input stream must be seekable to read page indexes.");
            if(!columnChunk.OffsetIndexOffset.HasValue)
                throw new InvalidOperationException("ColumnChunk does not contain OffsetIndexOffset.");

            input.Seek(columnChunk.OffsetIndexOffset.Value, SeekOrigin.Begin);
            return OffsetIndex.Read(readerFactory(input));
        }

        public static ColumnIndex ReadColumnIndex(
            Stream input,
            ColumnChunk columnChunk,
            Func<Stream, ThriftCompactProtocolReader> readerFactory) {
            if(input == null)
                throw new ArgumentNullException(nameof(input));
            if(columnChunk == null)
                throw new ArgumentNullException(nameof(columnChunk));
            if(readerFactory == null)
                throw new ArgumentNullException(nameof(readerFactory));
            if(!input.CanRead)
                throw new InvalidOperationException("Input stream not readable.");
            if(!input.CanSeek)
                throw new InvalidOperationException("Input stream must be seekable to read page indexes.");
            if(!columnChunk.ColumnIndexOffset.HasValue)
                throw new InvalidOperationException("ColumnChunk does not contain ColumnIndexOffset.");

            input.Seek(columnChunk.ColumnIndexOffset.Value, SeekOrigin.Begin);
            return ColumnIndex.Read(readerFactory(input));
        }

        public static OffsetIndex ReadEncryptedOffsetIndex(
            Stream input,
            ColumnChunk columnChunk,
            EncryptionBase decrypter,
            short rowGroupOrdinal,
            short columnOrdinal,
            Func<byte[], ThriftCompactProtocolReader> readerFactory) {
            if(input == null)
                throw new ArgumentNullException(nameof(input));
            if(columnChunk == null)
                throw new ArgumentNullException(nameof(columnChunk));
            if(decrypter == null)
                throw new ArgumentNullException(nameof(decrypter));
            if(readerFactory == null)
                throw new ArgumentNullException(nameof(readerFactory));
            if(!input.CanRead)
                throw new InvalidOperationException("Input stream not readable.");
            if(!input.CanSeek)
                throw new InvalidOperationException("Input stream must be seekable to read page indexes.");
            if(!columnChunk.OffsetIndexOffset.HasValue)
                throw new InvalidOperationException("ColumnChunk does not contain OffsetIndexOffset.");

            input.Seek(columnChunk.OffsetIndexOffset.Value, SeekOrigin.Begin);
            byte[] plain = decrypter.DecryptOffsetIndex(
                new ThriftCompactProtocolReader(input),
                rowGroupOrdinal,
                columnOrdinal);
            return OffsetIndex.Read(readerFactory(plain));
        }

        public static ColumnIndex ReadEncryptedColumnIndex(
            Stream input,
            ColumnChunk columnChunk,
            EncryptionBase decrypter,
            short rowGroupOrdinal,
            short columnOrdinal,
            Func<byte[], ThriftCompactProtocolReader> readerFactory) {
            if(input == null)
                throw new ArgumentNullException(nameof(input));
            if(columnChunk == null)
                throw new ArgumentNullException(nameof(columnChunk));
            if(decrypter == null)
                throw new ArgumentNullException(nameof(decrypter));
            if(readerFactory == null)
                throw new ArgumentNullException(nameof(readerFactory));
            if(!input.CanRead)
                throw new InvalidOperationException("Input stream not readable.");
            if(!input.CanSeek)
                throw new InvalidOperationException("Input stream must be seekable to read page indexes.");
            if(!columnChunk.ColumnIndexOffset.HasValue)
                throw new InvalidOperationException("ColumnChunk does not contain ColumnIndexOffset.");

            input.Seek(columnChunk.ColumnIndexOffset.Value, SeekOrigin.Begin);
            byte[] plain = decrypter.DecryptColumnIndex(
                new ThriftCompactProtocolReader(input),
                rowGroupOrdinal,
                columnOrdinal);
            return ColumnIndex.Read(readerFactory(plain));
        }

        private static void WriteModule(Stream output, byte[] plainBytes, byte[]? encryptedBytes) {
            byte[] bytesToWrite = encryptedBytes ?? plainBytes;
            output.Write(bytesToWrite, 0, bytesToWrite.Length);
        }
    }
}
