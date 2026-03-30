using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Encryption;
using Parquet.File;
using Parquet.Meta;
using Parquet.Meta.Proto;
using Parquet.Schema;

namespace Parquet {
    /// <summary>
    /// Operations available on a row group reader, omitting Dispose, which is 
    /// exposed on the implementing class for backward compatibility only.
    /// </summary>
    public interface IParquetRowGroupReader {
        /// <summary>
        /// Exposes raw metadata about this row group
        /// </summary>
        RowGroup RowGroup { get; }

        /// <summary>
        /// Gets the number of rows in this row group
        /// </summary>
        long RowCount { get; }

        /// <summary>
        /// Checks if this field exists in source schema
        /// </summary>
        bool ColumnExists(DataField field);

        /// <summary>
        /// Reads a column from this row group. Unlike writing, columns can be read in any order.
        /// If the column is missing, an exception will be thrown.
        /// </summary>
        Task<DataColumn> ReadColumnAsync(DataField field, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets raw column chunk metadata for this field
        /// </summary>
        ColumnChunk? GetMetadata(DataField field);

        /// <summary>
        /// Get custom key-value metadata for a data field
        /// </summary>
        Dictionary<string, string> GetCustomMetadata(DataField field);

        /// <summary>
        /// Returns data column statistics for a particular data field
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <exception cref="ParquetException"></exception>
        DataColumnStatistics? GetStatistics(DataField field);

        /// <summary>
        /// Uses the column chunk's Bloom filter (if present) to check whether this row group
        /// might contain at least one value equal to <paramref name="value"/> for <paramref name="field"/>.
        /// Returns <c>false</c> iff the Bloom filter definitively rules it out.
        /// If a Bloom filter is not present or disabled, returns <c>true</c> (i.e., "don't prune").
        /// </summary>
        /// <remarks>
        /// This does not read data pages; it only inspects the Bloom filter area.
        /// Stream position is preserved.
        /// </remarks>
        bool MightMatchEquals<T>(DataField field, T value);
    }

    /// <summary>
    /// Reader for Parquet row groups
    /// </summary>
    public class ParquetRowGroupReader : IDisposable, IParquetRowGroupReader {
        private readonly RowGroup _rowGroup;
        private readonly ThriftFooter _footer;
        private readonly Stream _stream;
        private readonly ParquetOptions? _options;
        private readonly Dictionary<FieldPath, ColumnChunk> _pathToChunk = new();
        private readonly List<(short colOrd, ColumnChunk cc)> _encryptedColumnChunks = new();
        private readonly Dictionary<FieldPath, OffsetIndex?> _offsetIndexes = new();
        private readonly Dictionary<FieldPath, ColumnIndex?> _columnIndexes = new();
        private readonly Dictionary<FieldPath, OffsetIndex> _scannedOffsetIndexes = new();
        private readonly Dictionary<FieldPath, ColumnIndex?> _computedColumnIndexes = new();

        internal ParquetRowGroupReader(
           RowGroup rowGroup,
           ThriftFooter footer,
           Stream stream,
           ParquetOptions? parquetOptions) {
            _rowGroup = rowGroup ?? throw new ArgumentNullException(nameof(rowGroup));
            _footer = footer ?? throw new ArgumentNullException(nameof(footer));
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            _options = parquetOptions ?? throw new ArgumentNullException(nameof(parquetOptions));

            //cache chunks
            for(int colIdx = 0; colIdx < rowGroup.Columns.Count; colIdx++) {
                ColumnChunk cc = rowGroup.Columns[colIdx];

                // Case 1: Plain metadata present -> register immediately
                if(cc.MetaData != null) {
                    FieldPath path = _footer.GetPath(cc);
                    _pathToChunk[path] = cc;
                    continue;
                }

                // Case 2: Metadata is encrypted -> DO NOT fail here.
                // We may still be able to read plaintext columns in this file without column keys.
                if(cc.EncryptedColumnMetadata != null) {
                    _encryptedColumnChunks ??= new List<(short colOrd, ColumnChunk cc)>();
                    short colOrd = (short)colIdx;
                    _encryptedColumnChunks.Add((colOrd, cc));
                    // We’ll resolve (decrypt or throw) lazily if the caller asks for this column.
                    continue;
                }
                // Truly malformed chunk (neither MetaData nor EncryptedColumnMetadata)
                throw new InvalidDataException("ColumnChunk is missing both MetaData and EncryptedColumnMetadata.");
            }
        }

        /// <summary>
        /// Exposes raw metadata about this row group
        /// </summary>
        public RowGroup RowGroup => _rowGroup;

        /// <summary>
        /// Gets the number of rows in this row group
        /// </summary>
        public long RowCount => _rowGroup.NumRows;

        /// <summary>
        /// Checks if this field exists in source schema
        /// </summary>
        public bool ColumnExists(DataField field) => GetMetadata(field) != null;


        /// <summary>
        /// Reads a column from this row group. Unlike writing, columns can be read in any order.
        /// If the column is missing, an exception will be thrown.
        /// </summary>
        public Task<DataColumn> ReadColumnAsync(DataField field, CancellationToken cancellationToken = default) {
            ColumnChunk columnChunk = GetMetadata(field)
                ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
            var columnReader = new DataColumnReader(field, _stream,
                columnChunk, ReadColumnStatistics(columnChunk), _footer, _options, _rowGroup);
            return columnReader.ReadAsync(cancellationToken);
        }

        /// <summary>
        /// Reads only the specified data pages from a column chunk.
        /// Use with <see cref="GetOffsetIndex(DataField)"/> and, optionally,
        /// <see cref="GetColumnIndex(DataField)"/> to implement page-level pruning.
        /// </summary>
        /// <param name="field">Field to read.</param>
        /// <param name="pageOrdinals">Zero-based data-page ordinals to include.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A column containing only the selected pages, in page order.</returns>
        public async Task<DataColumn> ReadColumnPagesAsync(
            DataField field,
            IReadOnlyCollection<int> pageOrdinals,
            CancellationToken cancellationToken = default) {
            if(field == null)
                throw new ArgumentNullException(nameof(field));
            if(pageOrdinals == null)
                throw new ArgumentNullException(nameof(pageOrdinals));

            ColumnChunk columnChunk = GetMetadata(field)
                ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
            OffsetIndex offsetIndex = await GetOrCreateOffsetIndexForPageReadsAsync(field, cancellationToken);

            var columnReader = new DataColumnReader(field, _stream,
                columnChunk, ReadColumnStatistics(columnChunk), _footer, _options, _rowGroup);
            return await columnReader.ReadPagesAsync(pageOrdinals, offsetIndex, cancellationToken);
        }

        /// <summary>
        /// Opens a public page reader for the specified field.
        /// </summary>
        /// <param name="field">Field to open a page reader for.</param>
        /// <returns>A page reader bound to the field's column chunk.</returns>
        public ParquetColumnPageReader OpenColumnPageReader(DataField field) {
            return OpenColumnPageReaderAsync(field).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Opens a public page reader for the specified field.
        /// </summary>
        /// <param name="field">Field to open a page reader for.</param>
        /// <param name="cancellationToken">Cancellation token used when scanning page locations.</param>
        /// <returns>A page reader bound to the field's column chunk.</returns>
        public async Task<ParquetColumnPageReader> OpenColumnPageReaderAsync(DataField field, CancellationToken cancellationToken = default) {
            if(field == null)
                throw new ArgumentNullException(nameof(field));

            ColumnChunk columnChunk = GetMetadata(field)
                ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
            OffsetIndex offsetIndex = await GetOrCreateOffsetIndexForPageReadsAsync(field, cancellationToken);
            ColumnIndex? columnIndex = GetColumnIndex(field);

            return new ParquetColumnPageReader(this, field, columnChunk, offsetIndex, columnIndex);
        }

        /// <summary>
        /// Returns the on-disk column index when present, or computes one page-by-page when it is absent.
        /// </summary>
        public async Task<ColumnIndex?> GetOrCreateColumnIndexAsync(
            DataField field,
            CancellationToken cancellationToken = default) {
            ColumnIndex? actualColumnIndex = GetColumnIndex(field);
            if(actualColumnIndex != null)
                return actualColumnIndex;

            FieldPath path = field.Path;
            if(_computedColumnIndexes.TryGetValue(path, out ColumnIndex? cachedColumnIndex))
                return cachedColumnIndex;

            ColumnChunk columnChunk = GetMetadata(field)
                ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
            SchemaElement schemaElement = _footer.GetSchemaElement(columnChunk)
                ?? throw new ParquetException($"cannot find schema element for '{field.Path}'");
            OffsetIndex offsetIndex = await GetOrCreateOffsetIndexForPageReadsAsync(field, cancellationToken);

            var nullPages = new List<bool>(offsetIndex.PageLocations.Count);
            var minValues = new List<byte[]>(offsetIndex.PageLocations.Count);
            var maxValues = new List<byte[]>(offsetIndex.PageLocations.Count);
            var nullCounts = new List<long>(offsetIndex.PageLocations.Count);

            for(int pageOrdinal = 0; pageOrdinal < offsetIndex.PageLocations.Count; pageOrdinal++) {
                DataColumn page = await ReadColumnPagesAsync(field, new[] { pageOrdinal }, cancellationToken);
                var pageStats = new DataColumnStatistics {
                    NullCount = page.NumValues - page.DefinedData.Length
                };

                bool isNullPage = pageStats.NullCount == page.NumValues;
                if(!isNullPage && !TryFillStats(page.DefinedData, 0, page.DefinedData.Length, pageStats)) {
                    _computedColumnIndexes[path] = null;
                    return null;
                }

                Statistics thriftStats = pageStats.ToThriftStatistics(schemaElement);
                nullPages.Add(isNullPage);
                minValues.Add(isNullPage ? Array.Empty<byte>() : thriftStats.MinValue!);
                maxValues.Add(isNullPage ? Array.Empty<byte>() : thriftStats.MaxValue!);
                nullCounts.Add(pageStats.NullCount ?? 0);
            }

            var computedColumnIndex = new ColumnIndex {
                NullPages = nullPages,
                MinValues = minValues,
                MaxValues = maxValues,
                BoundaryOrder = BoundaryOrder.UNORDERED,
                NullCounts = nullCounts
            };

            _computedColumnIndexes[path] = computedColumnIndex;
            return computedColumnIndex;
        }

        private async Task<OffsetIndex> GetOrCreateOffsetIndexForPageReadsAsync(
            DataField field,
            CancellationToken cancellationToken = default) {
            OffsetIndex? actualOffsetIndex = GetOffsetIndex(field);
            if(actualOffsetIndex != null)
                return actualOffsetIndex;

            FieldPath path = field.Path;
            if(_scannedOffsetIndexes.TryGetValue(path, out OffsetIndex? cachedOffsetIndex))
                return cachedOffsetIndex;

            ColumnChunk columnChunk = GetMetadata(field)
                ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
            var columnReader = new DataColumnReader(field, _stream,
                columnChunk, ReadColumnStatistics(columnChunk), _footer, _options, _rowGroup);
            OffsetIndex scannedOffsetIndex = await columnReader.ScanOffsetIndexAsync(cancellationToken);
            _scannedOffsetIndexes[path] = scannedOffsetIndex;
            return scannedOffsetIndex;
        }

        /// <summary>
        /// Gets raw column chunk metadata for this field
        /// </summary>
        public ColumnChunk? GetMetadata(DataField field) {
            if(field == null)
                throw new ArgumentNullException(nameof(field));
            return ResolveChunkFor(field.Path);
        }


        /// <summary>
        /// Get custom key-value metadata for a data field
        /// </summary>
        public Dictionary<string, string> GetCustomMetadata(DataField field) {
            ColumnChunk? cc = GetMetadata(field);
            if(cc?.MetaData?.KeyValueMetadata == null)
                return new();

            return cc.MetaData.KeyValueMetadata.ToDictionary(kv => kv.Key, kv => kv.Value!);
        }

        private DataColumnStatistics? ReadColumnStatistics(ColumnChunk cc) {
            Statistics? st = cc.MetaData?.Statistics;

            if((st == null || (st.MinValue == null && st.MaxValue == null && st.NullCount == null)) &&
                cc.EncryptedColumnMetadata != null &&
                cc.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY != null) {
                // Find path and column ordinal
                FieldPath path = _footer.GetPath(cc);
                short rgOrd = _footer.GetRowGroupOrdinal(_rowGroup);
                short colOrd = (short)_rowGroup.Columns.IndexOf(cc);

                // Resolve (will decrypt and cache MetaData)
                ColumnChunk? resolved = ResolveChunkFor(path);
                if(resolved?.MetaData?.Statistics != null)
                    st = resolved.MetaData.Statistics;
            }

            if(st == null)
                return null;

            SchemaElement? se = _footer.GetSchemaElement(cc)
                ?? throw new ArgumentException("can't find schema element", nameof(cc));

            ParquetPlainEncoder.TryDecode(st.MinValue, se, _options!, out object? min);
            ParquetPlainEncoder.TryDecode(st.MaxValue, se, _options!, out object? max);

            return new DataColumnStatistics(st.NullCount, st.DistinctCount, min, max);
        }



        /// <summary>
        /// Returns data column statistics for a particular data field
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <exception cref="ParquetException"></exception>
        public DataColumnStatistics? GetStatistics(DataField field) {
            ColumnChunk cc = GetMetadata(field) ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
            return ReadColumnStatistics(cc);
        }

        /// <summary>
        /// Gets the row group's offset index for a data field when present.
        /// </summary>
        /// <param name="field">Field to get the offset index for.</param>
        /// <returns>The offset index, or <see langword="null"/> when the column chunk does not contain one.</returns>
        /// <remarks>
        /// This does not read data pages. Stream position is preserved.
        /// </remarks>
        public OffsetIndex? GetOffsetIndex(DataField field) {
            return ReadPageIndex(
                field,
                _offsetIndexes,
                chunk => chunk.OffsetIndexOffset,
                (stream, chunk) => PageIndexIO.ReadOffsetIndex(stream, chunk, s => new ThriftCompactProtocolReader(s)),
                (stream, chunk, decrypter, rowGroupOrdinal, columnOrdinal) => PageIndexIO.ReadEncryptedOffsetIndex(
                    stream,
                    chunk,
                    decrypter,
                    rowGroupOrdinal,
                    columnOrdinal,
                    bytes => new ThriftCompactProtocolReader(new MemoryStream(bytes))),
                "offset index");
        }

        /// <summary>
        /// Gets the row group's column index for a data field when present.
        /// </summary>
        /// <param name="field">Field to get the column index for.</param>
        /// <returns>The column index, or <see langword="null"/> when the column chunk does not contain one.</returns>
        /// <remarks>
        /// This does not read data pages. Stream position is preserved.
        /// </remarks>
        public ColumnIndex? GetColumnIndex(DataField field) {
            return ReadPageIndex(
                field,
                _columnIndexes,
                chunk => chunk.ColumnIndexOffset,
                (stream, chunk) => PageIndexIO.ReadColumnIndex(stream, chunk, s => new ThriftCompactProtocolReader(s)),
                (stream, chunk, decrypter, rowGroupOrdinal, columnOrdinal) => PageIndexIO.ReadEncryptedColumnIndex(
                    stream,
                    chunk,
                    decrypter,
                    rowGroupOrdinal,
                    columnOrdinal,
                    bytes => new ThriftCompactProtocolReader(new MemoryStream(bytes))),
                "column index");
        }

        private TIndex? ReadPageIndex<TIndex>(
            DataField field,
            Dictionary<FieldPath, TIndex?> cache,
            Func<ColumnChunk, long?> getOffset,
            Func<Stream, ColumnChunk, TIndex> readPlain,
            Func<Stream, ColumnChunk, EncryptionBase, short, short, TIndex> readEncrypted,
            string indexName)
            where TIndex : class {
            if(field == null)
                throw new ArgumentNullException(nameof(field));

            FieldPath path = field.Path;
            if(cache.TryGetValue(path, out TIndex? cachedIndex))
                return cachedIndex;

            ColumnChunk cc = GetMetadata(field) ?? throw new ParquetException($"'{field.Path}' does not exist in this file");
            if(!getOffset(cc).HasValue) {
                cache[path] = null;
                return null;
            }

            long originalPosition = _stream.CanSeek ? _stream.Position : 0;
            try {
                TIndex index;
                if(cc.CryptoMetadata == null) {
                    index = readPlain(_stream, cc);
                } else {
                    short columnOrdinal = (short)_rowGroup.Columns.IndexOf(cc);
                    if(columnOrdinal < 0)
                        throw new InvalidDataException("Could not determine column ordinal.");

                    EncryptionBase decrypter = CreateColumnModuleDecrypter(cc, path, indexName);
                    short rowGroupOrdinal = _footer.GetRowGroupOrdinal(_rowGroup);
                    index = readEncrypted(_stream, cc, decrypter, rowGroupOrdinal, columnOrdinal);
                }

                cache[path] = index;
                return index;
            } finally {
                if(_stream.CanSeek)
                    _stream.Seek(originalPosition, SeekOrigin.Begin);
            }
        }

        private ColumnChunk? ResolveChunkFor(FieldPath path) {
            if(_pathToChunk.TryGetValue(path, out ColumnChunk? ccPlain))
                return ccPlain;

            // Try find a matching encrypted chunk by path; if found, decrypt and cache
            for(int i = 0; i < _encryptedColumnChunks.Count; i++) {
                (short colOrd, ColumnChunk? encCc) = _encryptedColumnChunks[i];
                FieldPath encPath = _footer.GetPath(encCc);
                if(!encPath.Equals(path))
                    continue;

                DecryptColumnMetaFor(encCc, encPath, colOrd);
                return encCc; // MetaData now populated & cached
            }

            return null;
        }

        private void DecryptColumnMetaFor(ColumnChunk encCc, FieldPath path, short colOrd) {
            EncryptionBase dec = CreateColumnKeyDecrypter(encCc, path, "metadata");

            // Decrypt ColumnMetaData (needs row-group & column ordinals)
            using var msEnc = new MemoryStream(encCc.EncryptedColumnMetadata!);
            var tpr = new Parquet.Meta.Proto.ThriftCompactProtocolReader(msEnc);

            short rgOrd = _footer.GetRowGroupOrdinal(_rowGroup);     // <-- use your ThriftFooter helper
            byte[] plain = dec.DecryptColumnMetaData(tpr, rgOrd, colOrd);

            using var ms = new MemoryStream(plain);
            var r = new Parquet.Meta.Proto.ThriftCompactProtocolReader(ms);
            ColumnMetaData realMeta = Meta.ColumnMetaData.Read(r);

            encCc.MetaData = realMeta;            // cache the real meta
            _pathToChunk[path] = encCc;         // make lookups fast next time
        }

        private EncryptionBase CreateColumnModuleDecrypter(ColumnChunk cc, FieldPath path, string moduleName) {
            if(cc.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY != null)
                return CreateColumnKeyDecrypter(cc, path, moduleName);

            if(cc.CryptoMetadata?.ENCRYPTIONWITHFOOTERKEY != null) {
                if(_footer.Decrypter?.FooterEncryptionKey == null) {
                    throw new InvalidDataException(
                        $"Column '{path}' has an encrypted {moduleName}, but no footer key/decrypter is available. " +
                        "Provide ParquetOptions.FooterEncryptionKey (and AAD prefix if required).");
                }

                return _footer.Decrypter;
            }

            throw new InvalidOperationException($"Column '{path}' does not have encryption metadata for its {moduleName}.");
        }

        private EncryptionBase CreateColumnKeyDecrypter(ColumnChunk cc, FieldPath path, string moduleName) {
            EncryptionWithColumnKey ck = cc.CryptoMetadata?.ENCRYPTIONWITHCOLUMNKEY
                ?? throw new NotSupportedException(
                    $"Column '{path}' has encrypted {moduleName} but lacks column-key crypto metadata.");

            string? keyString = _options?.ColumnKeyResolver?.Invoke(ck.PathInSchema, ck.KeyMetadata);
            if(string.IsNullOrWhiteSpace(keyString)) {
                throw new NotSupportedException(
                    $"Column '{path}' uses column-key encryption for its {moduleName}. " +
                    "Provide a ColumnKeyResolver in ParquetOptions to supply the key.");
            }

            byte[] columnKey = EncryptionBase.ParseKeyString(keyString!);
            EncryptionBase? ctx = _footer.Decrypter ?? _footer.Encrypter;
            if(ctx == null)
                throw new NotSupportedException($"Column '{path}' {moduleName} is encrypted and AAD context is unavailable.");

            EncryptionBase dec = ctx is AES_GCM_CTR_V1_Encryption
                ? new AES_GCM_CTR_V1_Encryption()
                : new AES_GCM_V1_Encryption();

            dec.AadPrefix = ctx.AadPrefix;
            dec.AadFileUnique = ctx.AadFileUnique;
            dec.FooterEncryptionKey = columnKey;
            return dec;
        }

        private static bool TryFillStats(Array data, int offset, int count, DataColumnStatistics stats) {
            if(count <= 0)
                return true;

            System.Type dataType = data.GetType();
            if(dataType == typeof(byte[])) {
                ParquetPlainEncoder.FillStats(((byte[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(sbyte[])) {
                ParquetPlainEncoder.FillStats(((sbyte[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(short[])) {
                ParquetPlainEncoder.FillStats(((short[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(ushort[])) {
                ParquetPlainEncoder.FillStats(((ushort[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(int[])) {
                ParquetPlainEncoder.FillStats(((int[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(uint[])) {
                ParquetPlainEncoder.FillStats(((uint[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(long[])) {
                ParquetPlainEncoder.FillStats(((long[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(ulong[])) {
                ParquetPlainEncoder.FillStats(((ulong[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(System.Numerics.BigInteger[])) {
                ParquetPlainEncoder.FillStats(((System.Numerics.BigInteger[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(decimal[])) {
                ParquetPlainEncoder.FillStats(((decimal[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(BigDecimal[])) {
                ParquetPlainEncoder.FillStats(((BigDecimal[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(double[])) {
                ParquetPlainEncoder.FillStats(((double[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(float[])) {
                ParquetPlainEncoder.FillStats(((float[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(DateTime[])) {
                ParquetPlainEncoder.FillStats(((DateTime[])data).AsSpan(offset, count), stats);
#if NET6_0_OR_GREATER || NET48
            } else if(dataType == typeof(DateOnly[])) {
                ParquetPlainEncoder.FillStats(((DateOnly[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(TimeOnly[])) {
                ParquetPlainEncoder.FillStats(((TimeOnly[])data).AsSpan(offset, count), stats);
#endif
            } else if(dataType == typeof(TimeSpan[])) {
                ParquetPlainEncoder.FillStats(((TimeSpan[])data).AsSpan(offset, count), stats);
            } else if(dataType == typeof(string[])) {
                ParquetPlainEncoder.FillStats(((string[])data).AsSpan(offset, count), stats);
            } else {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Uses the column chunk's Bloom filter (if present) to check whether this row group
        /// might contain at least one value equal to <paramref name="value"/> for <paramref name="field"/>.
        /// Returns <c>false</c> iff the Bloom filter definitively rules it out.
        /// If a Bloom filter is not present or disabled, returns <c>true</c> (i.e., "don't prune").
        /// </summary>
        /// <remarks>
        /// This does not read data pages; it only inspects the Bloom filter area.
        /// Stream position is preserved.
        /// </remarks>
        public bool MightMatchEquals<T>(DataField field, T value) {
            if(field is null)
                throw new ArgumentNullException(nameof(field));

            ColumnChunk cc = GetMetadata(field)
                ?? throw new ParquetException($"'{field.Path}' does not exist in this file");

            // Prepare stats (may be null in metadata); safe default is no nulls/distincts
            DataColumnStatistics stats = ReadColumnStatistics(cc) ?? new DataColumnStatistics(null, null, null, null);

            // Preserve stream position; bloom probing may seek.
            long pos = _stream.CanSeek ? _stream.Position : 0;
            try {
                var dcr = new DataColumnReader(field, _stream, cc, stats, _footer, _options!, _rowGroup);
                return dcr.MightMatchEquals(value);
            } finally {
                if(_stream.CanSeek)
                    _stream.Seek(pos, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Dispose isn't required, retained for backward compatibility
        /// </summary>
        public void Dispose() { }
    }
}
