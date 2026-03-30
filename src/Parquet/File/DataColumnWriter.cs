using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IO;
using Parquet.Bloom;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Encryption;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet.File;

class DataColumnWriter {
    private readonly Stream _stream;
    private readonly ThriftFooter _footer;
    private readonly SchemaElement _schemaElement;
    private readonly CompressionMethod _compressionMethod;
    private readonly CompressionLevel _compressionLevel;
    private readonly Dictionary<string, string>? _keyValueMetadata;
    private readonly ParquetOptions _options;
    private static readonly RecyclableMemoryStreamManager _rmsMgr = new RecyclableMemoryStreamManager();
    private readonly short _rowGroupOrdinal;
    private readonly short _columnOrdinal;
    private short _pageOrdinal; // increments per DATA page only

    public DataColumnWriter(
       Stream stream,
       ThriftFooter footer,
       SchemaElement schemaElement,
       CompressionMethod compressionMethod,
       ParquetOptions options,
       CompressionLevel compressionLevel,
       Dictionary<string, string>? keyValueMetadata,
       short rowGroupOrdinal = 0,
       short columnOrdinal = 0) {
        _stream = stream;
        _footer = footer;
        _schemaElement = schemaElement;
        _compressionMethod = compressionMethod;
        _compressionLevel = compressionLevel;
        _keyValueMetadata = keyValueMetadata;
        _options = options;
        _rmsMgr.Settings.MaximumSmallPoolFreeBytes = options.MaximumSmallPoolFreeBytes;
        _rmsMgr.Settings.MaximumLargePoolFreeBytes = options.MaximumLargePoolFreeBytes;
        _rowGroupOrdinal = rowGroupOrdinal;
        _columnOrdinal = columnOrdinal;
        _pageOrdinal = 0;
    }

    public async Task<ColumnChunk> WriteAsync(
        FieldPath fullPath,
        DataColumn column,
        CancellationToken cancellationToken = default
    ) {
        if(column == null)
            throw new ArgumentNullException(nameof(column));
        column.Field.EnsureAttachedToSchema(nameof(column));

        ColumnChunk chunk = _footer.CreateColumnChunk(
            _compressionMethod, _stream, _schemaElement.Type!.Value, fullPath, column.NumValues, _keyValueMetadata);

        // Global flags
        bool writerHasEncrypter = _footer.Encrypter is not null;
        bool encryptedFooterMode = writerHasEncrypter && !(_options?.UsePlaintextFooter ?? false);

        // Per-column selection
        bool useColumnKey = false;
        byte[]? columnKeyBytes = null;
        byte[]? columnKeyMetadata = null;

        // A footer key is considered "available" only if explicitly set in options
        bool footerKeyAvailable = !string.IsNullOrWhiteSpace(_options?.FooterEncryptionKey);

        // Decide crypto metadata + which encrypter instance to use for THIS column
        Encryption.EncryptionBase? encrForThisColumn = null;

        if(writerHasEncrypter) {
            string pathStr = string.Join(".", fullPath.ToList());

            if(_options?.ColumnKeys != null &&
                _options.ColumnKeys.TryGetValue(pathStr, out ParquetOptions.ColumnKeySpec? spec)) {
                // Column-key column
                useColumnKey = true;
                columnKeyBytes = Encryption.EncryptionBase.ParseKeyString(spec.Key);
                columnKeyMetadata = spec.KeyMetadata;

                chunk.CryptoMetadata = new ColumnCryptoMetaData {
                    ENCRYPTIONWITHCOLUMNKEY = new EncryptionWithColumnKey {
                        PathInSchema = fullPath.ToList(),
                        KeyMetadata = columnKeyMetadata
                    }
                };

                // We'll swap the key on the shared encrypter for this column
                encrForThisColumn = _footer.Encrypter;
            } else if(footerKeyAvailable) {
                // Footer-key column (only if footer key really exists)
                chunk.CryptoMetadata = new ColumnCryptoMetaData {
                    ENCRYPTIONWITHFOOTERKEY = new EncryptionWithFooterKey()
                };
                encrForThisColumn = _footer.Encrypter;
            } else {
                // PF + no footer key â†’ plaintext column (do NOT advertise crypto metadata)
                chunk.CryptoMetadata = null;
                encrForThisColumn = null;
            }
        }

        // If using a column key, temporarily swap it onto the encrypter instance
        byte[]? originalKey = null;
        if(useColumnKey && encrForThisColumn is not null) {
            originalKey = _footer.Encrypter!.FooterEncryptionKey;
            _footer.Encrypter.FooterEncryptionKey = columnKeyBytes!;
        }

        ColumnMetrics metrics;
        try {
            // Writes dictionary + data pages using encrForThisColumn (null => plaintext)
            metrics = await WriteColumnAsync(chunk, column, _schemaElement, encrForThisColumn, cancellationToken);
        } finally {
            if(useColumnKey && encrForThisColumn is not null) {
                _footer.Encrypter!.FooterEncryptionKey = originalKey!;
            }
        }

        chunk.MetaData!.Encodings = metrics.GetUsedEncodings();

        // Populate plaintext ColumnMetaData (in-memory)
        chunk.MetaData.Statistics = column.Statistics.ToThriftStatistics(_schemaElement);
        chunk.MetaData.TotalCompressedSize = metrics.CompressedSize;
        chunk.MetaData.TotalUncompressedSize = metrics.UncompressedSize;

        // If this is a column-key column, emit encrypted_column_metadata and handle PF redaction
        if(useColumnKey && encrForThisColumn is not null) {
            // 1) Serialize ColumnMetaData
            byte[] cmdPlain;
            using(var ms = new MemoryStream()) {
                chunk.MetaData!.Write(new Meta.Proto.ThriftCompactProtocolWriter(ms));
                cmdPlain = ms.ToArray();
            }

            // 2) Encrypt with the column key (swap already active here)
            byte[] encCmd = _footer.Encrypter!.EncryptColumnMetaDataWithKey(
                cmdPlain,
                _rowGroupOrdinal,
                _columnOrdinal,
                columnKeyBytes!);
            chunk.EncryptedColumnMetadata = encCmd;

            // 3) PF: keep redacted plaintext stats; EF: footer serializer will omit meta_data on-wire
            if(!encryptedFooterMode && chunk.MetaData?.Statistics != null) {
                chunk.MetaData.Statistics.MinValue = null;
                chunk.MetaData.Statistics.MaxValue = null;
                chunk.MetaData.Statistics.NullCount = null;
                chunk.MetaData.Statistics.DistinctCount = null;
            }

            if(encryptedFooterMode) {
                // EF mode must keep column-key metadata only in encrypted_column_metadata.
                chunk.OmitMetaDataOnWrite = true;
            }
        }

        return chunk;
    }

    class ColumnMetrics {
        public int CompressedSize;
        public int UncompressedSize;
        public readonly List<PageHeader> Pages = new();
        public readonly List<PageIndexEntry> DataPages = new();

        public List<Encoding> GetUsedEncodings() {
            var r = new HashSet<Encoding>();
            foreach(PageHeader page in Pages) {
                if(page.DictionaryPageHeader != null) {
                    r.Add(page.DictionaryPageHeader.Encoding);
                }
                if(page.DataPageHeader != null) {
                    r.Add(page.DataPageHeader.Encoding);
                    r.Add(page.DataPageHeader.DefinitionLevelEncoding);
                    r.Add(page.DataPageHeader.RepetitionLevelEncoding);
                }
                if(page.DataPageHeaderV2 != null) {
                    r.Add(page.DataPageHeaderV2.Encoding);
                }
            }
            return r.ToList();
        }
    }

    class PageIndexEntry {
        public PageLocation Location { get; set; } = new PageLocation();
        public Statistics? Statistics { get; set; }
        public long? UnencodedByteArrayDataBytes { get; set; }
        public int ValueCount { get; set; }
    }

    class PageWriteMetrics {
        public long Offset { get; set; }
        public int TotalSize { get; set; }
    }

    class PageSlice {
        public int ValueOffset { get; set; }
        public int ValueCount { get; set; }
        public int DefinedValueOffset { get; set; }
        public int DefinedValueCount { get; set; }
        public long FirstRowIndex { get; set; }
    }

    private async Task<PageWriteMetrics> CompressAndWriteAsync(
        PageHeader ph, MemoryStream uncompressedData,
        ColumnMetrics cs,
        Encryption.EncryptionBase? encrForThisColumn,
        CancellationToken cancellationToken) {

        int uncompressedLength = (int)uncompressedData.Length;
        using IMemoryOwner<byte> pageData = await Compressor.Instance.CompressAsync(
            _compressionMethod, _compressionLevel, uncompressedData);
        int compressedLength = pageData.Memory.Length;
        long pageOffset = _stream.Position;

        // ---- PLAINTEXT path for this column ----
        if(encrForThisColumn is null) {
            ph.UncompressedPageSize = uncompressedLength;
            ph.CompressedPageSize = compressedLength;

            //write the header in
            using(MemoryStream headerMs = _rmsMgr.GetStream()) {
                ph.Write(new Meta.Proto.ThriftCompactProtocolWriter(headerMs));
                int headerSize = (int)headerMs.Length;
                headerMs.Position = 0;

                // write header
                await headerMs.CopyToAsync(_stream);

                cs.CompressedSize += headerSize;
                cs.UncompressedSize += headerSize;
            }

            // write data
            await pageData.Memory.CopyToAsync(_stream);

            cs.CompressedSize += ph.CompressedPageSize;
            cs.UncompressedSize += ph.UncompressedPageSize;
            return new PageWriteMetrics {
                Offset = pageOffset,
                TotalSize = ph.CompressedPageSize + GetSerializedPageHeaderSize(ph)
            };
        }

        // ---- ENCRYPTED path for this column ----
        ph.UncompressedPageSize = uncompressedLength;

        byte[] bodyBytes = pageData.Memory.Span.ToArray();

        byte[] encBody = ph.Type == PageType.DICTIONARY_PAGE
            ? encrForThisColumn.EncryptDictionaryPage(bodyBytes, _rowGroupOrdinal, _columnOrdinal)
            : encrForThisColumn.EncryptDataPage(bodyBytes, _rowGroupOrdinal, _columnOrdinal, _pageOrdinal);

        ph.CompressedPageSize = encBody.Length;

        byte[] headerBytes;
        using(MemoryStream headerMs = _rmsMgr.GetStream()) {
            ph.Write(new Meta.Proto.ThriftCompactProtocolWriter(headerMs));
            headerBytes = headerMs.ToArray();
        }

        byte[] encHeader = ph.Type == PageType.DICTIONARY_PAGE
            ? encrForThisColumn.EncryptDictionaryPageHeader(headerBytes, _rowGroupOrdinal, _columnOrdinal)
            : encrForThisColumn.EncryptDataPageHeader(headerBytes, _rowGroupOrdinal, _columnOrdinal, _pageOrdinal);

        await _stream.WriteAsync(encHeader, 0, encHeader.Length, cancellationToken);
        await _stream.WriteAsync(encBody, 0, encBody.Length, cancellationToken);

        cs.CompressedSize += encHeader.Length + encBody.Length;
        cs.UncompressedSize += headerBytes.Length + ph.UncompressedPageSize;
        return new PageWriteMetrics {
            Offset = pageOffset,
            TotalSize = encHeader.Length + encBody.Length
        };
    }

    private async Task<ColumnMetrics> WriteColumnAsync(
        ColumnChunk chunk, DataColumn column,
        SchemaElement tse,
        Encryption.EncryptionBase? encrForThisColumn,
        CancellationToken cancellationToken = default) {

        column.Field.EnsureAttachedToSchema(nameof(column));

        var r = new ColumnMetrics();

        using var pc = new PackedColumn(column);
        pc.Pack(_options.UseDictionaryEncoding, _options.DictionaryEncodingThreshold);
        PopulateColumnStatistics(pc, column);

        // Bloom filter setup
        BloomCollector? bloom = null;
        if(_options.BloomFilterOptionsByColumn.TryGetValue(column.Field.Name, out ParquetOptions.BloomFilterOptions? bloomOptions)) {
            if(bloomOptions != null && bloomOptions.EnableBloomFilters) {
                BloomSizing.BloomPlan plan = BloomSizing.Plan(
                    estimatedDistinctValues: column.Statistics?.DistinctCount ?? column.NumValues,
                    targetFpp: bloomOptions.BloomFilterFpp,
                    bitsPerValueOverride: bloomOptions.BloomFilterBitsPerValueOverride);
                bloom = new BloomCollector(plan.Blocks);
            }
        }

        // dictionary page
        if(pc.HasDictionary) {
            chunk.MetaData!.DictionaryPageOffset = _stream.Position;
            PageHeader ph = _footer.CreateDictionaryPage(pc.Dictionary!.Length, out _);
            r.Pages.Add(ph);
            using MemoryStream ms = _rmsMgr.GetStream();
            ParquetPlainEncoder.Encode(pc.Dictionary, 0, pc.Dictionary.Length,
                   tse,
                   ms, column.Statistics);

            await CompressAndWriteAsync(ph, ms, r, encrForThisColumn, cancellationToken);

            // Feed dictionary values into bloom
            if(bloom != null) {
                BloomAddValues(bloom, pc.Dictionary, 0, pc.Dictionary.Length, _schemaElement);
            }
        }

        Array data = pc.GetPlainData(out _, out int definedValueCount);
        Array definedData = column.DefinedData;
        if(bloom != null && !pc.HasDictionary) {
            BloomAddValues(bloom, data, 0, definedValueCount, _schemaElement);
        }

        int[]? dictionaryIndexes = pc.HasDictionary
            ? pc.GetDictionaryIndexes(out _)
            : null;

        bool wroteDataPageOffset = false;
        foreach(PageSlice slice in BuildPageSlices(column, pc)) {
            using MemoryStream ms = _rmsMgr.GetStream();
            if(!wroteDataPageOffset) {
                chunk.MetaData!.DataPageOffset = _stream.Position;
                wroteDataPageOffset = true;
            }

            bool deltaEncode = !pc.HasDictionary &&
                column.IsDeltaEncodable &&
                _options.UseDeltaBinaryPackedEncoding &&
                DeltaBinaryPackedEncoder.CanEncode(data, slice.DefinedValueOffset, slice.DefinedValueCount);

            PageHeader ph = _footer.CreateDataPage(slice.ValueCount, pc.HasDictionary, deltaEncode, out DataPageHeader dph);
            r.Pages.Add(ph);

            if(pc.HasRepetitionLevels) {
                WriteLevels(ms, pc.RepetitionLevels!, slice.ValueOffset, slice.ValueCount, column.Field.MaxRepetitionLevel);
            }
            if(pc.HasDefinitionLevels) {
                WriteLevels(ms, pc.DefinitionLevels!, slice.ValueOffset, slice.ValueCount, column.Field.MaxDefinitionLevel);
            }

            var pageStats = new DataColumnStatistics {
                NullCount = slice.ValueCount - slice.DefinedValueCount
            };

            if(pc.HasDictionary) {
                int bitWidth = pc.Dictionary!.Length.GetBitWidth();
                ms.WriteByte((byte)bitWidth);   // bit width is stored as 1 byte before encoded data
                RleBitpackedHybridEncoder.Encode(ms, dictionaryIndexes!.AsSpan(slice.DefinedValueOffset, slice.DefinedValueCount), bitWidth);
                TryFillStats(definedData, slice.DefinedValueOffset, slice.DefinedValueCount, pageStats);
            } else if(deltaEncode) {
                DeltaBinaryPackedEncoder.Encode(data, slice.DefinedValueOffset, slice.DefinedValueCount, ms, pageStats);
            } else {
                ParquetPlainEncoder.Encode(data, slice.DefinedValueOffset, slice.DefinedValueCount, tse, ms, pageStats);
            }

            dph.Statistics = pageStats.ToThriftStatistics(tse);
            PageWriteMetrics pageMetrics = await CompressAndWriteAsync(ph, ms, r, encrForThisColumn, cancellationToken);
            r.DataPages.Add(new PageIndexEntry {
                Location = new PageLocation {
                    Offset = pageMetrics.Offset,
                    CompressedPageSize = pageMetrics.TotalSize,
                    FirstRowIndex = slice.FirstRowIndex
                },
                Statistics = dph.Statistics,
                ValueCount = slice.ValueCount
            });
            _pageOrdinal++;
        }

        RegisterPageIndexes(chunk, r, encrForThisColumn);

        // Write bloom filter after all pages
        if(bloom != null && chunk?.MetaData != null) {
            BloomFilterIO.WriteToStream(
                _stream,
                bloom.Filter,
                chunk.MetaData,
                s => new Meta.Proto.ThriftCompactProtocolWriter(s),
                encrForThisColumn,
                _rowGroupOrdinal,
                _columnOrdinal);
        }

        return r;
    }

    private IReadOnlyList<PageSlice> BuildPageSlices(DataColumn column, PackedColumn packedColumn) {
        int pageRowCountLimit = _options.DataPageRowCountLimit;
        if(pageRowCountLimit <= 0)
            throw new ArgumentOutOfRangeException(nameof(_options.DataPageRowCountLimit), pageRowCountLimit,
                "DataPageRowCountLimit must be greater than zero.");

        int totalValueCount = column.NumValues;
        int[]? definitionLevels = packedColumn.DefinitionLevels;
        int[] definedValueOffsets = BuildDefinedValueOffsets(definitionLevels, column.Field.MaxDefinitionLevel, totalValueCount);
        var slices = new List<PageSlice>();

        void AddSlice(int valueOffset, int valueCount, long firstRowIndex, int rowCount) {
            int definedValueOffset = definedValueOffsets[valueOffset];
            int definedValueCount = definedValueOffsets[valueOffset + valueCount] - definedValueOffset;
            slices.Add(new PageSlice {
                ValueOffset = valueOffset,
                ValueCount = valueCount,
                DefinedValueOffset = definedValueOffset,
                DefinedValueCount = definedValueCount,
                FirstRowIndex = firstRowIndex
            });
        }

        if(totalValueCount == 0) {
            AddSlice(0, 0, 0, 0);
            return slices;
        }

        if(!packedColumn.HasRepetitionLevels) {
            long firstRowIndex = 0;
            for(int valueOffset = 0; valueOffset < totalValueCount;) {
                int rowCount = Math.Min(pageRowCountLimit, totalValueCount - valueOffset);
                AddSlice(valueOffset, rowCount, firstRowIndex, rowCount);
                valueOffset += rowCount;
                firstRowIndex += rowCount;
            }
            return slices;
        }

        int[] repetitionLevels = packedColumn.RepetitionLevels!;
        int pageStart = 0;
        long currentFirstRowIndex = 0;

        while(pageStart < totalValueCount) {
            int pageEnd = pageStart;
            int rowCount = 0;

            while(pageEnd < totalValueCount) {
                if(repetitionLevels[pageEnd] == 0) {
                    if(rowCount == pageRowCountLimit && pageEnd > pageStart)
                        break;
                    rowCount++;
                }
                pageEnd++;
            }

            AddSlice(pageStart, pageEnd - pageStart, currentFirstRowIndex, rowCount);
            pageStart = pageEnd;
            currentFirstRowIndex += rowCount;
        }

        return slices;
    }

    private static int[] BuildDefinedValueOffsets(int[]? definitionLevels, int maxDefinitionLevel, int totalValueCount) {
        var offsets = new int[totalValueCount + 1];
        if(definitionLevels == null) {
            for(int i = 0; i <= totalValueCount; i++) {
                offsets[i] = i;
            }
            return offsets;
        }

        for(int i = 0; i < totalValueCount; i++) {
            offsets[i + 1] = offsets[i] + (definitionLevels[i] == maxDefinitionLevel ? 1 : 0);
        }

        return offsets;
    }

    private static void PopulateColumnStatistics(PackedColumn packedColumn, DataColumn column) {
        Array statsSource = packedColumn.HasDictionary
            ? packedColumn.Dictionary!
            : packedColumn.GetPlainData(out _, out _);

        int statsCount = packedColumn.HasDictionary
            ? packedColumn.Dictionary!.Length
            : column.DefinedData.Length;

        column.Statistics.MinValue = null;
        column.Statistics.MaxValue = null;
        TryFillStats(statsSource, 0, statsCount, column.Statistics);
    }

    private static void TryFillStats(Array data, int offset, int count, DataColumnStatistics stats) {
        if(count <= 0)
            return;
        if(offset < 0 || offset > data.Length || count > data.Length - offset) {
            throw new ArgumentOutOfRangeException(nameof(count),
                $"Invalid stats slice: offset={offset}, count={count}, length={data.Length}, type={data.GetType().FullName}");
        }

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
        }
    }

    private void RegisterPageIndexes(
        ColumnChunk chunk,
        ColumnMetrics metrics,
        Encryption.EncryptionBase? encrForThisColumn) {
        if(metrics.DataPages.Count == 0)
            return;

        var offsetIndex = new OffsetIndex {
            PageLocations = metrics.DataPages.Select(dp => dp.Location).ToList()
        };

        if(metrics.DataPages.Any(dp => dp.UnencodedByteArrayDataBytes.HasValue)) {
            offsetIndex.UnencodedByteArrayDataBytes = metrics.DataPages
                .Select(dp => dp.UnencodedByteArrayDataBytes ?? 0L)
                .ToList();
        }

        ColumnIndex? columnIndex = TryBuildColumnIndex(metrics);
        _footer.RegisterPageIndex(
            chunk,
            offsetIndex,
            columnIndex,
            encrForThisColumn,
            _rowGroupOrdinal,
            _columnOrdinal);
    }

    private static ColumnIndex? TryBuildColumnIndex(ColumnMetrics metrics) {
        if(metrics.DataPages.Count == 0)
            return null;

        var nullPages = new List<bool>(metrics.DataPages.Count);
        var minValues = new List<byte[]>(metrics.DataPages.Count);
        var maxValues = new List<byte[]>(metrics.DataPages.Count);
        var nullCounts = new List<long>(metrics.DataPages.Count);

        foreach(PageIndexEntry page in metrics.DataPages) {
            Statistics? stats = page.Statistics;
            if(stats == null)
                return null;

            bool isNullPage = stats.NullCount.HasValue
                && stats.NullCount.Value == page.ValueCount
                && stats.MinValue == null
                && stats.MaxValue == null;

            if(!isNullPage && (stats.MinValue == null || stats.MaxValue == null))
                return null;

            nullPages.Add(isNullPage);
            minValues.Add(isNullPage ? Array.Empty<byte>() : stats.MinValue!);
            maxValues.Add(isNullPage ? Array.Empty<byte>() : stats.MaxValue!);
            nullCounts.Add(stats.NullCount ?? 0);
        }

        return new ColumnIndex {
            NullPages = nullPages,
            MinValues = minValues,
            MaxValues = maxValues,
            BoundaryOrder = BoundaryOrder.UNORDERED,
            NullCounts = nullCounts
        };
    }

    private static int GetSerializedPageHeaderSize(PageHeader ph) {
        using var ms = new MemoryStream();
        ph.Write(new Meta.Proto.ThriftCompactProtocolWriter(ms));
        return checked((int)ms.Length);
    }

    private static void WriteLevels(Stream s, int[] levels, int offset, int count, int maxValue) {
        int bitWidth = maxValue.GetBitWidth();
        RleBitpackedHybridEncoder.EncodeWithLength(s, bitWidth, levels.AsSpan(offset, count));
    }

    private static void BloomAddValues(BloomCollector bloom, Array values, int offset, int count, SchemaElement tse) {
        switch(tse.Type!.Value) {
            case Meta.Type.BOOLEAN: {
                    if(values is bool[] a)
                        for(int i = 0; i < count; i++)
                            bloom.AddBoolean(a[offset + i]);
                    break;
                }
            case Meta.Type.INT32: {
                    if(values is int[] a)
                        for(int i = 0; i < count; i++)
                            bloom.AddInt32(a[offset + i]);
                    else if(values is uint[] au)
                        for(int i = 0; i < count; i++)
                            bloom.AddInt32(unchecked((int)au[offset + i]));
                    break;
                }
            case Meta.Type.INT64: {
                    if(values is long[] a)
                        for(int i = 0; i < count; i++)
                            bloom.AddInt64(a[offset + i]);
                    else if(values is ulong[] au)
                        for(int i = 0; i < count; i++)
                            bloom.AddInt64(unchecked((long)au[offset + i]));
                    break;
                }
            case Meta.Type.INT96: {
                    if(values is DateTime[] a)
                        for(int i = 0; i < count; i++)
                            bloom.AddInt96(a[offset + i]);
                    break;
                }
            case Meta.Type.FLOAT: {
                    if(values is float[] a)
                        for(int i = 0; i < count; i++)
                            bloom.AddFloat(a[offset + i]);
                    break;
                }
            case Meta.Type.DOUBLE: {
                    if(values is double[] a)
                        for(int i = 0; i < count; i++)
                            bloom.AddDouble(a[offset + i]);
                    break;
                }
            case Meta.Type.BYTE_ARRAY: {
                    if(values is string[] sa) {
                        for(int i = 0; i < count; i++)
                            bloom.AddString(sa[offset + i]);
                    } else if(values is byte[][] ba) {
                        for(int i = 0; i < count; i++)
                            bloom.AddByteArray(ba[offset + i]);
                    } else if(values is Array any && any.Length > 0 && any.GetValue(0) is byte[]) {
                        for(int i = 0; i < count; i++)
                            bloom.AddByteArray((byte[])any.GetValue(offset + i)!);
                    }
                    break;
                }
            case Meta.Type.FIXED_LEN_BYTE_ARRAY: {
                    if(values is byte[][] ba) {
                        for(int i = 0; i < count; i++)
                            bloom.AddFixed(ba[offset + i]);
                    } else if(values is Array any && any.Length > 0 && any.GetValue(0) is byte[]) {
                        for(int i = 0; i < count; i++)
                            bloom.AddFixed((byte[])any.GetValue(offset + i)!);
                    }
                    break;
                }
            default:
                break;
        }
    }
}
