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
    private readonly Dictionary<string, string>? _keyValueMetadata;
    private readonly ParquetOptions _options;
    private static readonly RecyclableMemoryStreamManager _rmsMgr = new RecyclableMemoryStreamManager();
    private readonly ColumnEncryptionContext? _columnEncryption;
    private readonly bool _encryptedFooter;
    private readonly short _rowGroupOrdinal;
    private readonly short _columnOrdinal;
    private short _pageOrdinal;

    public DataColumnWriter(
       Stream stream,
       ThriftFooter footer,
       SchemaElement schemaElement,
       ParquetOptions options,
       Dictionary<string, string>? keyValueMetadata,
       ColumnEncryptionContext? columnEncryption,
       bool encryptedFooter,
       short rowGroupOrdinal,
       short columnOrdinal) {
        _stream = stream;
        _footer = footer;
        _schemaElement = schemaElement;
        _keyValueMetadata = keyValueMetadata;
        _options = options;
        _columnEncryption = columnEncryption;
        _encryptedFooter = encryptedFooter;
        _rowGroupOrdinal = rowGroupOrdinal;
        _columnOrdinal = columnOrdinal;
        _rmsMgr.Settings.MaximumSmallPoolFreeBytes = options.MaximumSmallPoolFreeBytes;
        _rmsMgr.Settings.MaximumLargePoolFreeBytes = options.MaximumLargePoolFreeBytes;
    }

    public async Task<ColumnChunk> WriteAsync<T>(
        FieldPath fullPath,
        WritingColumn<T> wc,
        CancellationToken cancellationToken) where T : struct {
        // Num_values in the chunk does include null values - I have validated this by dumping spark-generated file.
        ColumnChunk chunk = _footer.CreateColumnChunk(
            _options.CompressionMethod, _stream, _schemaElement.Type!.Value, fullPath, wc.NumValues,
            _keyValueMetadata);
        if(chunk.MetaData == null)
            throw new InvalidDataException($"{nameof(chunk.MetaData)} can not be null");

        if(_columnEncryption != null) {
            chunk.CryptoMetadata = _columnEncryption.UsesColumnKey
                ? new ColumnCryptoMetaData {
                    ENCRYPTIONWITHCOLUMNKEY = new EncryptionWithColumnKey {
                        PathInSchema = fullPath.ToList(),
                        KeyMetadata = _columnEncryption.KeyMetadata
                    }
                }
                : new ColumnCryptoMetaData {
                    ENCRYPTIONWITHFOOTERKEY = new EncryptionWithFooterKey()
                };
        }

        ColumnMetrics metrics = await WriteAsync(
            chunk, wc, _schemaElement,
            cancellationToken);
        chunk.MetaData.Encodings = metrics.GetUsedEncodings();

        //generate stats for column chunk
        chunk.MetaData.Statistics = wc.Statistics.ToThriftStatistics(_schemaElement);

        //the following counters must include both data size and header size
        chunk.MetaData.TotalCompressedSize = metrics.CompressedSize;
        chunk.MetaData.TotalUncompressedSize = metrics.UncompressedSize;

        ProtectColumnMetadata(chunk);

        return chunk;
    }

    private void ProtectColumnMetadata(ColumnChunk chunk) {
        if(_columnEncryption == null || chunk.MetaData == null)
            return;
        if(!_columnEncryption.UsesColumnKey && _encryptedFooter)
            return;

        ColumnMetaData metadata = chunk.MetaData;
        using var metadataStream = _rmsMgr.GetStream();
        metadata.Write(new Meta.Proto.ThriftCompactProtocolWriter(metadataStream));
        chunk.EncryptedColumnMetadata = _columnEncryption.Crypto.Encrypt(
            metadataStream.GetBuffer().AsSpan(0, checked((int)metadataStream.Length)),
            ParquetModuleType.ColumnMetaData,
            _rowGroupOrdinal,
            _columnOrdinal);

        _footer.SetRuntimeColumnMetaData(chunk, metadata);
        if(_encryptedFooter) {
            chunk.MetaData = null;
        } else {
            metadata.Statistics = null;
        }
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

    private sealed class PageIndexEntry {
        public PageLocation Location { get; set; } = new PageLocation();
        public Statistics? Statistics { get; set; }
        public int ValueCount { get; set; }
    }

    private sealed class PageWriteMetrics {
        public long Offset { get; set; }
        public int TotalSize { get; set; }
    }

    private sealed class PageSlice {
        public int ValueOffset { get; set; }
        public int ValueCount { get; set; }
        public int DefinedValueOffset { get; set; }
        public int DefinedValueCount { get; set; }
        public long FirstRowIndex { get; set; }
    }

    private async Task<PageWriteMetrics> CompressAndWriteAsync(
        PageHeader ph, MemoryStream uncompressedData,
        ColumnMetrics cs,
        CancellationToken cancellationToken) {

        long pageOffset = _stream.Position;
        int uncompressedLength = (int)uncompressedData.Length;
        using IMemoryOwner<byte> pageData = await Compressor.Instance.CompressAsync(
            _options.CompressionMethod, _options.CompressionLevel, uncompressedData);
        int compressedLength = pageData.Memory.Length;

        ph.UncompressedPageSize = uncompressedLength;
        byte[]? encryptedBody = null;
        if(_columnEncryption != null) {
            encryptedBody = ph.Type == PageType.DICTIONARY_PAGE
                ? _columnEncryption.Crypto.Encrypt(
                    pageData.Memory.Span,
                    ParquetModuleType.DictionaryPage,
                    _rowGroupOrdinal,
                    _columnOrdinal)
                : _columnEncryption.Crypto.Encrypt(
                    pageData.Memory.Span,
                    ParquetModuleType.DataPage,
                    _rowGroupOrdinal,
                    _columnOrdinal,
                    _pageOrdinal);
        }
        ph.CompressedPageSize = encryptedBody?.Length ?? compressedLength;

        int writtenHeaderSize;

        //write the header in
        using(MemoryStream headerMs = _rmsMgr.GetStream()) {
            ph.Write(new Meta.Proto.ThriftCompactProtocolWriter(headerMs));
            int headerSize = (int)headerMs.Length;
            headerMs.Position = 0;
            _stream.Flush();

            if(_columnEncryption == null) {
                await headerMs.CopyToAsync(_stream);
            } else {
                byte[] encryptedHeader = ph.Type == PageType.DICTIONARY_PAGE
                    ? _columnEncryption.Crypto.Encrypt(
                        headerMs.GetBuffer().AsSpan(0, headerSize),
                        ParquetModuleType.DictionaryPageHeader,
                        _rowGroupOrdinal,
                        _columnOrdinal)
                    : _columnEncryption.Crypto.Encrypt(
                        headerMs.GetBuffer().AsSpan(0, headerSize),
                        ParquetModuleType.DataPageHeader,
                        _rowGroupOrdinal,
                        _columnOrdinal,
                        _pageOrdinal);
                await _stream.WriteAsync(encryptedHeader, 0, encryptedHeader.Length, cancellationToken);
                headerSize = encryptedHeader.Length;
            }

            writtenHeaderSize = headerSize;
            cs.CompressedSize += headerSize;
            cs.UncompressedSize += _columnEncryption == null ? headerSize : checked((int)headerMs.Length);
        }

        // write data
        if(encryptedBody == null)
            await pageData.Memory.CopyToAsync(_stream, cancellationToken);
        else
            await _stream.WriteAsync(encryptedBody, 0, encryptedBody.Length, cancellationToken);

        cs.CompressedSize += ph.CompressedPageSize;
        cs.UncompressedSize += ph.UncompressedPageSize;
        if(ph.Type != PageType.DICTIONARY_PAGE)
            _pageOrdinal = checked((short)(_pageOrdinal + 1));

        return new PageWriteMetrics {
            Offset = pageOffset,
            TotalSize = checked(writtenHeaderSize + ph.CompressedPageSize)
        };
    }

    private async Task<ColumnMetrics> WriteAsync<T>(ColumnChunk chunk,
        WritingColumn<T> wc,
        SchemaElement tse,
        CancellationToken cancellationToken) where T : struct {

        wc.Field.EnsureAttachedToSchema(nameof(wc.Field));
        wc.Pack(_options);
        if(!wc.HasDictionary)
            ParquetPlainEncoder.Encode(wc.Values, Stream.Null, tse, wc.Statistics);

        var r = new ColumnMetrics();
        BloomCollector? bloom = null;
        if(_options.BloomFilterOptionsByColumn.TryGetValue(wc.Field.Name, out ParquetOptions.BloomFilterOptions? bloomOptions)
            && bloomOptions.EnableBloomFilters) {
            BloomSizing.BloomPlan plan = BloomSizing.Plan(
                wc.Statistics.DistinctCount ?? wc.Values.Length,
                bloomOptions.BloomFilterFpp,
                bloomOptions.BloomFilterBitsPerValueOverride);
            bloom = new BloomCollector(plan.Blocks);
            BloomAddValues(bloom, wc.Values, tse);
        }

        /*
         * Page header must preceeed actual data (compressed or not) however it contains both
         * the uncompressed and compressed data size which we don't know! This somehow limits
         * the write efficiency.
         */

        // dictionary page
        if(wc.HasDictionary) {
            chunk.MetaData!.DictionaryPageOffset = _stream.Position;
            PageHeader ph = _footer.CreateDictionaryPage(wc.Dictionary.Length, out _);
            r.Pages.Add(ph);
            using MemoryStream ms = _rmsMgr.GetStream();
            ParquetPlainEncoder.Encode(wc.Dictionary, ms, tse, wc.Statistics);
            await CompressAndWriteAsync(ph, ms, r, cancellationToken);
        }

        bool wroteDataPageOffset = false;
        foreach(PageSlice slice in BuildPageSlices(wc)) {
            using MemoryStream ms = _rmsMgr.GetStream();
            if(!wroteDataPageOffset) {
                chunk.MetaData!.DataPageOffset = _stream.Position;
                wroteDataPageOffset = true;
            }

            ReadOnlySpan<T> pageValues = wc.Values.Slice(slice.DefinedValueOffset, slice.DefinedValueCount);
            bool deltaEncode = !wc.HasDictionary &&
                _options.GetEncodingHint(wc.Field) == EncodingHint.DeltaBinaryPacked &&
                DeltaBinaryPackedEncoder.CanEncode(pageValues);
            bool byteSplitStreamEncode = !wc.HasDictionary &&
                _options.GetEncodingHint(wc.Field) == EncodingHint.ByteSplitStream &&
                ByteStreamSplitEncoder.IsSupported(typeof(T));

            PageHeader ph = _footer.CreateDataPage(
                slice.ValueCount,
                wc.HasDictionary,
                deltaEncode,
                byteSplitStreamEncode,
                out DataPageHeader dph);
            r.Pages.Add(ph);

            if(wc.HasRepetitionLevels)
                WriteLevels(ms, wc.RepetitionLevels.Slice(slice.ValueOffset, slice.ValueCount), wc.Field.MaxRepetitionLevel);
            if(wc.HasDefinitionLevels)
                WriteLevels(ms, wc.DefinitionLevels.Slice(slice.ValueOffset, slice.ValueCount), wc.Field.MaxDefinitionLevel);

            var pageStats = new DataColumnStatistics {
                NullCount = slice.ValueCount - slice.DefinedValueCount
            };

            if(wc.HasDictionary) {
                int bitWidth = wc.Dictionary.Length.GetBitWidth();
                ms.WriteByte((byte)bitWidth);
                RleBitpackedHybridEncoder.Encode(
                    ms,
                    wc.DictionaryIndexes.Slice(slice.DefinedValueOffset, slice.DefinedValueCount),
                    bitWidth);
                ParquetPlainEncoder.Encode(pageValues, Stream.Null, tse, pageStats);
            } else if(deltaEncode) {
                DeltaBinaryPackedEncoder.Encode(pageValues, ms, pageStats);
            } else if(byteSplitStreamEncode) {
                ByteStreamSplitEncoder.Encode(pageValues, ms);
                ParquetPlainEncoder.Encode(pageValues, Stream.Null, tse, pageStats);
            } else {
                ParquetPlainEncoder.Encode(pageValues, ms, tse, pageStats);
            }

            dph.Statistics = pageStats.ToThriftStatistics(tse);
            PageWriteMetrics pageMetrics = await CompressAndWriteAsync(ph, ms, r, cancellationToken);
            r.DataPages.Add(new PageIndexEntry {
                Location = new PageLocation {
                    Offset = pageMetrics.Offset,
                    CompressedPageSize = pageMetrics.TotalSize,
                    FirstRowIndex = slice.FirstRowIndex
                },
                Statistics = dph.Statistics,
                ValueCount = slice.ValueCount
            });
        }

        RegisterPageIndexes(chunk, r);

        if(bloom != null && chunk.MetaData != null) {
            BloomFilterIO.WriteToStream(
                _stream,
                bloom.Filter,
                chunk.MetaData,
                stream => new Meta.Proto.ThriftCompactProtocolWriter(stream));
        }

        return r;
    }

    private IReadOnlyList<PageSlice> BuildPageSlices<T>(WritingColumn<T> column) where T : struct {
        int pageRowCountLimit = _options.DataPageRowCountLimit;
        if(pageRowCountLimit <= 0) {
            throw new ArgumentOutOfRangeException(
                nameof(_options.DataPageRowCountLimit),
                pageRowCountLimit,
                "DataPageRowCountLimit must be greater than zero.");
        }

        int totalValueCount = column.NumValues;
        int[] definedValueOffsets = new int[totalValueCount + 1];
        if(column.HasDefinitionLevels) {
            ReadOnlySpan<int> definitionLevels = column.DefinitionLevels;
            for(int i = 0; i < totalValueCount; i++) {
                definedValueOffsets[i + 1] = definedValueOffsets[i] +
                    (definitionLevels[i] == column.Field.MaxDefinitionLevel ? 1 : 0);
            }
        } else {
            for(int i = 0; i <= totalValueCount; i++)
                definedValueOffsets[i] = i;
        }

        var slices = new List<PageSlice>();
        void AddSlice(int valueOffset, int valueCount, long firstRowIndex) {
            int definedValueOffset = definedValueOffsets[valueOffset];
            slices.Add(new PageSlice {
                ValueOffset = valueOffset,
                ValueCount = valueCount,
                DefinedValueOffset = definedValueOffset,
                DefinedValueCount = definedValueOffsets[valueOffset + valueCount] - definedValueOffset,
                FirstRowIndex = firstRowIndex
            });
        }

        if(totalValueCount == 0) {
            AddSlice(0, 0, 0);
            return slices;
        }

        if(!column.HasRepetitionLevels) {
            for(int valueOffset = 0; valueOffset < totalValueCount; valueOffset += pageRowCountLimit) {
                int valueCount = Math.Min(pageRowCountLimit, totalValueCount - valueOffset);
                AddSlice(valueOffset, valueCount, valueOffset);
            }
            return slices;
        }

        ReadOnlySpan<int> repetitionLevels = column.RepetitionLevels;
        int pageStart = 0;
        long firstRowIndex = 0;
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

            AddSlice(pageStart, pageEnd - pageStart, firstRowIndex);
            pageStart = pageEnd;
            firstRowIndex += rowCount;
        }

        return slices;
    }

    private void RegisterPageIndexes(ColumnChunk chunk, ColumnMetrics metrics) {
        if(metrics.DataPages.Count == 0)
            return;

        var offsetIndex = new OffsetIndex {
            PageLocations = metrics.DataPages.Select(page => page.Location).ToList()
        };
        _footer.RegisterPageIndex(
            chunk,
            offsetIndex,
            TryBuildColumnIndex(metrics),
            _columnEncryption?.Crypto,
            _rowGroupOrdinal,
            _columnOrdinal);
    }

    private static ColumnIndex? TryBuildColumnIndex(ColumnMetrics metrics) {
        var nullPages = new List<bool>(metrics.DataPages.Count);
        var minValues = new List<byte[]>(metrics.DataPages.Count);
        var maxValues = new List<byte[]>(metrics.DataPages.Count);
        var nullCounts = new List<long>(metrics.DataPages.Count);

        foreach(PageIndexEntry page in metrics.DataPages) {
            Statistics? stats = page.Statistics;
            if(stats == null)
                return null;

            bool isNullPage = stats.NullCount == page.ValueCount &&
                stats.MinValue == null &&
                stats.MaxValue == null;
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

    private static void BloomAddValues<T>(BloomCollector bloom, ReadOnlySpan<T> values, SchemaElement schemaElement)
        where T : struct {
        foreach(T value in values) {
            object boxed = value;
            switch(schemaElement.Type!.Value) {
                case Meta.Type.BOOLEAN:
                    bloom.AddBoolean(Convert.ToBoolean(boxed));
                    break;
                case Meta.Type.INT32:
                    bloom.AddInt32(boxed is uint uintValue ? unchecked((int)uintValue) : Convert.ToInt32(boxed));
                    break;
                case Meta.Type.INT64:
                    bloom.AddInt64(boxed is ulong ulongValue ? unchecked((long)ulongValue) : Convert.ToInt64(boxed));
                    break;
                case Meta.Type.INT96:
                    if(boxed is DateTime dateTime)
                        bloom.AddInt96(dateTime);
                    break;
                case Meta.Type.FLOAT:
                    bloom.AddFloat(Convert.ToSingle(boxed));
                    break;
                case Meta.Type.DOUBLE:
                    bloom.AddDouble(Convert.ToDouble(boxed));
                    break;
                case Meta.Type.BYTE_ARRAY:
                    if(boxed is ReadOnlyMemory<char> chars)
                        bloom.AddString(chars.ToString());
                    else if(boxed is ReadOnlyMemory<byte> bytes)
                        bloom.AddByteArray(bytes.ToArray());
                    break;
                case Meta.Type.FIXED_LEN_BYTE_ARRAY:
                    if(boxed is ReadOnlyMemory<byte> fixedBytes)
                        bloom.AddFixed(fixedBytes.ToArray());
                    else if(boxed is Guid guid)
                        bloom.AddFixed(guid.ToByteArray());
                    break;
            }
        }
    }

    private static void WriteLevels(Stream s, ReadOnlySpan<int> levels, int maxValue) {
        int bitWidth = maxValue.GetBitWidth();
        RleBitpackedHybridEncoder.EncodeWithLength(s, bitWidth, levels);
    }
}
