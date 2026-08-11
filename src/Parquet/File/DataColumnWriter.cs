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
    private readonly short _rowGroupOrdinal;
    private readonly short _columnOrdinal;
    private short _pageOrdinal; // increments per DATA page only

    public DataColumnWriter(
       Stream stream,
       ThriftFooter footer,
       SchemaElement schemaElement,
       ParquetOptions options,
       Dictionary<string, string>? keyValueMetadata) {
        _stream = stream;
        _footer = footer;
        _schemaElement = schemaElement;
        _keyValueMetadata = keyValueMetadata;
        _options = options;
        _rmsMgr.Settings.MaximumSmallPoolFreeBytes = options.MaximumSmallPoolFreeBytes;
        _rmsMgr.Settings.MaximumLargePoolFreeBytes = options.MaximumLargePoolFreeBytes;
        _rowGroupOrdinal = rowGroupOrdinal;
        _columnOrdinal = columnOrdinal;
        _pageOrdinal = 0;
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

        ColumnMetrics metrics = await WriteAsync(
            chunk, wc, _schemaElement,
            cancellationToken);
        chunk.MetaData.Encodings = metrics.GetUsedEncodings();

        //generate stats for column chunk
        chunk.MetaData.Statistics = wc.Statistics.ToThriftStatistics(_schemaElement);

        //the following counters must include both data size and header size
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
            _options.CompressionMethod, _options.CompressionLevel, uncompressedData);
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

    private async Task<ColumnMetrics> WriteAsync<T>(ColumnChunk chunk,
        WritingColumn<T> wc,
        SchemaElement tse,
        CancellationToken cancellationToken) where T : struct {

        wc.Field.EnsureAttachedToSchema(nameof(wc.Field));
        wc.Pack(_options);

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
            PageHeader ph = _footer.CreateDictionaryPage(wc.Dictionary.Length, out _);
            r.Pages.Add(ph);
            using MemoryStream ms = _rmsMgr.GetStream();
            ParquetPlainEncoder.Encode(wc.Dictionary, ms, tse, wc.Statistics);
            await CompressAndWriteAsync(ph, ms, r, cancellationToken);
        }

        // data page
        using(MemoryStream ms = _rmsMgr.GetStream()) {
            bool deltaEncode = _options.GetEncodingHint(wc.Field) == EncodingHint.DeltaBinaryPacked && DeltaBinaryPackedEncoder.CanEncode(wc.Values);
            bool byteSplitStreamEncode = _options.GetEncodingHint(wc.Field) == EncodingHint.ByteSplitStream && ByteStreamSplitEncoder.IsSupported(typeof(T));

            // data page Num_values also does include NULLs
            PageHeader ph = _footer.CreateDataPage(wc.NumValues, wc.HasDictionary, deltaEncode, byteSplitStreamEncode, out DataPageHeader dph);
            r.Pages.Add(ph);

            if(wc.HasRepetitionLevels) {
                WriteLevels(ms, wc.RepetitionLevels!, wc.Field.MaxRepetitionLevel);
            }
            if(wc.HasDefinitionLevels) {
                WriteLevels(ms, wc.DefinitionLevels!, wc.Field.MaxDefinitionLevel);
            }

            if(wc.HasDictionary) {
                // dictionary indexes are always encoded with RLE
                int bitWidth = wc.Dictionary.Length.GetBitWidth();  // bit width is determined by the dictionary size
                ms.WriteByte((byte)bitWidth);   // bit width is stored as 1 byte before encoded data
                RleBitpackedHybridEncoder.Encode(ms, wc.DictionaryIndexes, bitWidth);
            } else {
                if(deltaEncode) {
                    DeltaBinaryPackedEncoder.Encode(wc.Values, ms, wc.Statistics);
                } else if(byteSplitStreamEncode) {
                    ByteStreamSplitEncoder.Encode(wc.Values, ms);
                } else {
                    ParquetPlainEncoder.Encode(wc.Values,
                        ms,
                        tse,
                        wc.HasDictionary ? null : wc.Statistics);
                }
            }

            dph.Statistics = wc.Statistics.ToThriftStatistics(tse);
            await CompressAndWriteAsync(ph, ms, r, cancellationToken);
        }

        if(bloom != null && chunk.MetaData != null) {
            BloomFilterIO.WriteToStream(
                _stream,
                bloom.Filter,
                chunk.MetaData,
                stream => new Meta.Proto.ThriftCompactProtocolWriter(stream));
        }

        return r;
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
