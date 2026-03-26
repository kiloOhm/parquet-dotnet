using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IO;
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
                // PF + no footer key → plaintext column (do NOT advertise crypto metadata)
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
        }

        return chunk;
    }

    class ColumnMetrics {
        public int CompressedSize;
        public int UncompressedSize;
        public readonly List<PageHeader> Pages = new();

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

    private async Task CompressAndWriteAsync(
        PageHeader ph, MemoryStream uncompressedData,
        ColumnMetrics cs,
        Encryption.EncryptionBase? encrForThisColumn,
        CancellationToken cancellationToken) {

        int uncompressedLength = (int)uncompressedData.Length;
        using IMemoryOwner<byte> pageData = await Compressor.Instance.CompressAsync(
            _compressionMethod, _compressionLevel, uncompressedData);
        int compressedLength = pageData.Memory.Length;

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
            return;
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
        }

        // data page
        using(MemoryStream ms = _rmsMgr.GetStream()) {
            chunk.MetaData!.DataPageOffset = _stream.Position;
            Array data = pc.GetPlainData(out int offset, out int count);
            bool deltaEncode = column.IsDeltaEncodable && _options.UseDeltaBinaryPackedEncoding && DeltaBinaryPackedEncoder.CanEncode(data, offset, count);

            // data page Num_values also does include NULLs
            PageHeader ph = _footer.CreateDataPage(column.NumValues, pc.HasDictionary, deltaEncode, out DataPageHeader dph);
            r.Pages.Add(ph);
            if(pc.HasRepetitionLevels) {
                WriteLevels(ms, pc.RepetitionLevels!, pc.RepetitionLevels!.Length, column.Field.MaxRepetitionLevel);
            }
            if(pc.HasDefinitionLevels) {
                WriteLevels(ms, pc.DefinitionLevels!, column.DefinitionLevels!.Length, column.Field.MaxDefinitionLevel);
            }

            if(pc.HasDictionary) {
                // dictionary indexes are always encoded with RLE
                int[] indexes = pc.GetDictionaryIndexes(out int indexesLength)!;
                int bitWidth = pc.Dictionary!.Length.GetBitWidth();
                ms.WriteByte((byte)bitWidth);   // bit width is stored as 1 byte before encoded data
                RleBitpackedHybridEncoder.Encode(ms, indexes.AsSpan(0, indexesLength), bitWidth);
            } else {
                if(deltaEncode) {
                    DeltaBinaryPackedEncoder.Encode(data, offset, count, ms, column.Statistics);
                } else {
                    ParquetPlainEncoder.Encode(data, offset, count, tse, ms, pc.HasDictionary ? null : column.Statistics);
                }
            }

            dph.Statistics = column.Statistics.ToThriftStatistics(tse);
            await CompressAndWriteAsync(ph, ms, r, encrForThisColumn, cancellationToken);
            _pageOrdinal++;
        }

        return r;
    }

    private static void WriteLevels(Stream s, Span<int> levels, int count, int maxValue) {
        int bitWidth = maxValue.GetBitWidth();
        RleBitpackedHybridEncoder.EncodeWithLength(s, bitWidth, levels.Slice(0, count));
    }
}
