using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Extensions;
using Parquet.Extensions.Streaming;
using Parquet.File;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet;

/// <summary>
/// Implements Apache Parquet format writer
/// </summary>
public sealed class ParquetWriter : ParquetActor, IAsyncDisposable {
    private ThriftFooter? _footer;
    private readonly ParquetSchema _schema;
    private readonly ParquetOptions _options;
    private bool _dataWritten;
    private readonly List<ParquetRowGroupWriter> _openedWriters = new List<ParquetRowGroupWriter>();
    private EncryptionBase? _encrypter;
    private Meta.FileCryptoMetaData? _cryptoMeta;

    // for plaintext-footer mode
    private Meta.EncryptionAlgorithm? _plaintextAlg;

    // holds AadPrefix/AadFileUnique to build AAD for signing
    private EncryptionBase? _signer;

    private ParquetWriter(ParquetSchema schema, Stream output, ParquetOptions? options = null, bool append = false)
       : base(output.CanSeek == true ? output : new MeteredWriteStream(output)) {
        if(output == null)
            throw new ArgumentNullException(nameof(output));

        if(!output.CanWrite)
            throw new ArgumentException("stream is not writeable", nameof(output));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _options = options ?? new ParquetOptions();
    }

    /// <summary>
    /// Creates an instance of parquet writer on top of a stream.
    /// </summary>
    /// <param name="schema"></param>
    /// <param name="output">Writeable, seekable stream</param>
    /// <param name="options">Additional options</param>
    /// <param name="append"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="ArgumentNullException">Output is null.</exception>
    /// <exception cref="ArgumentException">Output stream is not writeable</exception>
    public static async Task<ParquetWriter> CreateAsync(
        ParquetSchema schema, Stream output, ParquetOptions? options = null, bool append = false,
        CancellationToken cancellationToken = default) {
        var writer = new ParquetWriter(schema, output, options, append);
        await writer.PrepareFileAsync(append, cancellationToken);
        return writer;
    }

    /// <summary>
    /// Creates a new row group and a writer for it.
    /// </summary>
    public ParquetRowGroupWriter CreateRowGroup() {
        _dataWritten = true;

        var writer = new ParquetRowGroupWriter(Stream, _footer!, _options);

        _openedWriters.Add(writer);

        return writer;
    }

    /// <summary>
    /// Gets custom key-value pairs for metadata
    /// </summary>
    public IReadOnlyDictionary<string, string> CustomMetadata {
        get => _footer!.CustomMetadata;
        set => _footer!.CustomMetadata = value.ToDictionary(p => p.Key, p => p.Value);
    }

    private async Task PrepareFileAsync(bool append, CancellationToken cancellationToken) {
        if(append) {
            if(!Stream.CanSeek)
                throw new IOException("destination stream must be seekable for append operations.");

            if(Stream.Length == 0)
                throw new IOException("you can only append to existing streams, but current stream is empty.");

            await ValidateFileAsync();

            FileMetaData fileMeta = await ReadMetadataAsync();
            _footer = new ThriftFooter(fileMeta);

            ValidateSchemasCompatible(_footer, _schema);

            await GoBeforeFooterAsync();
            return;
        }

        // -------- New unified setup for fresh files (non-append) --------

        // Guard AAD prefix option
        if(_formatOptions.SupplyAadPrefix && string.IsNullOrWhiteSpace(_formatOptions.AADPrefix))
            throw new ArgumentException("SupplyAadPrefix=true requires AADPrefix to be set.");

        byte[]? aadPrefixBytes = _formatOptions.AADPrefix is null
            ? null
            : System.Text.Encoding.UTF8.GetBytes(_formatOptions.AADPrefix);

        bool wantsEncryptedFooter =
            !string.IsNullOrWhiteSpace(_formatOptions.FooterEncryptionKey) &&
            !_formatOptions.UsePlaintextFooter;

        bool hasColumnKeys =
            _formatOptions.ColumnKeys is not null && _formatOptions.ColumnKeys.Count > 0;

        // Optional: allow encrypting pages with the footer key even when PF is requested.
        bool wantsFooterKeyPagesInPF =
            _formatOptions.UsePlaintextFooter &&
            !string.IsNullOrWhiteSpace(_formatOptions.FooterEncryptionKey);

        // --- Create encrypter when needed ---
        // EF mode â†’ encrypter with FooterEncryptionKey (used for footer + pages).
        if(wantsEncryptedFooter) {
            (_encrypter, _cryptoMeta) = EncryptionBase.CreateEncryptorForWrite(
                _formatOptions.FooterEncryptionKey!,
                aadPrefixBytes,
                supplyAadPrefix: _formatOptions.SupplyAadPrefix,
                useCtrVariant: _formatOptions.UseCtrVariant
            );
            _encrypter = _encrypter ?? throw new InvalidOperationException("encrypter was not created");
            _cryptoMeta.KeyMetadata = _formatOptions.FooterEncryptionKeyMetadata;
        }
        // PF mode â†’ still create encrypter if any encryption is desired (column keys and/or footer-key pages).
        else if(_formatOptions.UsePlaintextFooter && (hasColumnKeys || wantsFooterKeyPagesInPF)) {
            // IMPORTANT: use FooterEncryptionKey for page encryption when present.
            string seedKey =
                !string.IsNullOrWhiteSpace(_formatOptions.FooterEncryptionKey)
                    ? _formatOptions.FooterEncryptionKey!
                    // no footer key â†’ only column-key encryption; seed with random (column writers swap keys per column)
                    : BitConverter.ToString(CryptoHelpers.GetRandomBytes(32)).Replace("-", ""); // 256-bit random hex

            (_encrypter, _cryptoMeta) = EncryptionBase.CreateEncryptorForWrite(
                seedKey,
                aadPrefixBytes,
                supplyAadPrefix: _formatOptions.SupplyAadPrefix,
                useCtrVariant: _formatOptions.UseCtrVariant
            );
            _encrypter = _encrypter ?? throw new InvalidOperationException("encrypter was not created");
            if(wantsFooterKeyPagesInPF) {
                _cryptoMeta.KeyMetadata = _formatOptions.FooterEncryptionKeyMetadata;
            }
        }

        // --- Build footer and write head magic ---
        if(_footer == null) {
            _footer = new ThriftFooter(_schema, 0, _formatOptions);
            _footer.Encrypter = _encrypter;

            bool encryptedFooterMode = _encrypter != null && !_formatOptions.UsePlaintextFooter;
            await WriteMagicAsync(encrypted: encryptedFooterMode);
        } else {
            if(_footer == null) {
                // totalRowCount is set to 0 with expectation that it will be updated at the end of writing (see DisposeCore)
                _footer = new ThriftFooter(_schema, 0, _options);

        // --- Plaintext footer (PF) signing setup (PAR1 tail, optional signature trailer) ---
        if(_formatOptions.UsePlaintextFooter && !string.IsNullOrWhiteSpace(_formatOptions.FooterSigningKey)) {
            (EncryptionBase? encTmp, FileCryptoMetaData? signMeta) = EncryptionBase.CreateEncryptorForWrite(
                _formatOptions.FooterSigningKey!,
                aadPrefixBytes,
                supplyAadPrefix: _formatOptions.SupplyAadPrefix,
                useCtrVariant: _formatOptions.UseCtrVariant
            );

            // IMPORTANT: Advertise algorithm using the SAME aad_file_unique as pages.
            if(_cryptoMeta is not null) {
                _plaintextAlg = _cryptoMeta.EncryptionAlgorithm;     // from PAGE ENCRYPTER
            } else {
                _plaintextAlg = signMeta.EncryptionAlgorithm;        // no page encrypter (column-keys only)
            }

                // it's set to 0 with expectation that row count will be updated at the end of writing (see DisposeCore)
                _footer.Add(0);
            }
        }
    }

    private void ValidateSchemasCompatible(ThriftFooter footer, ParquetSchema schema) {
        ParquetSchema existingSchema = footer.CreateModelSchema(_options);

        if(!schema.Equals(existingSchema)) {
            string reason = schema.GetNotEqualsMessage(existingSchema, "appending", "existing");
            throw new ParquetException($"passed schema does not match existing file schema, reason: {reason}");
        }
    }

    private Task WriteMagicAsync() => Stream.WriteAsync(MagicBytes, 0, MagicBytes.Length);

    private void DisposeCore() {
        if(_dataWritten) {
            //update row count (on append add row count to existing metadata)
            _footer!.Add(_openedWriters.Sum(w => w.RowCount ?? 0));
        }
    }

    /// <summary>
    /// Dispose the writer asynchronously
    /// </summary>
    public async ValueTask DisposeAsync() {
        DisposeCore();
        if(_footer == null) {
            return;
        }

        await _footer.WritePageIndexesAsync(Stream).ConfigureAwait(false);

        using var ms = new MemoryStream();

        // --- Plaintext footer mode (always ends with PAR1) ---
        if(_formatOptions.UsePlaintextFooter) {
            await _footer.WriteAsync(ms).ConfigureAwait(false);
            byte[] footerBytes = ms.ToArray();

            if(_plaintextAlg is not null) {
                if(_signer is null)
                    throw new InvalidOperationException("Signer missing in plaintext footer mode.");

                byte[] aad = (_encrypter ?? _signer)!.BuildAad(Meta.ParquetModules.Footer);

                byte[] nonce12 = CryptoHelpers.GetRandomBytes(12);

                byte[] tag = new byte[16];
                byte[] tmpCt = new byte[footerBytes.Length];

                CryptoHelpers.GcmEncryptOrThrow(_signer.FooterEncryptionKey!, nonce12, footerBytes, tmpCt, tag, aad);

                await Stream.WriteAsync(footerBytes, 0, footerBytes.Length).ConfigureAwait(false);
                await Stream.WriteAsync(nonce12, 0, nonce12.Length).ConfigureAwait(false);
                await Stream.WriteAsync(tag, 0, tag.Length).ConfigureAwait(false);
                await Stream.WriteInt32Async(footerBytes.Length + 28).ConfigureAwait(false);
                await WriteMagicAsync(false).ConfigureAwait(false);
                await Stream.FlushAsync().ConfigureAwait(false);
                return;
            } else {
                await Stream.WriteAsync(footerBytes, 0, footerBytes.Length).ConfigureAwait(false);
                await Stream.WriteInt32Async(footerBytes.Length).ConfigureAwait(false);
                await WriteMagicAsync(false).ConfigureAwait(false);
                await Stream.FlushAsync().ConfigureAwait(false);
                return;
            }
        }

        // --- Encrypted footer mode (PARE) ---
        if(_encrypter is not null) {
            ms.SetLength(0);
            await _footer.WriteAsync(ms).ConfigureAwait(false);
            byte[] encFooter = _encrypter.EncryptFooter(ms.ToArray());

            using var metaMs = new MemoryStream();
            var metaWriter = new Parquet.Meta.Proto.ThriftCompactProtocolWriter(metaMs);
            _cryptoMeta!.Write(metaWriter);
            byte[] metaBytes = metaMs.ToArray();

            await Stream.WriteAsync(metaBytes, 0, metaBytes.Length).ConfigureAwait(false);
            await Stream.WriteAsync(encFooter, 0, encFooter.Length).ConfigureAwait(false);
            await Stream.WriteInt32Async(metaBytes.Length + encFooter.Length).ConfigureAwait(false);
            await WriteMagicAsync(true).ConfigureAwait(false);
            await Stream.FlushAsync().ConfigureAwait(false);
            return;
        }

        // --- Legacy plaintext footer (no encryption anywhere) ---
        ms.SetLength(0);
        long size = await _footer.WriteAsync(Stream).ConfigureAwait(false);
        await Stream.WriteInt32Async((int)size);
        await WriteMagicAsync(false);
        await Stream.FlushAsync();
    }
}
