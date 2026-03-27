using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet {
    /// <summary>
    /// Extension methods for row-group reader functionality that is only available
    /// on the built-in <see cref="ParquetRowGroupReader"/> implementation.
    /// </summary>
    public static class ParquetRowGroupReaderExtensions {
        /// <summary>
        /// Gets the row group's offset index for a data field when present.
        /// </summary>
        /// <param name="rowGroupReader">Row-group reader created by <see cref="ParquetReader"/>.</param>
        /// <param name="field">Field to get the offset index for.</param>
        /// <returns>The offset index, or <see langword="null"/> when the column chunk does not contain one.</returns>
        public static OffsetIndex? GetOffsetIndex(this IParquetRowGroupReader rowGroupReader, DataField field) {
            if(rowGroupReader == null)
                throw new ArgumentNullException(nameof(rowGroupReader));

            if(rowGroupReader is not ParquetRowGroupReader concreteReader) {
                throw new NotSupportedException(
                    $"Page index access is only available on {nameof(ParquetRowGroupReader)} instances created by {nameof(ParquetReader)}.");
            }

            return concreteReader.GetOffsetIndex(field);
        }

        /// <summary>
        /// Gets the row group's column index for a data field when present.
        /// </summary>
        /// <param name="rowGroupReader">Row-group reader created by <see cref="ParquetReader"/>.</param>
        /// <param name="field">Field to get the column index for.</param>
        /// <returns>The column index, or <see langword="null"/> when the column chunk does not contain one.</returns>
        public static ColumnIndex? GetColumnIndex(this IParquetRowGroupReader rowGroupReader, DataField field) {
            if(rowGroupReader == null)
                throw new ArgumentNullException(nameof(rowGroupReader));

            if(rowGroupReader is not ParquetRowGroupReader concreteReader) {
                throw new NotSupportedException(
                    $"Page index access is only available on {nameof(ParquetRowGroupReader)} instances created by {nameof(ParquetReader)}.");
            }

            return concreteReader.GetColumnIndex(field);
        }

        /// <summary>
        /// Gets the row group's column index for a data field when present, or computes one page-by-page when it is absent.
        /// </summary>
        /// <param name="rowGroupReader">Row-group reader created by <see cref="ParquetReader"/>.</param>
        /// <param name="field">Field to get the column index for.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The on-disk column index when present, a computed in-memory column index when possible, or <see langword="null"/> for unsupported types.</returns>
        public static Task<ColumnIndex?> GetOrCreateColumnIndexAsync(
            this IParquetRowGroupReader rowGroupReader,
            DataField field,
            CancellationToken cancellationToken = default) {
            if(rowGroupReader == null)
                throw new ArgumentNullException(nameof(rowGroupReader));

            if(rowGroupReader is not ParquetRowGroupReader concreteReader) {
                throw new NotSupportedException(
                    $"Page index access is only available on {nameof(ParquetRowGroupReader)} instances created by {nameof(ParquetReader)}.");
            }

            return concreteReader.GetOrCreateColumnIndexAsync(field, cancellationToken);
        }

        /// <summary>
        /// Reads only the specified data pages from a column chunk.
        /// </summary>
        /// <param name="rowGroupReader">Row-group reader created by <see cref="ParquetReader"/>.</param>
        /// <param name="field">Field to read.</param>
        /// <param name="pageOrdinals">Zero-based data-page ordinals to include.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A column containing only the selected pages, in page order.</returns>
        public static Task<DataColumn> ReadColumnPagesAsync(
            this IParquetRowGroupReader rowGroupReader,
            DataField field,
            IReadOnlyCollection<int> pageOrdinals,
            CancellationToken cancellationToken = default) {
            if(rowGroupReader == null)
                throw new ArgumentNullException(nameof(rowGroupReader));

            if(rowGroupReader is not ParquetRowGroupReader concreteReader) {
                throw new NotSupportedException(
                    $"Page-selective reads are only available on {nameof(ParquetRowGroupReader)} instances created by {nameof(ParquetReader)}.");
            }

            return concreteReader.ReadColumnPagesAsync(field, pageOrdinals, cancellationToken);
        }

        /// <summary>
        /// Opens a public page reader for the specified field.
        /// </summary>
        public static ParquetColumnPageReader OpenColumnPageReader(
            this IParquetRowGroupReader rowGroupReader,
            DataField field) {
            if(rowGroupReader == null)
                throw new ArgumentNullException(nameof(rowGroupReader));

            if(rowGroupReader is not ParquetRowGroupReader concreteReader) {
                throw new NotSupportedException(
                    $"Page readers are only available on {nameof(ParquetRowGroupReader)} instances created by {nameof(ParquetReader)}.");
            }

            return concreteReader.OpenColumnPageReader(field);
        }

        /// <summary>
        /// Opens a public page reader for the specified field.
        /// </summary>
        public static Task<ParquetColumnPageReader> OpenColumnPageReaderAsync(
            this IParquetRowGroupReader rowGroupReader,
            DataField field,
            CancellationToken cancellationToken = default) {
            if(rowGroupReader == null)
                throw new ArgumentNullException(nameof(rowGroupReader));

            if(rowGroupReader is not ParquetRowGroupReader concreteReader) {
                throw new NotSupportedException(
                    $"Page readers are only available on {nameof(ParquetRowGroupReader)} instances created by {nameof(ParquetReader)}.");
            }

            return concreteReader.OpenColumnPageReaderAsync(field, cancellationToken);
        }
    }
}
