using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet {
    /// <summary>
    /// Represents a single data page read from a column chunk.
    /// </summary>
    public sealed class ParquetDataPage {
        internal ParquetDataPage(
            int ordinal,
            PageLocation location,
            long rowCount,
            DataColumn column) {
            Ordinal = ordinal;
            Location = location ?? throw new ArgumentNullException(nameof(location));
            RowCount = rowCount;
            Column = column ?? throw new ArgumentNullException(nameof(column));
        }

        /// <summary>
        /// Zero-based page ordinal within the column chunk.
        /// </summary>
        public int Ordinal { get; }

        /// <summary>
        /// Offset-index metadata for this page.
        /// </summary>
        public PageLocation Location { get; }

        /// <summary>
        /// Number of logical rows covered by this page.
        /// </summary>
        public long RowCount { get; }

        /// <summary>
        /// Page data decoded as a <see cref="DataColumn"/>.
        /// </summary>
        public DataColumn Column { get; }
    }

    /// <summary>
    /// Public page-level reader for a single column chunk.
    /// </summary>
    public sealed class ParquetColumnPageReader {
        private readonly ParquetRowGroupReader _rowGroupReader;
        private readonly long _rowGroupRowCount;
        private ColumnIndex? _columnIndex;

        internal ParquetColumnPageReader(
            ParquetRowGroupReader rowGroupReader,
            DataField field,
            ColumnChunk columnChunk,
            OffsetIndex offsetIndex,
            ColumnIndex? columnIndex) {
            _rowGroupReader = rowGroupReader ?? throw new ArgumentNullException(nameof(rowGroupReader));
            Field = field ?? throw new ArgumentNullException(nameof(field));
            ColumnChunk = columnChunk ?? throw new ArgumentNullException(nameof(columnChunk));
            OffsetIndex = offsetIndex ?? throw new ArgumentNullException(nameof(offsetIndex));
            _columnIndex = columnIndex;
            _rowGroupRowCount = rowGroupReader.RowCount;
        }

        /// <summary>
        /// Field this page reader is bound to.
        /// </summary>
        public DataField Field { get; }

        /// <summary>
        /// Column chunk metadata for the field.
        /// </summary>
        public ColumnChunk ColumnChunk { get; }

        /// <summary>
        /// Offset index for the column chunk.
        /// </summary>
        public OffsetIndex OffsetIndex { get; }

        /// <summary>
        /// Column index for the column chunk when present.
        /// </summary>
        public ColumnIndex? ColumnIndex => _columnIndex;

        /// <summary>
        /// Number of data pages in the column chunk.
        /// </summary>
        public int PageCount => OffsetIndex.PageLocations.Count;

        /// <summary>
        /// Reads a single data page by ordinal.
        /// </summary>
        public async Task<ParquetDataPage> ReadPageAsync(int pageOrdinal, CancellationToken cancellationToken = default) {
            if(pageOrdinal < 0 || pageOrdinal >= PageCount) {
                throw new ArgumentOutOfRangeException(nameof(pageOrdinal), pageOrdinal,
                    $"Page ordinal {pageOrdinal} is outside the available page range 0..{PageCount - 1}.");
            }

            DataColumn column = await _rowGroupReader.ReadColumnPagesAsync(Field, new[] { pageOrdinal }, cancellationToken);
            return new ParquetDataPage(
                pageOrdinal,
                OffsetIndex.PageLocations[pageOrdinal],
                GetRowCount(pageOrdinal),
                column);
        }

        /// <summary>
        /// Reads multiple data pages by ordinal, preserving page order.
        /// </summary>
        public async Task<IReadOnlyList<ParquetDataPage>> ReadPagesAsync(
            IReadOnlyCollection<int> pageOrdinals,
            CancellationToken cancellationToken = default) {
            if(pageOrdinals == null)
                throw new ArgumentNullException(nameof(pageOrdinals));

            int[] orderedPages = pageOrdinals
                .Distinct()
                .OrderBy(i => i)
                .ToArray();

            var pages = new List<ParquetDataPage>(orderedPages.Length);
            foreach(int pageOrdinal in orderedPages) {
                pages.Add(await ReadPageAsync(pageOrdinal, cancellationToken));
            }

            return pages;
        }

        /// <summary>
        /// Returns the on-disk column index when present, or computes one page-by-page when it is absent.
        /// </summary>
        public async Task<ColumnIndex?> GetColumnIndexAsync(CancellationToken cancellationToken = default) {
            _columnIndex ??= await _rowGroupReader.GetOrCreateColumnIndexAsync(Field, cancellationToken);
            return _columnIndex;
        }

        private long GetRowCount(int pageOrdinal) {
            long firstRowIndex = OffsetIndex.PageLocations[pageOrdinal].FirstRowIndex;
            long nextFirstRowIndex = pageOrdinal + 1 < PageCount
                ? OffsetIndex.PageLocations[pageOrdinal + 1].FirstRowIndex
                : _rowGroupRowCount;
            return nextFirstRowIndex - firstRowIndex;
        }
    }
}
