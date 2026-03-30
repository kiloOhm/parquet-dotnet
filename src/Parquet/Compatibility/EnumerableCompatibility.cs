#if NET48
using System;
using System.Collections.Generic;

namespace Parquet.Extensions;

internal static class EnumerableCompatibility
{
    public static IEnumerable<TSource[]> Chunk<TSource>(this IEnumerable<TSource> source, int size)
    {
        if (source is null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (size <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(size));
        }

        using var enumerator = source.GetEnumerator();
        while (enumerator.MoveNext())
        {
            var chunk = new TSource[size];
            chunk[0] = enumerator.Current;

            var count = 1;
            while (count < size && enumerator.MoveNext())
            {
                chunk[count++] = enumerator.Current;
            }

            if (count == size)
            {
                yield return chunk;
                continue;
            }

            var lastChunk = new TSource[count];
            Array.Copy(chunk, lastChunk, count);
            yield return lastChunk;
        }
    }
}
#endif
