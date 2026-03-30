using System.IO;
using System.Threading.Tasks;

namespace Parquet.Compatibility;

internal static class AsyncCompatibility
{
    public static ValueTask DisposeAsync(Stream stream)
    {
        stream.Dispose();
        return default;
    }
}
