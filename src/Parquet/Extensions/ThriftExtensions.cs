using System.IO;
using System.Linq;
using Parquet.File;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet;

/// <summary>
/// Internal data structure helpers
/// </summary>
static class ThriftExtensions {
    public static bool IsAnnotatedWithAny(this SchemaElement schemaElement, ConvertedType[] convertedTypes) {
        if(convertedTypes == null || convertedTypes.Length == 0)
            return false;

        return
           schemaElement.ConvertedType != null &&
           convertedTypes.Any(ct => ct == schemaElement.ConvertedType);
    }

    public static bool IsList(this SchemaElement? se) {
        if(se == null) return false;

        return se.LogicalType?.LIST != null || se.ConvertedType == ConvertedType.LIST;
    }

    public static bool IsMap(this SchemaElement? se) {
        if(se == null) return false;
        return se.LogicalType?.MAP != null || se.ConvertedType == ConvertedType.MAP || se.ConvertedType == ConvertedType.MAP_KEY_VALUE;
    }

    public static bool IsDecimal(this SchemaElement? se) {
        if(se == null) return false;
        return se.LogicalType?.DECIMAL != null || se.ConvertedType == ConvertedType.DECIMAL;
    }

    public static bool IsString(this SchemaElement? se) {
        if(se == null) return false;
        return se.LogicalType?.STRING != null || se.ConvertedType == ConvertedType.UTF8 || se.ConvertedType == ConvertedType.ENUM;
    }

    public static bool IsDate(this SchemaElement? se) {
        if(se == null) return false;
        return se.LogicalType?.DATE != null || se.ConvertedType == ConvertedType.DATE;
    }

    public static bool IsTimestampMillis(this SchemaElement? se) {
        if(se == null) return false;
        return se.LogicalType?.TIMESTAMP?.Unit?.MILLIS != null || se.ConvertedType == ConvertedType.TIMESTAMP_MILLIS;
    }

    public static bool IsTimestampMicros(this SchemaElement? se) {
        if(se == null) return false;
        return se.LogicalType?.TIMESTAMP?.Unit?.MICROS != null || se.ConvertedType == ConvertedType.TIMESTAMP_MICROS;
    }

    public static bool IsNullable(this SchemaElement schemaElement) {
        return schemaElement.RepetitionType != FieldRepetitionType.REQUIRED;
    }

    public static FieldPath GetPath(this ColumnChunk columnChunk) {
        return new FieldPath(columnChunk.MetaData!.PathInSchema);
    }

    public static string Describe(this SchemaElement se) {
        return $"[n: {se.Name}, t: {se.Type}, ct: {se.ConvertedType}, rt: {se.RepetitionType}, c: {se.NumChildren}]";
    }

    public static FieldPath GetPath(this ThriftFooter footer, ColumnChunk cc) {
        if(cc.MetaData?.PathInSchema != null)
            return new FieldPath(cc.MetaData.PathInSchema);

        // Fallback: look up the SchemaElement for this chunk and derive path from the schema tree
        SchemaElement? se = footer.GetSchemaElement(cc);
        if(se != null)
            return footer.GetPath(se);

        throw new InvalidDataException("Unable to determine column path (no MetaData.PathInSchema and no matching schema element).");
    }
}
