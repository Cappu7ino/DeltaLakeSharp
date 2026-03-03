// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace Microsoft.ADMS.Testing.DeltaTableService.Client.Models
{
    /// <summary>
    /// Represents the schema of a Delta table as a list of column definitions.
    /// </summary>
    public sealed class TableSchema
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TableSchema"/> class.
        /// </summary>
        /// <param name="columns">The ordered list of columns in the table.</param>
        public TableSchema(IReadOnlyList<ColumnDefinition> columns)
        {
            Columns = columns ?? throw new System.ArgumentNullException(nameof(columns));
        }

        /// <summary>
        /// Gets the ordered list of columns in the table.
        /// </summary>
        public IReadOnlyList<ColumnDefinition> Columns { get; }
    }

    /// <summary>
    /// Describes a single column in a Delta table schema.
    /// </summary>
    public sealed class ColumnDefinition
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnDefinition"/> class.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="dataType">The column data type (e.g. "string", "int", "long").</param>
        /// <param name="nullable">Whether the column allows null values. Defaults to true.</param>
        public ColumnDefinition(string name, string dataType, bool nullable = true)
        {
            Name = name ?? throw new System.ArgumentNullException(nameof(name));
            DataType = dataType ?? throw new System.ArgumentNullException(nameof(dataType));
            Nullable = nullable;
        }

        /// <summary>
        /// Gets the column name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the data type name (e.g. "string", "int64", "boolean", "timestamp").
        /// </summary>
        public string DataType { get; }

        /// <summary>
        /// Gets whether this column allows null values.
        /// </summary>
        public bool Nullable { get; }
    }
}
