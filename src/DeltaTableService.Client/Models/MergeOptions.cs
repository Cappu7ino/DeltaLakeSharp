// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;

namespace Microsoft.ADMS.Testing.DeltaTableService.Client.Models
{
    /// <summary>
    /// Encapsulates the parameters for a Delta MERGE operation that streams
    /// source data to the server via DoPut. This is semantically equivalent to:
    /// <code>
    /// MERGE INTO target USING source ON predicate
    ///   WHEN MATCHED [AND condition] THEN UPDATE SET col1 = expr1, ...
    ///   WHEN MATCHED [AND condition] THEN DELETE
    ///   WHEN NOT MATCHED [AND condition] THEN INSERT (col1, ...) VALUES (expr1, ...)
    ///   WHEN NOT MATCHED BY SOURCE [AND condition] THEN DELETE
    ///   WHEN NOT MATCHED BY SOURCE [AND condition] THEN UPDATE SET col1 = expr1, ...
    /// </code>
    /// </summary>
    public sealed class MergeOptions
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MergeOptions"/> class.
        /// </summary>
        /// <param name="predicate">
        /// The join predicate between the target and source, e.g. <c>"target.id = source.id"</c>.
        /// </param>
        /// <param name="sourceAlias">
        /// Alias for the source data (default <c>"source"</c>).
        /// </param>
        /// <param name="targetAlias">
        /// Alias for the target Delta table (default <c>"target"</c>).
        /// </param>
        public MergeOptions(string predicate, string sourceAlias = "source", string targetAlias = "target")
        {
            Predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            SourceAlias = sourceAlias ?? throw new ArgumentNullException(nameof(sourceAlias));
            TargetAlias = targetAlias ?? throw new ArgumentNullException(nameof(targetAlias));
        }

        /// <summary>
        /// Gets the join predicate between target and source,
        /// e.g. <c>"target.id = source.id"</c>.
        /// </summary>
        public string Predicate { get; }

        /// <summary>
        /// Gets the alias for the source data stream.
        /// </summary>
        public string SourceAlias { get; }

        /// <summary>
        /// Gets the alias for the target Delta table.
        /// </summary>
        public string TargetAlias { get; }

        // ------------------------------------------------------------------ //
        //  WHEN MATCHED clauses
        // ------------------------------------------------------------------ //

        /// <summary>
        /// When <c>true</c>, matched rows are updated with all columns from the source.
        /// Equivalent to <c>WHEN MATCHED THEN UPDATE SET *</c>.
        /// </summary>
        public bool WhenMatchedUpdateAll { get; set; }

        /// <summary>
        /// Explicit column-level update assignments for matched rows,
        /// e.g. <c>{"col1": "source.col1", "col2": "source.col2 + 1"}</c>.
        /// Equivalent to <c>WHEN MATCHED THEN UPDATE SET col1 = expr1, ...</c>.
        /// When <see cref="WhenMatchedUpdateAll"/> is <c>true</c>, this is ignored.
        /// </summary>
        public Dictionary<string, string> WhenMatchedUpdateSet { get; set; }

        /// <summary>
        /// An optional predicate for deleting matched rows,
        /// e.g. <c>"source.deleted = true"</c>.
        /// Equivalent to <c>WHEN MATCHED AND condition THEN DELETE</c>.
        /// When set to <c>"true"</c> (or any truthy expression), all matched
        /// rows are deleted unconditionally.
        /// </summary>
        public string WhenMatchedDeletePredicate { get; set; }

        // ------------------------------------------------------------------ //
        //  WHEN NOT MATCHED clauses (source rows with no target match)
        // ------------------------------------------------------------------ //

        /// <summary>
        /// When <c>true</c>, unmatched source rows are inserted with all columns.
        /// Equivalent to <c>WHEN NOT MATCHED THEN INSERT *</c>.
        /// </summary>
        public bool WhenNotMatchedInsertAll { get; set; }

        /// <summary>
        /// Explicit column-level insert assignments for unmatched source rows,
        /// e.g. <c>{"col1": "source.col1", "col2": "'default'"}</c>.
        /// Equivalent to <c>WHEN NOT MATCHED THEN INSERT (col1, col2) VALUES (expr1, expr2)</c>.
        /// When <see cref="WhenNotMatchedInsertAll"/> is <c>true</c>, this is ignored.
        /// </summary>
        public Dictionary<string, string> WhenNotMatchedInsertSet { get; set; }

        // ------------------------------------------------------------------ //
        //  WHEN NOT MATCHED BY SOURCE clauses (target rows with no source match)
        // ------------------------------------------------------------------ //

        /// <summary>
        /// An optional predicate for deleting target rows that have no match
        /// in the source, e.g. <c>"true"</c> to delete all unmatched target rows.
        /// Equivalent to <c>WHEN NOT MATCHED BY SOURCE [AND condition] THEN DELETE</c>.
        /// </summary>
        public string WhenNotMatchedBySourceDeletePredicate { get; set; }

        /// <summary>
        /// Explicit column-level update assignments for target rows that have
        /// no match in the source,
        /// e.g. <c>{"active": "'false'"}</c>.
        /// Equivalent to <c>WHEN NOT MATCHED BY SOURCE THEN UPDATE SET col = expr</c>.
        /// </summary>
        public Dictionary<string, string> WhenNotMatchedBySourceUpdateSet { get; set; }

        /// <summary>
        /// An optional predicate that gates the
        /// <see cref="WhenNotMatchedBySourceUpdateSet"/> clause,
        /// e.g. <c>"target.active = true"</c>.
        /// </summary>
        public string WhenNotMatchedBySourceUpdatePredicate { get; set; }

        // ------------------------------------------------------------------ //
        //  Serialisation helper (used by FlightClientWrapper)
        // ------------------------------------------------------------------ //

        /// <summary>
        /// Converts the merge options into a dictionary suitable for JSON
        /// serialisation as part of the DoPut command descriptor.
        /// Only non-default / non-null properties are included.
        /// </summary>
        internal Dictionary<string, object> ToDictionary()
        {
            var dict = new Dictionary<string, object>
            {
                ["predicate"] = Predicate,
                ["source_alias"] = SourceAlias,
                ["target_alias"] = TargetAlias,
            };

            if (WhenMatchedUpdateAll)
            {
                dict["when_matched_update_all"] = true;
            }

            if (WhenMatchedUpdateSet != null && WhenMatchedUpdateSet.Count > 0)
            {
                dict["when_matched_update_set"] = WhenMatchedUpdateSet;
            }

            if (WhenMatchedDeletePredicate != null)
            {
                dict["when_matched_delete_predicate"] = WhenMatchedDeletePredicate;
            }

            if (WhenNotMatchedInsertAll)
            {
                dict["when_not_matched_insert_all"] = true;
            }

            if (WhenNotMatchedInsertSet != null && WhenNotMatchedInsertSet.Count > 0)
            {
                dict["when_not_matched_insert_set"] = WhenNotMatchedInsertSet;
            }

            if (WhenNotMatchedBySourceDeletePredicate != null)
            {
                dict["when_not_matched_by_source_delete_predicate"] = WhenNotMatchedBySourceDeletePredicate;
            }

            if (WhenNotMatchedBySourceUpdateSet != null && WhenNotMatchedBySourceUpdateSet.Count > 0)
            {
                dict["when_not_matched_by_source_update_set"] = WhenNotMatchedBySourceUpdateSet;
            }

            if (WhenNotMatchedBySourceUpdatePredicate != null)
            {
                dict["when_not_matched_by_source_update_predicate"] = WhenNotMatchedBySourceUpdatePredicate;
            }

            return dict;
        }
    }
}
