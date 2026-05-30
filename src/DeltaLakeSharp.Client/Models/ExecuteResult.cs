using System.Collections.Generic;

namespace DeltaLakeSharp.Client.Models
{
    /// <summary>
    /// Represents the result of a Spark SQL execution sent via the
    /// Arrow Flight <c>DoAction</c> RPC (e.g. <c>execute_dml</c>, <c>create_table</c>).
    /// </summary>
    public sealed class ExecuteResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ExecuteResult"/> class.
        /// </summary>
        /// <param name="success">Whether the SQL execution succeeded.</param>
        /// <param name="message">A human-readable status or error message.</param>
        /// <param name="result">Optional result rows returned by SELECT or DML statements.</param>
        public ExecuteResult(bool success, string message, IReadOnlyList<Dictionary<string, object?>>? result = null)
        {
            Success = success;
            Message = message ?? string.Empty;
            Result = result ?? new List<Dictionary<string, object?>>();
        }

        /// <summary>
        /// Gets a value indicating whether the SQL statement executed successfully.
        /// </summary>
        public bool Success { get; }

        /// <summary>
        /// Gets the status or error message from the server.
        /// </summary>
        public string Message { get; }

        /// <summary>
        /// Gets the result rows (if any). Each dictionary maps column name to value.
        /// Empty for DDL statements or failed executions.
        /// </summary>
        public IReadOnlyList<Dictionary<string, object?>> Result { get; }
    }
}
