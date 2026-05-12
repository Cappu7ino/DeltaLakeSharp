using System;

namespace DeltaLakeSharp.Benchmark
{
    /// <summary>
    /// Provides logging utilities for benchmark tests with colored console output.
    /// </summary>
    internal static class Logger
    {
        /// <summary>
        /// The prefix used for all log messages.
        /// </summary>
        private const string LogPrefix = "[DeltaLakeSharp.Benchmark]";

        /// <summary>
        /// Logs an informational message to the console with the specified color.
        /// </summary>
        /// <param name="message">The message to log.</param>
        /// <param name="color">The console color to use for the message. Defaults to Blue.</param>
        public static void Info(string message, ConsoleColor color = ConsoleColor.Blue)
        {
            Console.ForegroundColor = color;
            Console.WriteLine($"{LogPrefix} {message}");
            Console.ForegroundColor = ConsoleColor.White;
        }

        /// <summary>
        /// Logs an error message to the console in red color.
        /// </summary>
        /// <param name="message">The error message to log.</param>
        public static void Error(string message) => Info(message, ConsoleColor.Red);
    }
}
