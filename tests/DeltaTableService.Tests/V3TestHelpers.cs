// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.ADMS.Testing.DeltaTableService.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ADMS.Testing.DeltaTableService.Tests
{
    internal static class V3TestHelpers
    {
        internal static Schema BuildIdNameSchema()
        {
            return new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, nullable: true))
                .Field(new Field("name", StringType.Default, nullable: true))
                .Build();
        }

        internal static RecordBatch BuildIdNameBatch(int[] ids, string[] names)
        {
            return new RecordBatch.Builder()
                .Append("id", nullable: true, new Int32Array.Builder().AppendRange(ids).Build())
                .Append("name", nullable: true, new StringArray.Builder().AppendRange(names).Build())
                .Build();
        }

        internal static async IAsyncEnumerable<RecordBatch> ToAsyncEnumerable(RecordBatch batch)
        {
            yield return batch;
            await Task.CompletedTask;
        }

        internal static string ReadStringValue(IArrowArray array, int index)
        {
            return array switch
            {
                StringArray sa => sa.GetString(index),
                StringViewArray sva => sva.GetString(index),
                LargeStringArray lsa => lsa.GetString(index),
                _ => throw new AssertFailedException(
                    $"Unexpected string column type: {array.GetType().FullName}")
            } ?? string.Empty;
        }

        internal static async Task<List<(int id, string? name)>> ReadAllRowsSorted(
            DeltaTableServiceClient client,
            string tablePath)
        {
            var ids = new List<int>();
            var names = new List<string?>();

            await foreach (RecordBatch batch in client.ReadTableAsync(tablePath))
            {
                var idArray = (Int32Array)batch.Column(0);
                IArrowArray nameCol = batch.Column(1);

                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(idArray.GetValue(i)!.Value);
                    names.Add(ReadStringValue(nameCol, i));
                }
            }

            return ids.Zip(names, (id, name) => (id, name))
                .OrderBy(x => x.id)
                .ToList();
        }
    }
}
