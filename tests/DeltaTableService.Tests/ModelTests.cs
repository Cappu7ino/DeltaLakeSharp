// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Microsoft.ADMS.Testing.DeltaTableService.Client.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ADMS.Testing.DeltaTableService.Tests
{
    /// <summary>
    /// Unit tests for the public model classes.
    /// </summary>
    [TestClass]
    public class ModelTests
    {
        // ================================================================== //
        //  StorageConfig
        // ================================================================== //

        [TestMethod]
        public void StorageConfig_Constructor_SetsProperties()
        {
            var config = new StorageConfig("myaccount", "mytoken");
            Assert.AreEqual("myaccount", config.StorageAccount);
            Assert.AreEqual("mytoken", config.SasToken);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void StorageConfig_NullStorageAccount_Throws()
        {
            _ = new StorageConfig(null!, "token");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void StorageConfig_NullSasToken_Throws()
        {
            _ = new StorageConfig("account", null!);
        }

        // ================================================================== //
        //  TableSchema
        // ================================================================== //

        [TestMethod]
        public void TableSchema_Constructor_SetsColumns()
        {
            var columns = new List<ColumnDefinition>
            {
                new ColumnDefinition("id", "int32"),
                new ColumnDefinition("name", "string"),
            };

            var schema = new TableSchema(columns);
            Assert.AreEqual(2, schema.Columns.Count);
            Assert.AreEqual("id", schema.Columns[0].Name);
            Assert.AreEqual("string", schema.Columns[1].DataType);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void TableSchema_NullColumns_Throws()
        {
            _ = new TableSchema(null!);
        }

        // ================================================================== //
        //  ColumnDefinition
        // ================================================================== //

        [TestMethod]
        public void ColumnDefinition_Constructor_SetsProperties()
        {
            var col = new ColumnDefinition("age", "int64");
            Assert.AreEqual("age", col.Name);
            Assert.AreEqual("int64", col.DataType);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ColumnDefinition_NullName_Throws()
        {
            _ = new ColumnDefinition(null!, "string");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ColumnDefinition_NullDataType_Throws()
        {
            _ = new ColumnDefinition("col", null!);
        }

        // ================================================================== //
        //  ExecuteResult
        // ================================================================== //

        [TestMethod]
        public void ExecuteResult_Constructor_SetsAllProperties()
        {
            var rows = new List<Dictionary<string, object>>
            {
                new Dictionary<string, object> { ["a"] = 1 },
            };

            var result = new ExecuteResult(true, "ok", rows);
            Assert.IsTrue(result.Success);
            Assert.AreEqual("ok", result.Message);
            Assert.AreEqual(1, result.Result.Count);
        }

        [TestMethod]
        public void ExecuteResult_NullMessage_DefaultsToEmpty()
        {
            var result = new ExecuteResult(false, null);
            Assert.IsFalse(result.Success);
            Assert.AreEqual(string.Empty, result.Message);
        }

        [TestMethod]
        public void ExecuteResult_NullResult_DefaultsToEmptyList()
        {
            var result = new ExecuteResult(true, "ok");
            Assert.AreEqual(0, result.Result.Count);
        }

        // ================================================================== //
        //  MergeOptions
        // ================================================================== //

        [TestMethod]
        public void MergeOptions_Constructor_SetsRequiredProperties()
        {
            var opts = new MergeOptions("target.id = source.id");
            Assert.AreEqual("target.id = source.id", opts.Predicate);
            Assert.AreEqual("source", opts.SourceAlias);
            Assert.AreEqual("target", opts.TargetAlias);
        }

        [TestMethod]
        public void MergeOptions_Constructor_CustomAliases()
        {
            var opts = new MergeOptions("t.id = s.id", "s", "t");
            Assert.AreEqual("s", opts.SourceAlias);
            Assert.AreEqual("t", opts.TargetAlias);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void MergeOptions_NullPredicate_Throws()
        {
            _ = new MergeOptions(null!);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void MergeOptions_NullSourceAlias_Throws()
        {
            _ = new MergeOptions("pred", null!, "target");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void MergeOptions_NullTargetAlias_Throws()
        {
            _ = new MergeOptions("pred", "source", null!);
        }

        [TestMethod]
        public void MergeOptions_ToDictionary_MinimalContainsRequiredKeys()
        {
            var opts = new MergeOptions("target.id = source.id");
            var dict = opts.ToDictionary();

            Assert.AreEqual("target.id = source.id", dict["predicate"]);
            Assert.AreEqual("source", dict["source_alias"]);
            Assert.AreEqual("target", dict["target_alias"]);

            // No optional keys should be present.
            Assert.IsFalse(dict.ContainsKey("when_matched_update_all"));
            Assert.IsFalse(dict.ContainsKey("when_matched_update_set"));
            Assert.IsFalse(dict.ContainsKey("when_matched_delete_predicate"));
            Assert.IsFalse(dict.ContainsKey("when_not_matched_insert_all"));
            Assert.IsFalse(dict.ContainsKey("when_not_matched_insert_set"));
            Assert.IsFalse(dict.ContainsKey("when_not_matched_by_source_delete_predicate"));
            Assert.IsFalse(dict.ContainsKey("when_not_matched_by_source_update_set"));
            Assert.IsFalse(dict.ContainsKey("when_not_matched_by_source_update_predicate"));
        }

        [TestMethod]
        public void MergeOptions_ToDictionary_UpsertAll()
        {
            var opts = new MergeOptions("target.id = source.id")
            {
                WhenMatchedUpdateAll = true,
                WhenNotMatchedInsertAll = true,
            };
            var dict = opts.ToDictionary();

            Assert.AreEqual(true, dict["when_matched_update_all"]);
            Assert.AreEqual(true, dict["when_not_matched_insert_all"]);
        }

        [TestMethod]
        public void MergeOptions_ToDictionary_ExplicitUpdateSet()
        {
            var opts = new MergeOptions("target.id = source.id")
            {
                WhenMatchedUpdateSet = new Dictionary<string, string>
                {
                    ["col1"] = "source.col1",
                    ["col2"] = "source.col2 + 1",
                },
            };
            var dict = opts.ToDictionary();

            Assert.IsTrue(dict.ContainsKey("when_matched_update_set"));
            var updateSet = (Dictionary<string, string>)dict["when_matched_update_set"];
            Assert.AreEqual("source.col1", updateSet["col1"]);
            Assert.AreEqual("source.col2 + 1", updateSet["col2"]);
            // when_matched_update_all should NOT be present since it's false.
            Assert.IsFalse(dict.ContainsKey("when_matched_update_all"));
        }

        [TestMethod]
        public void MergeOptions_ToDictionary_DeletePredicate()
        {
            var opts = new MergeOptions("target.id = source.id")
            {
                WhenMatchedDeletePredicate = "source.deleted = true",
            };
            var dict = opts.ToDictionary();

            Assert.AreEqual("source.deleted = true", dict["when_matched_delete_predicate"]);
        }

        [TestMethod]
        public void MergeOptions_ToDictionary_NotMatchedBySource()
        {
            var opts = new MergeOptions("target.id = source.id")
            {
                WhenNotMatchedBySourceDeletePredicate = "true",
                WhenNotMatchedBySourceUpdateSet = new Dictionary<string, string>
                {
                    ["active"] = "'false'",
                },
                WhenNotMatchedBySourceUpdatePredicate = "target.active = true",
            };
            var dict = opts.ToDictionary();

            Assert.AreEqual("true", dict["when_not_matched_by_source_delete_predicate"]);
            Assert.IsTrue(dict.ContainsKey("when_not_matched_by_source_update_set"));
            Assert.AreEqual("target.active = true", dict["when_not_matched_by_source_update_predicate"]);
        }

        [TestMethod]
        public void MergeOptions_ToDictionary_EmptyDictNotIncluded()
        {
            var opts = new MergeOptions("target.id = source.id")
            {
                WhenMatchedUpdateSet = new Dictionary<string, string>(),  // empty
                WhenNotMatchedInsertSet = new Dictionary<string, string>(),
                WhenNotMatchedBySourceUpdateSet = new Dictionary<string, string>(),
            };
            var dict = opts.ToDictionary();

            // Empty dicts should NOT be included in the serialised output.
            Assert.IsFalse(dict.ContainsKey("when_matched_update_set"));
            Assert.IsFalse(dict.ContainsKey("when_not_matched_insert_set"));
            Assert.IsFalse(dict.ContainsKey("when_not_matched_by_source_update_set"));
        }

        [TestMethod]
        public void MergeOptions_ToDictionary_NotMatchedInsertSet()
        {
            var opts = new MergeOptions("target.id = source.id")
            {
                WhenNotMatchedInsertSet = new Dictionary<string, string>
                {
                    ["id"] = "source.id",
                    ["name"] = "source.name",
                    ["status"] = "'new'",
                },
            };
            var dict = opts.ToDictionary();

            Assert.IsTrue(dict.ContainsKey("when_not_matched_insert_set"));
            var insertSet = (Dictionary<string, string>)dict["when_not_matched_insert_set"];
            Assert.AreEqual(3, insertSet.Count);
            Assert.AreEqual("source.id", insertSet["id"]);
            Assert.AreEqual("'new'", insertSet["status"]);
            Assert.IsFalse(dict.ContainsKey("when_not_matched_insert_all"));
        }
    }
}
