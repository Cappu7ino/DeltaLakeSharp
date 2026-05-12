# DeltaTableService Client Example

This console project is a compileable SDK consumer sample. It intentionally references the client project's `netstandard2.0` target from a `net8.0` host so the example stays compatible with the broadest public client asset.

## Build

```powershell
dotnet build examples\DeltaTableService.Client.Examples\DeltaTableService.Client.Examples.csproj /p:SkipRustBuild=true
```

## Run

The sample uses `ServiceMode.V3_Rust`, so running it requires the V3 native library to be built or available through the package runtime assets.

```powershell
dotnet run --project examples\DeltaTableService.Client.Examples\DeltaTableService.Client.Examples.csproj -- <optional-local-delta-table-path>
```

When no table path is provided, the sample creates a temporary local Delta table, reads it as streaming Arrow batches, reads it through `DbDataReader`, runs a SQL query, and deletes the temporary table before exit.
