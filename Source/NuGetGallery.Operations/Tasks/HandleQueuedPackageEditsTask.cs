using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NuGet;
using NuGetGallery.Operations.Common;

namespace NuGetGallery.Operations.Tasks
{
    class PackageEdit
    {
        // meta
        public int EditKey { get; set; }
        public int PackageKey { get; set; }
        public string PackageId { get; set; }
        public string Version { get; set; }
        public string EditName { get; set; }
        public int TriedCount { get; set; }

        // edits
        public string Authors { get; set; }
        public string Copyright { get; set; }
        public string Description { get; set; }
        public string IconUrl { get; set; }
        public string LicenseUrl { get; set; }
        public string ProjectUrl { get; set; }
        public string ReleaseNotes { get; set; }
        public string Summary { get; set; }
        public string Tags { get; set; }
        public string Title { get; set; }

        // output
        public string BlobUrl { get; set; }
    }

    [Command("handlequeuededits", "Handle Queued Package Edits", AltName = "hqe", MaxArgs = 0)]
    public class HandleQueuedPackageEditsTask : DatabaseAndStorageTask
    {
        public override void ExecuteCommand()
        {
            // Work to do:
            // 0) Find Pending Edits in DB that have been attempted less than 3 times
            // 1) Backup all old NUPKGS
            // 2) Generate all new NUPKGs (in place), and tell gallery the edit is completed
            var connectionString = ConnectionString.ConnectionString;
            var storageAccount = StorageAccount;

            var edits = ReadEdits(connectionString);
            ConcurrentDictionary<PackageEdit, CloudBlockBlob> blobCache = new ConcurrentDictionary<PackageEdit,CloudBlockBlob>();

            Parallel.ForEach(edits, new ParallelOptions { MaxDegreeOfParallelism = 10 }, edit =>
                {
                    var blob = BackupBlob(edit);
                    blobCache.TryAdd(edit, blob); //should always succeed
                });

            foreach (var edit in edits)
            {
                UpdateNupkgBlob(edit, blobCache[edit], connectionString);
            }
        }

        private List<PackageEdit> ReadEdits(string connectionString)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                var results = connection.Query<PackageEdit>(@";
SELECT [PackageMetadatas].[Key] AS EditKey
      ,[PackageRegistrations].[Id] AS PackageId
      ,[Packages].[Key] AS PackageKey
      ,[Packages].[Version]
      ,[PackageMetadatas].[EditName]
      ,[PackageMetadatas].[TriedCount]
      ,[PackageMetadatas].[Authors]
      ,[PackageMetadatas].[Copyright]
      ,[PackageMetadatas].[Description]
      ,[PackageMetadatas].[IconUrl]
      ,[PackageMetadatas].[LicenseUrl]
      ,[PackageMetadatas].[ProjectUrl]
      ,[PackageMetadatas].[ReleaseNotes]
      ,[PackageMetadatas].[Summary]
      ,[PackageMetadatas].[Tags]
      ,[PackageMetadatas].[Title]
  FROM [PackageMetadatas], [Packages], [PackageRegistrations] WHERE [PackageMetadatas].[PackageKey] = [Packages].[Key] AND [Packages].[PackageRegistrationKey] = [PackageRegistrations].[Key] AND IsCompleted = 0 AND TriedCount < 3");
                return results.ToList();
            }
        }

        // Hack: in WhatIf mode, returns the original blob
        private CloudBlockBlob BackupBlob(PackageEdit edit)
        {
            CloudStorageAccount storageAccount = CurrentEnvironment.MainStorage;
            var blobClient = storageAccount.CreateCloudBlobClient();
            var packagesContainer = Util.GetPackagesBlobContainer(blobClient);

            var latestPackageFileName = Util.GetPackageFileName(edit.PackageId, edit.Version);
            var originalPackageFileName = Util.GetBackupOriginalPackageFileName(edit.PackageId, edit.Version);

            var originalPackageBlob = packagesContainer.GetBlockBlobReference(originalPackageFileName);
            var latestPackageBlob = packagesContainer.GetBlockBlobReference(latestPackageFileName);

            if (!originalPackageBlob.Exists())
            {
                if (WhatIf)
                {
                    return latestPackageBlob; // said hack
                }
                else
                {
                    Log.Info("Backing up blob: {0} to {1}", latestPackageFileName, originalPackageFileName);
                    originalPackageBlob.StartCopyFromBlob(latestPackageBlob);
                    CopyState state = originalPackageBlob.CopyState;
                    while (state == null || state.Status == CopyStatus.Pending)
                    {
                        Log.Info("(sleeping for a copy completion)");
                        Thread.Sleep(3000);
                        originalPackageBlob.FetchAttributes(); // To get a refreshed x-ms-copy-status response header - according to my theoretical understanding

                        //refresh state
                        state = originalPackageBlob.CopyState;
                    }

                    if (state.Status != CopyStatus.Success)
                    {
                        throw new BlobBackupFailedException(string.Format("Blob copy failed: CopyState={0}", state.StatusDescription));
                    }
                }
            }

            return originalPackageBlob;
        }

        private void UpdateNupkgBlob(PackageEdit edit, CloudBlockBlob nupkgBlob, string connectionString)
        {
            // Work to do:
            // 1) Backup old blob, if it is an original
            // 2) Download blob, create new NUPKG locally
            // 3) Upload blob

            List<Action<ManifestMetadata>> edits = new List<Action<ManifestMetadata>>
            { 
                (m) => { m.Authors = edit.Authors; },
                (m) => { m.Copyright = edit.Copyright; },
                (m) => { m.Description = edit.Description; },
                (m) => { m.IconUrl = edit.IconUrl; },
                (m) => { m.LicenseUrl = edit.LicenseUrl; },
                (m) => { m.ProjectUrl = edit.ProjectUrl; },
                (m) => { m.ReleaseNotes = edit.ReleaseNotes; },
                (m) => { m.Summary = edit.Summary; },
                (m) => { m.Title = edit.Title; },
                (m) => { m.Tags = edit.Tags; },
            };

            Log.Info(
                "Processing Edit Key={0}, Package={1}, Version={2}",
                edit.EditKey,
                edit.PackageId,
                edit.Version);

            if (!WhatIf)
            {
                using (var readWriteStream = new MemoryStream())
                {
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        int nr = connection.Execute(@"
                            UPDATE [PackageMetadatas]
                            SET [TriedCount] = [TriedCount] + 1
                            WHERE [Key] = @Key", new { Key = edit.EditKey});

                        if (nr != 1)
                        {
                            throw new InvalidOperationException("Something went wrong, no rows were updated");
                        }
                    }

                    // Download to memory
                    Log.Info("Downloading blob to memory {0}", nupkgBlob.Name);
                    nupkgBlob.DownloadToStream(readWriteStream);

                    // Rewrite in memory
                    Log.Info("Rewriting nupkg package in memory", nupkgBlob.Name);
                    NupkgRewriter.RewriteNupkgManifest(readWriteStream, edits);

                    // Reupload blob
                    Log.Info("Uploading blob from memory {0}", nupkgBlob.Name);
                    readWriteStream.Position = 0;
                    nupkgBlob.UploadFromStream(readWriteStream);

                    // Complete the edit in the gallery DB.
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        int nr = connection.Execute(@"
                            BEGIN TRAN 

                            UPDATE [PackageMetadatas]
                            SET [IsCompleted] = 1
                            WHERE [Key] = @EditKey

                            UPDATE [Packages]
                            SET [MetadataKey] = @EditKey
                              , [LastUpdated] = GETUTCDATE()
                            WHERE [Key] = @PackageKey

                            COMMIT TRAN
                            ", new { edit.EditKey, edit.PackageKey });

                        if (nr != 1)
                        {
                            throw new InvalidOperationException("Something went wrong, no rows were updated");
                        }

                        Log.Info("(success)");
                    }
                }
            }
        }

    }
}
