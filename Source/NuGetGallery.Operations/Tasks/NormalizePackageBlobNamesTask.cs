using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AnglicanGeek.DbExecutor;

namespace NuGetGallery.Operations.Tasks
{
    [Command("normalizepackageblobnames", "Standardize all blob names as lowercase with a normalized SemVer string.", AltName = "npbn", MaxArgs = 0)]
    public class NormalizePackageBlobNamesTask : DatabaseAndStorageTask
    {
        public override void ExecuteCommand()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString.ConnectionString))
            using (var dbExecutor = new SqlExecutor(sqlConnection))
            {
                sqlConnection.Open();
                
                // Query for packages
                var packages = dbExecutor.Query<PackageSummary>(@"
                    SELECT p.[Key], p.PackageRegistrationKey, r.Id, p.Version, p.Hash, p.LastUpdated, p.Created, p.Listed, p.IsLatestStable
                    FROM   Packages p
                    INNER JOIN PackageRegistrations r ON p.PackageRegistrationKey = r.[Key]");

                // Find all where the normalized version does not match it's actual version
                var toBeNormalized = packages.Where(p => !String.Equals(p.Version, SemanticVersionExtensions.Normalize(p.Version), StringComparison.Ordinal));
                var normedCount = 0;
                var missingCount = 0;

                var client = CreateBlobClient();
                var container = client.GetContainerReference("packages");
                foreach (var normalize in toBeNormalized)
                {
                    // Test if there is a blob with the right name
                    string normalVer = SemanticVersionExtensions.Normalize(normalize.Version);
                    string normalized = String.Format("{0}.{1}.nupkg", normalize.Id, normalVer).ToLowerInvariant();
                    if (container.GetBlockBlobReference(normalized).Exists())
                    {
                        Log.Info("Setting {0}@{1} -> {2}", normalize.Id, normalize.Version, normalVer);
                        normedCount++;
                    }
                    else
                    {
                        Log.Warn("Can't normalize {0}@{1}, missing blob: {2}.", normalize.Id, normalize.Version, normalized);
                        missingCount++;
                    }
                }
                Log.Info(" {0} packages normalized", normedCount);
                Log.Info(" {0} packages could not be normalized as the relevant blob does not exist", missingCount);
            }
        }
    }
}
