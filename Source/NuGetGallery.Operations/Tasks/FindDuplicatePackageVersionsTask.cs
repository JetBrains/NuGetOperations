using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AnglicanGeek.DbExecutor;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace NuGetGallery.Operations.Tasks
{
    [Command("findduplicatepackageversions", "Finds Duplicate Package Versions", AltName = "fdpv", IsSpecialPurpose = true)]
    public class FindDuplicatePackageVersionsTask : DatabaseAndStorageTask
    {
        [Option("The Location to write the CSV report, if any", AltName="o")]
        public string OutputFile { get; set; }

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

                // Group by Id and and SemVer
                Log.Info("Grouping by Package ID and Actual Version...");
                var groups = packages.GroupBy(p => new { p.Id, Version = SemanticVersionExtensions.Normalize(p.Version) });

                // Find any groups with more than one entry
                Log.Info("Finding Duplicates...");
                var dups = groups.Where(g => g.Count() > 1);

                // Print them out
                StringBuilder csv = new StringBuilder();
                csv.AppendLine("Key,PackageRegistrationKey,Id,Version,CanonicalVersion,Hash,LastUpdated,Created,Listed,Latest,OwnerNames,OwnerEmails");
                int dupsUnlistedCount = 0;
                int latestCount = 0;
                foreach (var dup in dups)
                {
                    Log.Info("Found Duplicates of: {0} {1}", dup.Key.Id, dup.Key.Version);
                    int listedCount = 0;
                    foreach (var package in dup)
                    {
                        if (package.Listed) { listedCount++; }
                        if (package.Latest) { latestCount++; }
                    }
                    if (listedCount == 1)
                    {
                        dupsUnlistedCount++;
                    }
                }
                var totalDupes = dups.Count();
                Log.Info("Found {0} Packages with duplicates.", totalDupes);
                Log.Info(" {0} of them have no listed duplicates.", dupsUnlistedCount);
                Log.Info(" {0} of them have multiple listed duplicates.", totalDupes - dupsUnlistedCount);
                if (latestCount > 0)
                {
                    Log.Warn(" {0} of them are the latest version of the relevant package", latestCount);
                }
                else
                {
                    Log.Info(" NONE of them are the latest version of the relevant package");
                }

                if (!String.IsNullOrEmpty(OutputFile))
                {
                    File.WriteAllText(OutputFile, csv.ToString());
                    Log.Info("Wrote report to {0}", OutputFile);
                }
            }
        }
    }

    public class PackageOwner
    {
        public string Username { get; set; }
        public string EmailAddress { get; set; }
    }

    public class PackageSummary
    {
        public int Key { get; set; }
        public int PackageRegistrationKey { get; set; }
        public string Id { get; set; }
        public string Version { get; set; }
        public string Hash { get; set; }
        public bool Listed { get; set; }
        public bool Latest { get; set; }
        public DateTime LastUpdated { get; set; }
        public DateTime Created { get; set; }
    }
}
