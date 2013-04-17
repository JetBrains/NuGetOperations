using System.Data.SqlClient;
using Dapper;

namespace NuGetGallery.Operations
{
    [Command("deletepackageversion", "Delete a specific package version", AltName = "dpv")]
    public class DeletePackageVersionTask : DatabasePackageVersionTask
    {
        public override void ExecuteCommand()
        {
            using (var db = new SqlConnection(ConnectionString.ConnectionString))
            {
                db.Open();

                var package = Util.GetPackage(
                    db,
                    PackageId,
                    PackageVersion);

                if (package == null)
                {
                    Log.Error("Package version does not exist: '{0}.{1}'", PackageId, PackageVersion);
                    return;
                }

                Log.Info(
                    "Deleting package data for '{0}.{1}'", 
                    package.Id, 
                    package.Version);

                if (!WhatIf)
                {
                    db.Execute(
                        "DELETE pa FROM PackageAuthors pa JOIN Packages p ON p.[Key] = pa.PackageKey WHERE p.[Key] = @key",
                        new { key = package.Key });
                    db.Execute(
                        "DELETE pd FROM PackageDependencies pd JOIN Packages p ON p.[Key] = pd.PackageKey WHERE p.[Key] = @key",
                        new { key = package.Key });
                    db.Execute(
                        "DELETE ps FROM PackageStatistics ps JOIN Packages p ON p.[Key] = ps.PackageKey WHERE p.[Key] = @key",
                        new { key = package.Key });
                    db.Execute(
                        "DELETE pf FROM PackageFrameworks pf JOIN Packages p ON p.[Key] = pf.Package_Key WHERE p.[Key] = @key",
                        new { key = package.Key });
                    db.Execute(
                        "DELETE p FROM Packages p JOIN PackageRegistrations pr ON pr.[Key] = p.PackageRegistrationKey WHERE p.[Key] = @key",
                        new { key = package.Key });
                }

                new DeletePackageFileTask {
                    StorageAccount = StorageAccount,
                    PackageId = package.Id,
                    PackageVersion = package.Version,
                    PackageHash = package.Hash,
                    WhatIf = WhatIf
                }.ExecuteCommand();
            }
        }
    }
}
