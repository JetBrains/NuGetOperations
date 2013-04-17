using System;
using System.Data.SqlClient;
using System.Threading;
using Dapper;
using NuGetGallery.Operations.Common;

namespace NuGetGallery.Operations
{
    [Command("restoredb", "Restore a database backup which already resides on the specified database server", AltName = "rdb")]
    public class RestoreDatabaseTask : DatabaseTask
    {
        [Option("Name of the backup file", AltName = "n")]
        public string BackupName { get; set; }

        public override void ValidateArguments()
        {
            base.ValidateArguments();
            ArgCheck.Required(BackupName, "BackupName");
        }

        public override void ExecuteCommand()
        {
            using (var db = new SqlConnection(Util.GetMasterConnectionString(ConnectionString.ConnectionString)))
            {
                db.Open();
                
                var restoreDbName = CopyDatabaseForRestore(db);

                using (var restoreDb = new SqlConnection(Util.GetConnectionString(ConnectionString.ConnectionString, restoreDbName)))
                {
                    restoreDb.Open();

                    PrepareDataForRestore(restoreDb);

                    RenameLiveDatabase(db);

                    RenameDatabaseBackup(db, restoreDbName);
                }
            }
        }

        private string CopyDatabaseForRestore(
            SqlConnection masterDb)
        {
            var restoreDbName = string.Format("Restore_{0}", Util.GetTimestamp());
            Log.Info("Copying {0} to {1}.", BackupName, restoreDbName);
            masterDb.Execute(string.Format("CREATE DATABASE {0} AS COPY OF {1}", restoreDbName, BackupName));
            Log.Info("Waiting for copy to complete.");
            WaitForBackupCopy(
                masterDb,
                restoreDbName);
            return restoreDbName;
        }

        private void WaitForBackupCopy(
            SqlConnection masterDb,
            string restoreDbName)
        {
            var timeToGiveUp = DateTime.UtcNow.AddHours(1).AddSeconds(30);
            while (DateTime.UtcNow < timeToGiveUp)
            {
                if (Util.DatabaseExistsAndIsOnline(
                    masterDb,
                    restoreDbName))
                {
                    Log.Info("Copy is complete.");
                    return;
                }
                Thread.Sleep(1 * 60 * 1000);
            }
        }

        private void PrepareDataForRestore(
            SqlConnection db)
        {
            Log.Info("Deleting incomplete jobs.");
            db.Execute("DELETE FROM WorkItems WHERE Completed IS NULL");
            Log.Info("Deleted incomplete jobs.");
        }

        private void RenameDatabaseBackup(
            SqlConnection masterDb,
            string restoreDbName)
        {
            Log.Info("Renaming {0} to NuGetGallery.", restoreDbName);
            var sql = string.Format("ALTER DATABASE {0} MODIFY Name = NuGetGallery", restoreDbName);
            masterDb.Execute(sql);
            Log.Info("Renamed {0} to NuGetGallery.", restoreDbName);
        }

        private void RenameLiveDatabase(
            SqlConnection masterDb)
        {
            var timestamp = Util.GetTimestamp();
            var liveDbName = "Live_" + timestamp;
            Log.Info("Renaming NuGetGallery to {0}.", liveDbName);
            var sql = string.Format("ALTER DATABASE NuGetGallery MODIFY Name = {0}", liveDbName);
            masterDb.Execute(sql);
            Log.Info("Renamed NuGetGallery to {0}.", liveDbName);
        }
    }
}
