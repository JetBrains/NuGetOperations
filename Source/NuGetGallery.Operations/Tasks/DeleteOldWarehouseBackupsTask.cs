using System;
using System.Data.SqlClient;
using Dapper;
using NuGetGallery.Operations.Common;

namespace NuGetGallery.Operations
{
    [Command("purgewarehousebackups", "Deletes old database backups", AltName = "pwh")]
    public class DeleteOldWarehouseBackupsTask : WarehouseTask
    {
        public override void ExecuteCommand()
        {
            var dbServer = ConnectionString.DataSource;
            var masterConnectionString = Util.GetMasterConnectionString(ConnectionString.ConnectionString);

            Log.Trace("Deleting old warehouse backups for server '{0}':", dbServer);

            using (var db = new SqlConnection(masterConnectionString))
            {
                db.Open();

                var backups = db.Query<Database>(
                    "SELECT name FROM sys.databases WHERE name LIKE 'WarehouseBackup_%' AND state = @state",
                    new { state = Util.OnlineState });

                foreach (var backup in backups)
                {
                    var timestamp = Util.GetDatabaseNameTimestamp(backup);
                    var date = Util.GetDateTimeFromTimestamp(timestamp);
                    if (DateTime.UtcNow.Subtract(TimeSpan.FromDays(7)) > date)
                    {
                        DeleteDatabaseBackup(backup, db);
                    }
                }
            }
        }

        private void DeleteDatabaseBackup(Database backup, SqlConnection db)
        {
            if (!WhatIf)
            {
                db.Execute(string.Format("DROP DATABASE {0}", backup.Name));
            }
            Log.Info("Deleted database {0}.", backup.Name);
        }
    }
}
