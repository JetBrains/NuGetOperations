using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using Dapper;

namespace NuGetGallery.Operations
{
    [Command("purgedatabasebackups", "Deletes old database backups", AltName = "pdb")]
    public class DeleteOldDatabaseBackupsTask : DatabaseTask
    {
        public override void ExecuteCommand()
        {
            var dbServer = ConnectionString.DataSource;
            var masterConnectionString = Util.GetMasterConnectionString(ConnectionString.ConnectionString);

            Log.Trace("Deleting old database backups for server '{0}':", dbServer);

            using (var db = new SqlConnection(masterConnectionString))
            {
                db.Open();

                var backups = db.Query<Database>(
                    "SELECT name FROM sys.databases WHERE name LIKE 'Backup_%' AND state = @state",
                    new { state = Util.OnlineState }).ToArray();

                // Policy #1: retain last backup each day for last week [day = UTC day]
                // Policy #2: retain the last 5 backups
                var dailyBackups = backups.OrderByDescending(GetTimestamp).GroupBy(GetDay).Take(8).Select(Enumerable.Last);
                var latestBackups = backups.OrderByDescending(GetTimestamp).Take(5);

                var backupsToSave = new HashSet<Database>();
                backupsToSave.UnionWith(dailyBackups);
                backupsToSave.UnionWith(latestBackups);

                if (backupsToSave.Count <= 0)
                {
                    throw new ApplicationException("Abort - sanity check failed - we are about to delete all backups");
                }

                foreach (var backup in backups)
                {
                    if (backupsToSave.Contains(backup))
                    {
                        Log.Info("Retained backup: " + backup.Name);
                    }
                    else
                    {
                        DeleteDatabaseBackup(backup, db);
                    }
                }
            }
        }

        private static DateTime GetTimestamp(Database db)
        {
            var timestamp = Util.GetDatabaseNameTimestamp(db);
            var date = Util.GetDateTimeFromTimestamp(timestamp);
            return date;
        }

        private static int GetDay(Database db)
        {
            var timestamp = Util.GetDatabaseNameTimestamp(db);
            var date = Util.GetDateTimeFromTimestamp(timestamp);
            if (date.Kind != DateTimeKind.Utc)
            {
                throw new InvalidDataException("DateTime must be Utc");
            }

            var daysSinceMillenium = (int)date.Subtract(new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalDays;
            return daysSinceMillenium;
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
