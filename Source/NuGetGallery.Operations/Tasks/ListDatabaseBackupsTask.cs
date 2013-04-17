using System.Data.SqlClient;
using Dapper;

namespace NuGetGallery.Operations
{
    [Command("listdatabasebackups", "List database backups at the specified database server", AltName = "ldb")]
    public class ListDatabaseBackupsTask : DatabaseTask
    {
        public override void ExecuteCommand()
        {
            var dbServer = ConnectionString.DataSource;
            var masterConnectionString = Util.GetMasterConnectionString(ConnectionString.ConnectionString);

            Log.Info("Listing backups for server '{0}':", dbServer);
            
            using (var db = new SqlConnection(masterConnectionString))
            {
                db.Open();

                var backups = db.Query<Database>(
                    "SELECT name FROM sys.databases WHERE name LIKE 'Backup_%' AND state = @state",
                    new { state = Util.OnlineState });

                foreach(var backup in backups)
                {
                    var timestamp = Util.GetDatabaseNameTimestamp(backup);
                    var date = Util.GetDateTimeFromTimestamp(timestamp);

                    Log.Info("{0} ({1})", timestamp, date);
                }
            }
        }
    }
}
