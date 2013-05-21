using System;
using System.Data.SqlClient;
using AnglicanGeek.DbExecutor;
using NuGetGallery.Operations.Common;
using System.Text;

namespace NuGetGallery.Operations
{
    [Command("createdatabaseuser", "Creates a user account in the database which has just access to NuGetGallery", AltName = "cdu", MaxArgs = 0)]
    public class CreateDatabaseUserTask : DatabaseTask
    {
        [Option("Environment name", AltName = "env")]
        public string EnvironmentName { get; set; }

        //Expose the properties so that they can be used by tests and other tasks.
        public string UserName { get; set; }
        public string Password { get; set; }

        public override void ExecuteCommand()
        {
            var masterConnectionString = Util.GetMasterConnectionString(ConnectionString.ConnectionString);
            UserName = string.Format("nuget-{0}-site{1}", EnvironmentName, DateTime.Now.ToString("MMMddyyyy"));
            Password = DateTime.Now.ToString("MMMddyy") + "!" + Convert.ToBase64String(Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()));
            //Connect to the master database and create the login
            Log.Info("Creating the LOGIN for {0}", UserName);
            using (var sqlConnection = new SqlConnection(masterConnectionString))
            {
                using (var dbExecutor = new SqlExecutor(sqlConnection))
                {
                    sqlConnection.Open();
                    dbExecutor.Execute(string.Format("CREATE LOGIN [{0}] WITH password='{1}'",UserName,Password));
                }
            }
            //Connect to the Gallery database and create the user.
            Log.Info("Creating the user account for {0}", UserName);
            using (var sqlConnection = new SqlConnection(ConnectionString.ConnectionString))
            {
                using (var dbExecutor = new SqlExecutor(sqlConnection))
                {
                    sqlConnection.Open();
                    dbExecutor.Execute(string.Format("CREATE USER [{0}] FROM LOGIN [{0}]", UserName));
                    dbExecutor.Execute(string.Format("EXEC sp_addrolemember 'db_owner' ,'{0}'", UserName));
                }
            }
           
            Log.Info("Created the site user {0} with password {1}", UserName, Password);
        }
    }
}
                
       
