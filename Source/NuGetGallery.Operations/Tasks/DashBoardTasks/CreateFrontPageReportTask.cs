using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json.Linq;
using NuGetGallery.Operations.Common;
using AnglicanGeek.DbExecutor;
using System;

namespace NuGetGallery.Operations
{
    [Command("createpackageuploadreport", "Creates the over-all package upload trending report for Gallery", AltName = "cpurep")]
    public class CreatePackageUploadReportTask : ReportsTask
    {
        private string SqlQuery = @"SELECT Count (*) FROM [dbo].[Packages] where [Created] < '{0}'";
        private DateTime startingTime = new DateTime(2011, 04, 01);
        
        public override void ExecuteCommand()
        {                             
            List<Tuple<string, string>> uploadDataPoints = new List<Tuple<string, string>>();
               using (var sqlConnection = new SqlConnection(ConnectionString.ConnectionString))
               {
                   using (var dbExecutor = new SqlExecutor(sqlConnection))
                   {
                       sqlConnection.Open();
               
                       while (startingTime < DateTime.Now)
                       {
                           var packageCount = dbExecutor.Query<Int32>(string.Format(SqlQuery, startingTime.ToString("yyy-MM-dd"))).SingleOrDefault();
                           uploadDataPoints.Add(new Tuple<string,string>(String.Format("{0:MMM}", startingTime).ToString() + startingTime.Year.ToString().Substring(2,2) , packageCount.ToString()));
                           startingTime = startingTime.AddMonths(4);
                       }
                   }
              }
               JArray reportObject = ReportHelpers.GetJson(uploadDataPoints);
               ReportHelpers.CreateBlob(ReportStorage, "UploadDataPoints" + ".json", "dashboard", "application/json", ReportHelpers.ToStream(reportObject));
        }       
    }
}
