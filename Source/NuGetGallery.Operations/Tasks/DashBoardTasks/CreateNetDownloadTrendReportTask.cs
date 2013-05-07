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
    [Command("createpackagedownloadreport", "Creates the over-all download trending report for Gallery", AltName = "cndtrep")]
    public class CreateNetDownloadTrendReportTask : ReportsTask
    {    
        private string SqlQuery = @" SELECT SUM(f.DownloadCount) FROM [dbo].[Fact_Download] f JOIN [dbo].[Dimension_Date] dd ON dd.[Id] = f.Dimension_Date_Id WHERE dd.[Date] < '{0}'";
        private DateTime startingTime = new DateTime(2012, 10, 10); 

        public override void ExecuteCommand()
        {
            List<Tuple<string, string>> DownloadDataPoints = new List<Tuple<string, string>>();
            while (startingTime < DateTime.Now)
            {
                using (var sqlConnection = new SqlConnection(ConnectionString.ConnectionString))
                {
                    using (var dbExecutor = new SqlExecutor(sqlConnection))
                    {
                        sqlConnection.Open();
                        //Execute the query and add the result to the list.
                        var packageCount = dbExecutor.Query<Int32>(string.Format(SqlQuery, startingTime.ToString("yyyy-MM-dd"))).FirstOrDefault();
                        DownloadDataPoints.Add(new Tuple<string, string>(String.Format("{0:MMM}", startingTime).ToString() + startingTime.Year.ToString().Substring(2, 2), packageCount.ToString()));
                        startingTime = startingTime.AddMonths(1);

                    }
                }
            }
            //Get JSON object and upload it.
            JArray reportObject = ReportHelpers.GetJson(DownloadDataPoints);
            ReportHelpers.CreateBlob(ReportStorage, "NetDownloadTrend" + ".json", "dashboard", "application/json", ReportHelpers.ToStream(reportObject));
        }
    }
}

