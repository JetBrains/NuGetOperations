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
using System.Web.Script.Serialization;
using System.Net;

namespace NuGetGallery.Operations
{
    [Command("createtrendingpackagesreport", "Creates the report for downloads per client in the previous week", AltName = "ccvrep")]
    public class CreateClientVersionReportTask : ReportsTask
    {
        private string SqlQuery = @"
            SELECT Dimension_UserAgent.ClientMajorVersion, Dimension_UserAgent.ClientMinorVersion, SUM(DownloadCount) 'Downloads'
FROM Fact_Download
INNER JOIN Dimension_UserAgent ON Dimension_UserAgent.Id = Fact_Download.Dimension_UserAgent_Id
INNER JOIN Dimension_Date ON Dimension_Date.Id = Fact_Download.Dimension_Date_Id
WHERE Dimension_Date.[Date] >= CONVERT(DATE, DATEADD(day, -7, GETDATE()))
  AND Dimension_Date.[Date] < CONVERT(DATE, GETDATE())
  AND Dimension_UserAgent.ClientCategory = 'NuGet'
  AND Dimension_UserAgent.ClientMajorVersion = 2
  AND Dimension_UserAgent.ClientMinorVersion <= 7
GROUP BY Dimension_UserAgent.ClientMajorVersion, Dimension_UserAgent.ClientMinorVersion
ORDER BY Dimension_UserAgent.ClientMajorVersion, Dimension_UserAgent.ClientMinorVersion
";

        public override void ExecuteCommand()
        {
            List<Tuple<string, string>> clientVersionDataPoints = new List<Tuple<string, string>>();           
            using (SqlConnection connection = new SqlConnection(ConnectionString.ConnectionString))
            {
                connection.Open();

                SqlCommand command = new SqlCommand(SqlQuery, connection);
                command.CommandType = CommandType.Text;

                SqlDataReader reader = command.ExecuteReader();

                while (reader.Read())
                {
                    clientVersionDataPoints.Add(new Tuple<string, string>(((int)reader.GetValue(0)).ToString() + "." + ((int)reader.GetValue(1)).ToString(), ((int)reader.GetValue(2)).ToString()));
                }
            }

            JArray reportObject = ReportHelpers.GetJson(clientVersionDataPoints);
            ReportHelpers.CreateBlob(ReportStorage, "ClientVersionWeeklyReport.json", "dashboard", "application/json", ReportHelpers.ToStream(reportObject));
        }
    } 
}
