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
    [Command("createtrendingpackagesreport", "Creates the over-all package upload trending report for Gallery", AltName = "ctprep")]
    public class CreateTrendingPackagesReportTask : ReportsTask
    {       
        private string SqlQuery = @" SELECT SUM(f.DownloadCount) FROM [dbo].[Fact_Download] f JOIN [dbo].[Dimension_Date] dd ON dd.[Id] = f.Dimension_Date_Id WHERE dd.[Date] >= '{0}' AND dd.[Date] < '{1}' AND f.[Dimension_Package_Id] = '{2}' ";        

        public override void ExecuteCommand()
        {
            List<Tuple<string, string,string,double>> DownloadDataPoints = new List<Tuple<string, string,string,double>>();
            List<string> top100Packages = GetTop100PackageIds().ToList();
            DateTime day15 = DateTime.Now.Subtract(new TimeSpan(15, 0, 0, 0));
            DateTime day8 = day15.AddDays(7);
            foreach (string packageId in top100Packages)
            {

                using (var sqlConnection = new SqlConnection(ConnectionString.ConnectionString))
                {
                    using (var dbExecutor = new SqlExecutor(sqlConnection))
                    {
                        sqlConnection.Open();
                        var packageRegistrationKey =dbExecutor.Query<Int32>(string.Format("select Id FROM [dbo].[Dimension_Package] where [PackageId]= '{0}'",packageId)).FirstOrDefault();
                        //Execute the query and add the result to the list.                       
                        double lastWeekDownloadCount = 0, thisWeekDownloadCount = 0;
                        try
                        {
                            lastWeekDownloadCount = dbExecutor.Query<Int32>(string.Format(SqlQuery, day15.ToString("yyyy-MM-dd"), day8.ToString("yyyy-MM-dd"), packageRegistrationKey)).FirstOrDefault();
                        }
                        catch (NullReferenceException) { }
                        try
                        {
                            thisWeekDownloadCount = dbExecutor.Query<Int32>(string.Format(SqlQuery, day8.ToString("yyyy-MM-dd"), DateTime.Now.ToString("yyyy-MM-dd"), packageRegistrationKey)).FirstOrDefault();
                        }
                        catch (NullReferenceException) { }
                        double trendratio = thisWeekDownloadCount / lastWeekDownloadCount;
                        DownloadDataPoints.Add(new Tuple<string, string,string,double>(packageId ,lastWeekDownloadCount.ToString(), thisWeekDownloadCount.ToString(), trendratio));                    
                    }
                }
            }
            CreateBlobAndUploadForTopTrendingPackages(DownloadDataPoints);
        }

        private void CreateBlobAndUploadForTopTrendingPackages(List<Tuple<string,string,string,double>> rawData)
        {
            List<Tuple<string, string>> blobReportForLastWeekDownloads = new List<Tuple<string, string>>();
            List<Tuple<string, string>> blobReportForThisWeekDownloads = new List<Tuple<string, string>>();
            rawData.Sort((x, y) => y.Item4.CompareTo(x.Item4));
            rawData =  rawData.GetRange(0, 10);
            foreach (Tuple<string, string, string, double> record in rawData)
            {
                blobReportForLastWeekDownloads.Add(new Tuple<string,string>(record.Item1, record.Item2));
                blobReportForThisWeekDownloads.Add(new Tuple<string,string>(record.Item1,record.Item3));
            }

            //Get JSON object and upload it.
            JArray reportObjectLastWeek = ReportHelpers.GetJson(blobReportForLastWeekDownloads);
            ReportHelpers.CreateBlob(ReportStorage, "TrendingPackageLastWeek" + ".json", "dashboard", "application/json", ReportHelpers.ToStream(reportObjectLastWeek));
            JArray reportObjectThisWeek = ReportHelpers.GetJson(blobReportForThisWeekDownloads);
            ReportHelpers.CreateBlob(ReportStorage, "TrendingPackageThisWeek" + ".json", "dashboard", "application/json", ReportHelpers.ToStream(reportObjectThisWeek));
        }

        private IEnumerable<string> GetTop100PackageIds()
        {
             
            WebRequest request = WebRequest.Create("http://www.nuget.org/api/v2/stats/downloads/last6weeks?count=50");          
            request.Method = "GET";
            List<string> packages = new List<string>();
            WebResponse respose = request.GetResponse();
            using (var reader = new StreamReader(respose.GetResponseStream()))
            {
                JavaScriptSerializer js = new JavaScriptSerializer();
                var summaryObject = js.Deserialize<dynamic>(reader.ReadToEnd());
                foreach (var summary in summaryObject)
                {                   
                        packages.Add(summary["PackageId"]);
                   
                }
            }
            return packages;
          
        }
    }
}
