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
using System.Net;
using System.Web.Script.Serialization;

namespace NuGetGallery.Operations
{
    [Command("createpingdommontlyreport", "Creates report for the monthly average pingdom values", AltName = "cpdmr")]
    public class CreatePingdomMonthlyReportTask : StorageTask
    {
        [Option("PingdomUserName", AltName = "user")]
        public string UserName { get; set; }

        [Option("PingdomUserpassword", AltName = "password")]
        public string Password { get; set; }

        public override void ExecuteCommand()
        {
            NetworkCredential nc = new NetworkCredential(UserName, Password);
            WebRequest request = WebRequest.Create("https://api.pingdom.com/api/2.0/checks");
            request.Credentials = nc;
            request.PreAuthenticate = true;
            request.Method = "GET";
            WebResponse respose = request.GetResponse();
            using (var reader = new StreamReader(respose.GetResponseStream()))
            {
                JavaScriptSerializer js = new JavaScriptSerializer();
                var objects = js.Deserialize<dynamic>(reader.ReadToEnd());
                foreach (var o in objects["checks"])
                {
                    List<Tuple<string, string>> summary = GetCheckSummaryAvgForLastMonth(o["id"]);
                    JArray reportObject = ReportHelpers.GetJson(summary);
                    ReportHelpers.CreateBlob(StorageAccount, o["id"] + "MonthlyReport.json", "dashboard", "application/json", ReportHelpers.ToStream(reportObject));
                }
            }
        } 

        private List<Tuple<string,string>> GetCheckSummaryAvgForLastMonth(int checkId)
        {
            long currentTime = UnixTimeStampUtility.GetCurrentUnixTimestampSeconds();
            long lastMonth = UnixTimeStampUtility.GetLastMonthUnixTimestampSeconds();            
            NetworkCredential nc = new NetworkCredential(UserName,Password);
            WebRequest request = WebRequest.Create(string.Format("https://api.pingdom.com/api/2.0/summary.average/{0}?includeuptime=true&from={1}&to={2}",checkId, lastMonth, currentTime));
            request.Credentials = nc;
            request.PreAuthenticate = true;      
            request.Method = "GET";
            List<Tuple<string,string>> summaryValues = new List<Tuple<string,string>>();

            WebResponse respose = request.GetResponse();
            using (var reader = new StreamReader(respose.GetResponseStream()))
            {
                JavaScriptSerializer js = new JavaScriptSerializer();
                var summaryObject = js.Deserialize<dynamic>(reader.ReadToEnd());
                foreach (var summary in summaryObject["summary"])
                {
                    foreach (var status in summary.Value)
                    {
                        summaryValues.Add(new Tuple<string,string>(status.Key, status.Value));                       
                    }                  
                }
            }
            return summaryValues;            
        }        
    }
}