using System;
using System.Collections.Generic;
using System.Net.Http;
using Microsoft.WindowsAzure.Storage;
using NuGet;
using Newtonsoft.Json.Linq;
using System.IO;

namespace NuGetGallery.Operations.Worker.Jobs
{
    class PackageEdit
    {
        internal static PackageEdit Parse(string messageStr)
        {
            PackageEdit ret = new PackageEdit();
            JToken token = JObject.Parse(messageStr);

            ret.PackageId = (string)token.SelectToken("PackageId");
            ret.Version = (string)token.SelectToken("Version");
            ret.EditId = (string)token.SelectToken("EditId");
            ret.CallbackAddress = (string)token.SelectToken("CallbackAddress");
            ret.SecurityToken = (string)token.SelectToken("SecurityToken");
            ret.Title = (string)token.SelectToken("EditPackageVersionRequest.VersionTitle");
            ret.IconUrl = (string)token.SelectToken("EditPackageVersionRequest.IconUrl");
            ret.Summary = (string)token.SelectToken("EditPackageVersionRequest.Summary");
            ret.Description = (string)token.SelectToken("EditPackageVersionRequest.Description");
            ret.ProjectUrl = (string)token.SelectToken("EditPackageVersionRequest.ProjectUrl");
            ret.Authors = (string)token.SelectToken("EditPackageVersionRequest.Authors");
            ret.Copyright = (string)token.SelectToken("EditPackageVersionRequest.Copyright");
            ret.Tags = (string)token.SelectToken("EditPackageVersionRequest.Tags");
            ret.ReleaseNotes = (string)token.SelectToken("EditPackageVersionRequest.ReleaseNotes");

            return ret;
        }

        // meta
        public string PackageId { get; set; }
        public string Version { get; set; }
        public string EditId { get; set; }
        public string CallbackAddress { get; set; }
        public string SecurityToken { get; set; }

        // edits
        public string Title { get; set; }
        public string IconUrl { get; set; }
        public string Summary { get; set; }
        public string Description { get; set; }
        public string ProjectUrl { get; set; }
        public string Authors { get; set; }
        public string Copyright { get; set; }
        public string Tags { get; set; }
        public string ReleaseNotes { get; set; }

        // output
        public string BlobUrl { get; set; }
    }

    class QueuedPackageEditsJob : WorkerJob
    {
        public override TimeSpan Period
        {
            get
            {
                return TimeSpan.FromMinutes(1);
            }
        }

        public override void RunOnce()
        {
            CloudStorageAccount storageAccount = Settings.MainStorage;

            var queueClient = storageAccount.CreateCloudQueueClient();
            queueClient.ServerTimeout = TimeSpan.FromSeconds(5);

            var editsQueue = queueClient.GetQueueReference("EditPackage");
            if (!editsQueue.Exists())
            {
                return;
            }

            var message = editsQueue.GetMessage();
            if (message == null)
            {
                return;
            }

            PackageEdit edit = PackageEdit.Parse(message.AsString);

            // Note, any azure queue message will be delivered AT LEAST ONCE.

            // Work to do:
            // 1) Generate new NUPKG, if it doesn't exist
            // 2) Call Gallery to say 'new NUPKG is ready sir, you can finish the edit (if you so desire)!'
            // If either failed, we won't complete (delete) the messsage from the qeueue
            GenerateNewNupkg(edit);
            if (NotifyGallery(edit))
            {
                editsQueue.DeleteMessage(message);
            }
        }

        private void GenerateNewNupkg(PackageEdit edit)
        {
            List<Action<ManifestMetadata>> edits = new List<Action<ManifestMetadata>>
            { 
                (m) => { m.Title = edit.Title; },
                (m) => { m.IconUrl = edit.IconUrl; },
                (m) => { m.Summary = edit.Summary; },
                (m) => { m.Description = edit.Description; },
                (m) => { m.ProjectUrl = edit.ProjectUrl; },
                (m) => { m.Authors = edit.Authors; },
                (m) => { m.Copyright = edit.Copyright; },
                (m) => { m.Tags = edit.Tags; },
                (m) => { m.ReleaseNotes = edit.ReleaseNotes; },
            };

            CloudStorageAccount storageAccount = Settings.MainStorage;
            var blobClient = storageAccount.CreateCloudBlobClient();
            var packagesContainer = Util.GetPackagesBlobContainer(blobClient);
            var originalPackageFileName = Util.GetPackageFileName(edit.PackageId, edit.Version);
            var blob = packagesContainer.GetBlockBlobReference(originalPackageFileName);

            using (var readWriteStream = new MemoryStream())
            {
                blob.DownloadToStream(readWriteStream);
                NupkgRewriter.RewriteNupkgManifest(readWriteStream, edits);

                var editedPackageFileName = Util.GetEditedPackageFileName(edit.PackageId, edit.Version, edit.EditId);
                var editedBlob = packagesContainer.GetBlockBlobReference(editedPackageFileName);
                
                // set up blob url on edit, which will be posted to gallery once we are done
                edit.BlobUrl = editedBlob.Uri;

                readWriteStream.Position = 0;
                editedBlob.UploadFromStream(readWriteStream);
            }
        }

        private bool NotifyGallery(PackageEdit edit)
        {
            Uri callbackAddress = new Uri(edit.CallbackAddress());
            var client = new HttpClient(callbackAddress);
            var json = new JObject(new
            {
                PackageId = edit.PackageId,
                Version = edit.Version,
                EditId = edit.EditId,
                BlobUrl = edit.BlobUrl,
                SecurityToken = edit.SecurityToken,
            });
            HttpContent content = new StringContent(json.ToString());
            try
            {
                HttpResponseMessage response = client.PutAsync(callbackAddress, content).Result;
                return response.IsSuccessStatusCode;
            }
            catch (Exception e)
            {
                return false;
            }
        }
    }
}
