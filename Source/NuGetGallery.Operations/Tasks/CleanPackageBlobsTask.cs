using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGetGallery.Operations.Tasks
{
    public class CleanPackageBlobsTask : StorageTask
    {
        public override void ExecuteCommand()
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var packagesContainer = client.GetContainerReference("packages");
            var backupsContainer = client.GetContainerReference("packagebackups");
            var deletesContainer = client.GetContainerReference("deletes");
            deletesContainer.CreateIfNotExists();

            // Get all the packages that aren't normalized
            var blobs = Util.CollectBlobs(
                Log,
                packagesContainer,
                String.Empty,

                // ** THE CONDITION WHICH DEFINES BROKEN BLOBS ** //
                condition: b => (b.Name.Length > 0 && b.Name[0] == '/') || !String.Equals(b.Name, b.Name.ToLowerInvariant(), StringComparison.Ordinal),
                // ** THE CONDITION WHICH DEFINES BROKEN BLOBS ** //

                countEstimate: 140000);
        }
    }
}
