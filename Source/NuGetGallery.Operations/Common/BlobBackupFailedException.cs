using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGetGallery.Operations.Common
{
    [Serializable]
    public class BlobBackupFailedException : Exception
    {
        public BlobBackupFailedException(string message)
            : base(message)
        {
        }
    }
}
