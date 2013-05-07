using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGetGallery.Operations
{
    /// <summary>
    /// Helper class to convert C# DateTime to UNIX time stamp.
    /// </summary>
    public class UnixTimeStampUtility
    {
        private static readonly DateTime UnixEpoch =
  new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long GetCurrentUnixTimestampMillis()
        {
            return (long)(DateTime.UtcNow - UnixEpoch).TotalMilliseconds;
        }

        public static DateTime DateTimeFromUnixTimestampMillis(long millis)
        {
            return UnixEpoch.AddMilliseconds(millis);
        }

        public static long GetCurrentUnixTimestampSeconds()
        {
            return (long)(DateTime.UtcNow - UnixEpoch).TotalSeconds;
        }

        public static long GetUnixTimestampSeconds(DateTime time)
        {
            return (long)(time - UnixEpoch).TotalSeconds;
        }

        public static long GetLastMonthUnixTimestampSeconds()
        {
            return (long)(DateTime.UtcNow.Subtract(new TimeSpan(30, 0, 0, 0)) - UnixEpoch).TotalSeconds;
        }

        public static long GetLastWeekUnixTimestampSeconds()
        {
            return (long)(DateTime.UtcNow.Subtract(new TimeSpan(7, 0, 0, 0)) - UnixEpoch).TotalSeconds;
        }

        public static double GetSecondsFor30Days()
        {
            double total = new TimeSpan(30, 0, 0, 0).TotalSeconds;
            return total;
        }

        public static DateTime DateTimeFromUnixTimestampSeconds(long seconds)
        {
            return UnixEpoch.AddSeconds(seconds);
        }
    }
}
