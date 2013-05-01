using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;
using AnglicanGeek.DbExecutor;
using AnglicanGeek.MarkdownMailer;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NuGetGallery.Operations.Common;

namespace NuGetGallery.Operations.Tasks
{
    [Command("findduplicatepackageversions", "Finds Duplicate Package Versions", AltName = "fdpv", IsSpecialPurpose = true)]
    public class FindDuplicatePackageVersionsTask : DatabaseAndStorageTask
    {
        // 0 - Id, 1 - Normal Version, 2 - List of Duplicates, 3 - Selected version
        private const string MailTemplate = @"

Hello,
 
We’re writing to let you know about duplicate data that we found in our database and how we plan to handle it.  We noticed that a package you published, {0}, has multiple entries in our database that would be treated by NuGet as version {1}. The following package versions were found to be duplicates:
 
{2}
                                
We want to make it clear that no data have been lost.  However, going forward, we’ll be keeping only one of these versions of the package.  The version we’ll be keeping is the one labelled _{0} {3}_.  If you don't get back to us by **May 31, 2013 at Noon Pacific Time (8:00AM UTC)**, we will delete the other version(s) from our system.

As part of this change, we’ll change the version number to a canonical format to make sure duplicate data scenarios like this can’t happen again. This shouldn’t affect anyone’s ability to install or update your package.
 
We keep backups of deleted data, so your package won't be gone forever.  If you need a copy of that backup, we can help you out. If you would like us to do something else, please get back to us by the deadline above.
 
If you have any concerns about the validity of this email, please contact us directly at nugetgallery@outercurve.org or use the ""Contact Support"" link on your package’s page to reach out to us.
 
We apologize for any inconvenience.

Sincerely,

The NuGet Gallery Team
";
        private const string PackageListLineFormat = "* {0} {1}";

        [Option("The SMTP host", AltName="host")]
        public string MailHost { get; set; }
        
        [Option("The SMTP user", AltName = "user")]
        public string MailUser { get; set; }

        [Option("The SMTP port", AltName = "port")]
        public int MailPort { get; set; }

        [Option("The SMTP password", AltName = "password")]
        public string MailPassword { get; set; }

        [Option("Address to BCC")]
        public string Bcc { get; set; }

        public override void ValidateArguments()
        {
            ArgCheck.Required(MailHost, "MailHost");
            ArgCheck.Required(MailUser, "MailUser");
            ArgCheck.Required(MailPassword, "MailPassword");
            base.ValidateArguments();
        }

        public override void ExecuteCommand()
        {
            // Set up email
            MailSender sender = new MailSender(new MailSenderConfiguration()
            {
                DeliveryMethod = SmtpDeliveryMethod.Network,
                Host = MailHost,
                Port = MailPort,
                EnableSsl = true,
                Credentials = new NetworkCredential(MailUser, MailPassword)
            });
            Log.Info("Connecting to {0}:{1} to send mail as {2}", MailHost, MailPort, MailUser);

            using (var sqlConnection = new SqlConnection(ConnectionString.ConnectionString))
            using (var dbExecutor = new SqlExecutor(sqlConnection))
            {
                sqlConnection.Open();

                // Query for packages
                Log.Info("Gathering list of packages...");
                var packages = dbExecutor.Query<PackageSummary>(@"
                    SELECT p.[Key], p.PackageRegistrationKey, r.Id, p.Version, p.Hash, p.LastUpdated, p.Published, p.Listed, p.IsLatestStable
                    FROM   Packages p
                    INNER JOIN PackageRegistrations r ON p.PackageRegistrationKey = r.[Key]");

                // Group by Id and and SemVer
                Log.Info("Grouping by Package ID and Actual Version...");
                var groups = packages.GroupBy(p => new { p.Id, Version = SemanticVersionExtensions.Normalize(p.Version) });

                // Find any groups with more than one entry
                Log.Info("Finding Duplicates...");
                var dups = groups.Where(g => g.Count() > 1);

                // Print them out
                int dupsUnlistedCount = 0;
                int latestCount = 0;
                foreach (var dup in dups)
                {
                    ProcessDuplicate(dup.Key.Id, dup.Key.Version, dup.ToList(), dbExecutor, sender, ref dupsUnlistedCount, ref latestCount);
                }
                var totalDupes = dups.Count();
                Log.Info("Found {0} Packages with duplicates.", totalDupes);
                Log.Info(" {0} of them have no listed duplicates.", dupsUnlistedCount);
                Log.Info(" {0} of them have multiple listed duplicates.", totalDupes - dupsUnlistedCount);
                if (latestCount > 0)
                {
                    Log.Warn(" {0} of them are the latest version of the relevant package", latestCount);
                }
                else
                {
                    Log.Info(" NONE of them are the latest version of the relevant package");
                }
            }
        }

        private void ProcessDuplicate(string id, string normalVersion, List<PackageSummary> packages, SqlExecutor dbExecutor, MailSender sender, ref int unlistedCount, ref int latestCount)
        {
            // Are any of these the latest version?
            var latest = packages.Where(p => p.Latest).ToList();
            if (latest.Count > 0)
            {
                latestCount++;
                Log.Error("Unable to process: {0}@{1}, it is the latest version of {0}", id, normalVersion);
            }
            else
            {
                // Is there only one listed version?
                var listed = packages.Where(p => p.Listed).ToList();
                if (listed.Count == 1)
                {
                    unlistedCount++;
                    ContactOwners(MailTemplate, id, normalVersion, listed.Single(), packages, dbExecutor, sender);
                }
                else
                {
                    // Select the most recent pacakge
                    var selected = packages.OrderByDescending(p => p.Published).FirstOrDefault();
                    if (selected == null)
                    {
                        Log.Error("Weird. There wasn't a most recent upload of {0}@{1}?", id, normalVersion);
                    }
                    else
                    {
                        ContactOwners(MailTemplate, id, normalVersion, selected, packages, dbExecutor, sender);
                    }
                }
            }
        }

        private void ContactOwners(string template, string id, string normalVersion, PackageSummary selected, List<PackageSummary> packages, SqlExecutor executor, MailSender sender)
        {
            // Collect owners
            var owners = executor.Query<PackageOwner>(@"
                SELECT u.Username, u.EmailAddress
                FROM   PackageRegistrationOwners p
                INNER JOIN Users u ON p.UserKey = u.[Key]
                WHERE  p.PackageRegistrationKey = @key", new { key = selected.PackageRegistrationKey });
            
            // Prepare the message
            // 0 - Id, 1 - Normal Version, 2 - List of Duplicates, 3 - Selected version
            var message = String.Format(
                template,
                id,
                normalVersion,
                String.Join(Environment.NewLine, packages.Select(p => String.Format(PackageListLineFormat, p.Id, p.Version))),
                selected.Version);
            MailMessage mail = new MailMessage();
            mail.From = new MailAddress("nugetgallery@outercurve.org", "NuGet Gallery");
            foreach (var owner in owners)
            {
                mail.To.Add(new MailAddress(owner.EmailAddress, owner.Username));
            }
            mail.Subject = String.Format("IMPORTANT: Duplicate Package Data for {0} {1}", id, normalVersion);
            mail.Priority = MailPriority.High;
            if (!String.IsNullOrEmpty(Bcc))
            {
                mail.Bcc.Add(new MailAddress(Bcc));
            }
            mail.Body = message;

            // Send the message!
            if (!WhatIf)
            {
                sender.Send(mail);
            }
            Log.Info("Sent mail for {0}@{1}", id, normalVersion);
        }
    }

    public class PackageOwner
    {
        public string Username { get; set; }
        public string EmailAddress { get; set; }
    }

    public class PackageSummary
    {
        public int Key { get; set; }
        public int PackageRegistrationKey { get; set; }
        public string Id { get; set; }
        public string Version { get; set; }
        public string Hash { get; set; }
        public bool Listed { get; set; }
        public bool Latest { get; set; }
        public DateTime LastUpdated { get; set; }
        public DateTime Published { get; set; }
    }
}
