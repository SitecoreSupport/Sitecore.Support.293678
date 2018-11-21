using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Sitecore.Data;
using Sitecore.Data.DataProviders;
using Sitecore.Framework.Publishing.Manifest;
using Sitecore.Jobs;
using Sitecore.Publishing.Service.Events;
using Sitecore.Publishing.Service.Pipelines.BulkPublishingEnd;
using Sitecore.Publishing.Service.Security;
using Sitecore.Publishing.Service.SitecoreAbstractions;

namespace Sitecore.Support.Publishing.Service.Pipelines.BulkPublishingEnd
{
  public class RaiseRemoteEvents : Sitecore.Publishing.Service.Pipelines.BulkPublishingEnd.RaiseRemoteEvents
  {
    public RaiseRemoteEvents(string remoteEventCacheClearingThreshold, ITargetCacheClearHistory targetCacheClearHistory)
      : base(remoteEventCacheClearingThreshold, targetCacheClearHistory)
    {
    }

    public RaiseRemoteEvents(IDatabaseFactory sitecoreFactory, ILanguageManager langManager, ISitecoreSettings settings, IUserRoleService userRoleService, IPublishingLog logger, IRemoteItemEventFactory remoteItemEventFactory, ITargetCacheClearHistory targetCacheClearHistory, int remoteEventCacheClearingThreshold)
      : base(sitecoreFactory, langManager, settings, userRoleService, logger, remoteItemEventFactory, targetCacheClearHistory, remoteEventCacheClearingThreshold)
    {
    }

    public new void Process(PublishEndResultBatchArgs args)
    {
      if (!_cacheClearHistory.Exists(args.TargetInfo.ManifestId, args.TargetInfo.TargetId))
      {
        int eventThreshold = CalculateRemoteEventCacheClearingThreshold(args.JobData);
        Task.WaitAll(this.RaiseRemoteEventsOnTarget(args, args.JobData.LanguageNames.FirstOrDefault(), eventThreshold));
      }
    }

    protected override async Task RaiseRemoteEventsOnTarget(PublishEndResultBatchArgs publishEndResultBatchArgs, string languageName, int eventThreshold)
    {
      if (publishEndResultBatchArgs.TotalResultCount > eventThreshold || eventThreshold == 0)
      {
        await (Task)typeof(Sitecore.Publishing.Service.Pipelines.BulkPublishingEnd.RaiseRemoteEvents)
          .GetMethod("RaiseRemoteCacheClearingEventsOnTarget", BindingFlags.Instance | BindingFlags.NonPublic)
          .Invoke(this, new object[] { publishEndResultBatchArgs.TargetInfo });

        _cacheClearHistory.Add(publishEndResultBatchArgs.TargetInfo.ManifestId, publishEndResultBatchArgs.TargetInfo.TargetId);
      }
      else
      {
        typeof(Sitecore.Publishing.Service.Pipelines.BulkPublishingEnd.RaiseRemoteEvents)
          .GetMethod("RaiseRemoteItemEventsOnTarget", BindingFlags.NonPublic | BindingFlags.Instance)
          .Invoke(this, new object[] { publishEndResultBatchArgs, languageName });
      }

      // Make sure that not more then 1 rebuild is queued.
      var jobName = $"{publishEndResultBatchArgs.TargetInfo.TargetDatabaseName} rebuild descendants";
      var currentRebuildJobs = JobManager.GetJobs().Where(job => job.Name == jobName);
      if (currentRebuildJobs == null || currentRebuildJobs.Count(job => job.Status.State == JobState.Queued) == 0)
      {
        // Rebuild descendants on target 
        var targetDataProvider = _factory.GetDatabase(publishEndResultBatchArgs.TargetInfo.TargetDatabaseName)
          .Database.GetDataProviders()
          .FirstOrDefault(x => x as Data.DataProviders.Sql.SqlDataProvider != null);

        JobOptions options = new JobOptions(jobName, "Sitecore.Support.293678", "publisher", targetDataProvider, "RebuildDescendants");
        JobManager.Start(options);
      }
    }
  }
}