package expo.modules.updates.procedures

import android.content.Context
import expo.modules.core.logging.localizedMessageWithCauseLocalizedMessage
import expo.modules.updates.IUpdatesController
import expo.modules.updates.UpdatesConfiguration
import expo.modules.updates.db.DatabaseHolder
import expo.modules.updates.db.entity.UpdateEntity
import expo.modules.updates.loader.FileDownloader
import expo.modules.updates.loader.LoaderTask
import expo.modules.updates.loader.UpdateDirective
import expo.modules.updates.loader.UpdateResponse
import expo.modules.updates.logging.UpdatesLogger
import expo.modules.updates.manifest.EmbeddedManifestUtils
import expo.modules.updates.selectionpolicy.SelectionPolicy
import expo.modules.updates.statemachine.UpdatesStateEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import org.json.JSONObject
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class CheckForUpdateProcedure(
  private val context: Context,
  private val updatesConfiguration: UpdatesConfiguration,
  private val databaseHolder: DatabaseHolder,
  private val updatesLogger: UpdatesLogger,
  private val fileDownloader: FileDownloader,
  private val selectionPolicy: SelectionPolicy,
  private val launchedUpdate: UpdateEntity?,
  private val procedureScope: CoroutineScope = CoroutineScope(Dispatchers.IO),
  private val callback: (IUpdatesController.CheckForUpdateResult) -> Unit
) : StateMachineProcedure() {
  override val loggerTimerLabel = "timer-check-for-update"

  override suspend fun run(procedureContext: ProcedureContext) {
    procedureContext.processStateEvent(UpdatesStateEvent.Check())

    val embeddedUpdate = EmbeddedManifestUtils.getEmbeddedUpdate(context, updatesConfiguration)?.updateEntity
    val extraHeaders = FileDownloader.getExtraHeadersForRemoteUpdateRequest(
      databaseHolder.database,
      updatesConfiguration,
      launchedUpdate,
      embeddedUpdate
    )
    try {
      val updateResponse = downloadRemoteUpdate(extraHeaders)
      handleUpdateResponse(updateResponse, embeddedUpdate, procedureContext)
    } catch (e: Exception) {
      procedureContext.processStateEvent(UpdatesStateEvent.CheckError(e.localizedMessageWithCauseLocalizedMessage()))
      callback(IUpdatesController.CheckForUpdateResult.ErrorResult(e))
      procedureContext.onComplete()
    } finally {
      databaseHolder.releaseDatabase()
    }
  }

  private suspend fun downloadRemoteUpdate(extraHeaders: JSONObject): UpdateResponse =
    suspendCancellableCoroutine { continuation ->
      fileDownloader.downloadRemoteUpdate(
        extraHeaders,
        object : FileDownloader.RemoteUpdateDownloadCallback {
          override fun onFailure(e: Exception) {
            if (continuation.isActive) {
              continuation.resumeWithException(e)
            }
          }

          override fun onSuccess(updateResponse: UpdateResponse) {
            if (continuation.isActive) {
              continuation.resume(updateResponse)
            }
          }
        }
      )
    }

  private fun handleUpdateResponse(updateResponse: UpdateResponse, embeddedUpdate: UpdateEntity?, procedureContext: ProcedureContext) {
    val updateDirective = updateResponse.directiveUpdateResponsePart?.updateDirective
    val update = updateResponse.manifestUpdateResponsePart?.update

    fun handleNoUpdateAvailable(reason: LoaderTask.RemoteCheckResultNotAvailableReason) {
      procedureContext.processStateEvent(UpdatesStateEvent.CheckCompleteUnavailable())
      callback(IUpdatesController.CheckForUpdateResult.NoUpdateAvailable(reason))
      procedureContext.onComplete()
    }

    when {
      updateDirective is UpdateDirective.NoUpdateAvailableUpdateDirective -> {
        handleNoUpdateAvailable(LoaderTask.RemoteCheckResultNotAvailableReason.NO_UPDATE_AVAILABLE_ON_SERVER)
      }

      updateDirective is UpdateDirective.RollBackToEmbeddedUpdateDirective -> {
        when {
          !updatesConfiguration.hasEmbeddedUpdate || embeddedUpdate == null -> {
            handleNoUpdateAvailable(LoaderTask.RemoteCheckResultNotAvailableReason.ROLLBACK_NO_EMBEDDED)
          }

          !selectionPolicy.shouldLoadRollBackToEmbeddedDirective(
            updateDirective,
            embeddedUpdate,
            launchedUpdate,
            updateResponse.responseHeaderData?.manifestFilters
          ) -> {
            handleNoUpdateAvailable(LoaderTask.RemoteCheckResultNotAvailableReason.ROLLBACK_REJECTED_BY_SELECTION_POLICY)
          }

          else -> {
            procedureContext.processStateEvent(UpdatesStateEvent.CheckCompleteWithRollback(updateDirective.commitTime))
            callback(IUpdatesController.CheckForUpdateResult.RollBackToEmbedded(updateDirective.commitTime))
            procedureContext.onComplete()
          }
        }
      }

      update == null -> {
        handleNoUpdateAvailable(LoaderTask.RemoteCheckResultNotAvailableReason.NO_UPDATE_AVAILABLE_ON_SERVER)
      }

      else -> {
        if (launchedUpdate == null) {
          procedureContext.processStateEvent(UpdatesStateEvent.CheckCompleteWithUpdate(update.manifest.getRawJson()))
          callback(IUpdatesController.CheckForUpdateResult.UpdateAvailable(update))
          procedureContext.onComplete()
          return
        }

        var failedPreviously = false
        var shouldLaunch: Boolean
        procedureScope.launch {
          shouldLaunch = update.updateEntity?.let {
            selectionPolicy.shouldLoadNewUpdate(
              it,
              launchedUpdate,
              updateResponse.responseHeaderData?.manifestFilters
            ).also { shouldLoad ->
              if (shouldLoad) {
                val storedUpdateEntity = withContext(Dispatchers.IO) {
                  databaseHolder.database.updateDao().loadUpdateWithId(it.id)
                }
                storedUpdateEntity?.let { storedUpdate ->
                  failedPreviously = storedUpdate.failedLaunchCount != 0
                  if (failedPreviously) {
                    updatesLogger.info(
                      "Stored update found: ID = ${it.id}, failureCount = ${storedUpdate.failedLaunchCount}"
                    )
                  }
                }
              }
            }
          } ?: false

          if (shouldLaunch) {
            procedureContext.processStateEvent(UpdatesStateEvent.CheckCompleteWithUpdate(update.manifest.getRawJson()))
            callback(IUpdatesController.CheckForUpdateResult.UpdateAvailable(update))
            procedureContext.onComplete()
          } else {
            val reason = if (failedPreviously) {
              LoaderTask.RemoteCheckResultNotAvailableReason.UPDATE_PREVIOUSLY_FAILED
            } else {
              LoaderTask.RemoteCheckResultNotAvailableReason.UPDATE_REJECTED_BY_SELECTION_POLICY
            }
            handleNoUpdateAvailable(reason)
          }
        }
        return
      }
    }
  }
}
