package com.twitter.finagle.http

import com.twitter.finagle.http.HttpTracing.stripParameters
import com.twitter.finagle.tracing.Trace

/**
 * [[datadog.trace.finagle.ServerTraceInitializer]] makes use of [[TraceInfoCompat.traceRpc]], originally of [[TraceInfo]], and it's unclear what the replacement would be. Note that [[TraceInfo]] is now `private`.
 *
 * @todo Investigate a more graceful migration.
 *
 * @see [[datadog.trace.finagle.ServerTraceInitializer]]
 * @see [[https://github.com/twitter/finagle/commit/78d93fde6cdf18534d6990ec7c8d4e97260e5c57#diff-ffd6be2f027273f0afc98c006963391d0bc9f66393a8c5f749dd6053e9f4d1bbL142-L147]]
 *
 * @since June 23, 2021
 * @author <a href="mailto:michael@ahlers.consulting">Michael Ahlers</a>
 */
object TraceInfoCompat {

  def traceRpc(request: Request): Unit = {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      trace.recordRpc(request.method.toString)
      trace.recordBinary("http.uri", stripParameters(request.uri))
    }
  }

}
