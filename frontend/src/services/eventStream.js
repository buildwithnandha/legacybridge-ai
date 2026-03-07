const API_BASE = import.meta.env.VITE_API_URL || ''

/**
 * Connect to the SSE stream for a recon run's RCA agent.
 * Returns a cleanup function to close the connection.
 *
 * @param {string} runId
 * @param {object} handlers - { onThinking, onToolCall, onToolResult, onRootCause, onComplete, onError }
 */
export function connectAgentStream(runId, handlers) {
  const url = `${API_BASE}/api/recon/${runId}/stream`
  const source = new EventSource(url)

  source.addEventListener('agent_start', (e) => {
    const data = JSON.parse(e.data)
    handlers.onStart?.(data)
  })

  source.addEventListener('thinking', (e) => {
    const data = JSON.parse(e.data)
    handlers.onThinking?.(data)
  })

  source.addEventListener('tool_call', (e) => {
    const data = JSON.parse(e.data)
    handlers.onToolCall?.(data)
  })

  source.addEventListener('tool_result', (e) => {
    const data = JSON.parse(e.data)
    handlers.onToolResult?.(data)
  })

  source.addEventListener('root_cause', (e) => {
    const data = JSON.parse(e.data)
    handlers.onRootCause?.(data)
  })

  source.addEventListener('agent_complete', (e) => {
    const data = JSON.parse(e.data)
    handlers.onComplete?.(data)
    source.close()
  })

  source.onerror = (err) => {
    handlers.onError?.(err)
    source.close()
  }

  return () => source.close()
}
