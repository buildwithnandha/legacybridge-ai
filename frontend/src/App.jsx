import { useState, useCallback, useEffect, useRef } from 'react'
import Header from './components/Header'
import LiveStatsBar from './components/LiveStatsBar'
import AgentPanel from './components/agent/AgentPanel'
import ReportPanel from './components/report/ReportPanel'
import StartupScreen from './components/StartupScreen'
import CompletionModal from './components/CompletionModal'
import Toast from './components/Toast'
import { triggerReconRun, getReport, downloadPdfWithRetry } from './services/api'
import { connectAgentStream } from './services/eventStream'

export default function App() {
  const [startupDone, setStartupDone] = useState(false)
  const [runId, setRunId] = useState(null)
  const [isRunning, setIsRunning] = useState(false)
  const [agentSteps, setAgentSteps] = useState([])
  const [rootCauses, setRootCauses] = useState([])
  const [report, setReport] = useState(null)
  const [summary, setSummary] = useState(null)
  const [tableStatuses, setTableStatuses] = useState({})
  const [demoMode, setDemoMode] = useState(false)
  const [error, setError] = useState(null)
  const [showModal, setShowModal] = useState(false)
  const [toasts, setToasts] = useState([])
  const [redFlash, setRedFlash] = useState(false)
  const [startTime, setStartTime] = useState(null)
  const timerRef = useRef(null)

  // Progressive report data built from SSE tool_result events
  const [streamedTables, setStreamedTables] = useState({})
  const [tablesInvestigated, setTablesInvestigated] = useState(new Set())

  // Force re-render for live timer
  useEffect(() => {
    if (isRunning) {
      timerRef.current = setInterval(() => setStartTime(s => s), 200)
      return () => clearInterval(timerRef.current)
    }
    if (timerRef.current) clearInterval(timerRef.current)
  }, [isRunning])

  const addToast = useCallback((message, table) => {
    const id = Date.now()
    setToasts(prev => [...prev, { id, message, table }])
  }, [])

  const dismissToast = useCallback((id) => {
    setToasts(prev => prev.filter(t => t.id !== id))
  }, [])

  const triggerRedFlash = useCallback(() => {
    setRedFlash(true)
    setTimeout(() => setRedFlash(false), 400)
  }, [])

  const handleRunRecon = useCallback(async () => {
    setError(null)
    setAgentSteps([])
    setRootCauses([])
    setReport(null)
    setSummary(null)
    setTableStatuses({})
    setStreamedTables({})
    setTablesInvestigated(new Set())
    setIsRunning(true)
    setShowModal(false)
    setStartTime(Date.now())

    try {
      const result = await triggerReconRun()
      setRunId(result.run_id)
      setDemoMode(!!result.demo_mode)

      connectAgentStream(result.run_id, {
        onStart: (data) => {
          setAgentSteps(prev => [
            ...prev,
            { type: 'start', content: `Investigating ${data.tables.length} tables...`, ts: Date.now() },
          ])
        },
        onThinking: (data) => {
          setAgentSteps(prev => [
            ...prev,
            { type: 'thinking', content: data.content, turn: data.turn, ts: Date.now() },
          ])
        },
        onToolCall: (data) => {
          setAgentSteps(prev => [
            ...prev,
            { type: 'tool_call', tool: data.tool, input: data.input, turn: data.turn, ts: Date.now() },
          ])
          // Track which table is being investigated from tool call input
          const tableName = data.input?.table_name
          if (tableName) {
            setTablesInvestigated(prev => new Set([...prev, tableName]))
          }
        },
        onToolResult: (data) => {
          setAgentSteps(prev => [
            ...prev,
            { type: 'tool_result', tool: data.tool, result: data.result, turn: data.turn, ts: Date.now() },
          ])

          // Update table statuses
          if (data.result?.severity) {
            setTableStatuses(prev => ({ ...prev, [data.result.table]: data.result.severity }))
          }
          if (data.result?.status && data.result?.table) {
            setTableStatuses(prev => ({ ...prev, [data.result.table]: data.result.status }))
          }

          // Progressively build report data from tool results
          const r = data.result
          const tn = r?.table
          if (tn && data.tool === 'get_schema_diff') {
            setStreamedTables(prev => ({
              ...prev,
              [tn]: { ...prev[tn], schema_diff: r, status: r.severity === 'HEALTHY' ? (prev[tn]?.status || 'HEALTHY') : r.severity }
            }))
          }
          if (tn && data.tool === 'get_row_recon') {
            setStreamedTables(prev => ({
              ...prev,
              [tn]: { ...prev[tn], row_recon: r, status: r.status === 'MISMATCH' ? 'CRITICAL' : (prev[tn]?.status || 'HEALTHY') }
            }))
          }
          if (tn && data.tool === 'get_cdc_events') {
            setStreamedTables(prev => ({
              ...prev,
              [tn]: { ...prev[tn], cdc_analysis: r, status: r.missed > 0 ? 'CRITICAL' : (prev[tn]?.status || 'HEALTHY') }
            }))
          }
        },
        onRootCause: (data) => {
          setRootCauses(prev => [...prev, data])
          if (data.table) {
            const status = data.root_cause === 'HEALTHY' ? 'HEALTHY'
              : data.priority === 'P1' ? 'CRITICAL' : 'WARNING'
            setTableStatuses(prev => ({ ...prev, [data.table]: status }))
          }
          if (data.priority === 'P1') {
            triggerRedFlash()
            addToast(data.root_cause, data.table)
          }
        },
        onComplete: async (data) => {
          setSummary(data)
          setIsRunning(false)
          // Fetch full report to fill in any missing data
          try {
            const fullReport = await getReport(result.run_id)
            setReport(fullReport)
          } catch (_e) {
            // report may not be ready yet
          }
          // Show modal AFTER health score animation
          setTimeout(() => setShowModal(true), 3000)
        },
        onError: () => {
          setError('Agent stream disconnected.')
          setIsRunning(false)
        },
      })
    } catch (err) {
      setError(err.message || 'Failed to start reconciliation')
      setIsRunning(false)
    }
  }, [addToast, triggerRedFlash])

  const handleExportPdf = useCallback(async () => {
    if (runId) await downloadPdfWithRetry(runId)
  }, [runId])

  // Build a progressive report from streamed data when full report isn't available yet
  const progressiveReport = Object.keys(streamedTables).length > 0 ? {
    recon: { tables: streamedTables }
  } : null

  // Use full report if available, otherwise progressive
  const displayReport = report || progressiveReport

  if (!startupDone) {
    return <StartupScreen onComplete={() => setStartupDone(true)} />
  }

  return (
    <div className="h-screen flex flex-col" style={{ background: 'var(--bg-primary)' }}>
      {redFlash && <div className="fixed inset-0 z-40 pointer-events-none red-flash" />}

      {toasts.map(t => (
        <Toast key={t.id} message={t.message} table={t.table} onDismiss={() => dismissToast(t.id)} />
      ))}

      {showModal && (
        <CompletionModal summary={summary} onExportPdf={handleExportPdf} onDismiss={() => setShowModal(false)} />
      )}

      <Header
        onRunRecon={handleRunRecon}
        onExportPdf={handleExportPdf}
        isRunning={isRunning}
        canExport={!!summary}
        demoMode={demoMode}
      />

      {isRunning && (
        <LiveStatsBar agentSteps={agentSteps} tableStatuses={tableStatuses} startTime={startTime} />
      )}

      {error && (
        <div className="px-4 py-2 text-xs"
          style={{ background: 'var(--critical-bg)', borderBottom: '1px solid rgba(255,68,68,0.3)', color: 'var(--critical)' }}>
          {error}
        </div>
      )}

      <div className="flex-1 flex overflow-hidden">
        <div className="flex flex-col" style={{ width: '45%', borderRight: '1px solid var(--border-primary)' }}>
          <AgentPanel steps={agentSteps} isRunning={isRunning} tableStatuses={tableStatuses} />
        </div>

        <div className="flex flex-col" style={{ width: '55%' }}>
          <ReportPanel
            report={displayReport}
            rootCauses={rootCauses}
            summary={summary}
            runId={runId}
            tablesInvestigated={tablesInvestigated.size}
          />
        </div>
      </div>
    </div>
  )
}
