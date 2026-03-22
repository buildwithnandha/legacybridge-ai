import { LayoutGrid, ShieldAlert, Wrench, Timer, Activity } from 'lucide-react'

export default function LiveStatsBar({ agentSteps, tableStatuses, startTime }) {
  const tablesScanned = new Set(Object.keys(tableStatuses)).size
  const issueCount = agentSteps.filter(s => s.type === 'tool_result' && s.result?.root_cause).length
  const toolCalls = agentSteps.filter(s => s.type === 'tool_call').length
  const elapsed = startTime ? Math.floor((Date.now() - startTime) / 1000) : 0
  const mins = String(Math.floor(elapsed / 60)).padStart(2, '0')
  const secs = String(elapsed % 60).padStart(2, '0')

  return (
    <div className="h-8 flex items-center gap-0 px-5"
      style={{ background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-primary)' }}>
      <Stat icon={<LayoutGrid size={11} />} label="Tables" value={`${tablesScanned}/5`} />
      <Divider />
      <Stat icon={<ShieldAlert size={11} />} label="Issues" value={String(issueCount)} />
      <Divider />
      <Stat icon={<Wrench size={11} />} label="Tool Calls" value={String(toolCalls)} />
      <Divider />
      <Stat icon={<Timer size={11} />} label="" value={`${mins}:${secs}`} />
      <Divider />
      <span className="flex items-center gap-1.5 font-mono text-[11px]" style={{ color: 'var(--accent-cyan)' }}>
        <Activity size={11} />
        Agent Active
      </span>
    </div>
  )
}

function Stat({ icon, label, value }) {
  return (
    <span className="flex items-center gap-1.5 font-mono text-[11px]" style={{ color: 'var(--text-secondary)' }}>
      <span style={{ color: 'var(--text-muted)' }}>{icon}</span>
      {label && <span>{label}:</span>}
      <span style={{ color: 'var(--text-primary)' }}>{value}</span>
    </span>
  )
}

function Divider() {
  return <span className="mx-3 text-[11px]" style={{ color: 'var(--border-primary)' }}>|</span>
}
