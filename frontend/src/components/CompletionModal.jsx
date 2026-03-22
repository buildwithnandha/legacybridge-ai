import { CheckCircle, Timer, ShieldAlert, AlertTriangle, Brain, FileDown, X, Activity } from 'lucide-react'

export default function CompletionModal({ summary, onExportPdf, onDismiss }) {
  if (!summary) return null

  const healthColor = summary.health_score >= 70 ? 'var(--healthy)'
    : summary.health_score >= 40 ? 'var(--warning)' : 'var(--critical)'

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop fade-in" onClick={onDismiss}>
      <div className="w-full max-w-md rounded-lg overflow-hidden slide-in-top"
        onClick={e => e.stopPropagation()}
        style={{
          background: 'var(--bg-secondary)',
          border: '1px solid var(--accent-cyan)',
          boxShadow: '0 0 60px rgba(99,102,241,0.08), 0 20px 60px rgba(0,0,0,0.5)',
        }}>

        {/* Header with gradient */}
        <div className="px-6 pt-5 pb-4"
          style={{ background: 'linear-gradient(180deg, rgba(99,102,241,0.06) 0%, transparent 100%)' }}>
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-lg flex items-center justify-center"
                style={{ background: 'rgba(99,102,241,0.1)', border: '1px solid rgba(99,102,241,0.3)' }}>
                <CheckCircle size={22} style={{ color: 'var(--accent-cyan)' }} />
              </div>
              <div>
                <div className="text-base font-bold text-white">Investigation Complete</div>
                <div className="text-[10px] uppercase tracking-wider" style={{ color: 'var(--text-muted)' }}>
                  All 5 tables analyzed
                </div>
              </div>
            </div>
            <button onClick={onDismiss} className="p-1.5 rounded-md transition-colors hover:bg-white/5" style={{ color: 'var(--text-muted)' }}>
              <X size={16} />
            </button>
          </div>

          {/* Health score inline */}
          <div className="flex items-center gap-3 px-3 py-2 rounded-md"
            style={{ background: 'var(--bg-primary)', border: '1px solid var(--border-primary)' }}>
            <Activity size={16} style={{ color: healthColor }} />
            <div className="flex-1">
              <div className="text-[10px] uppercase tracking-wider" style={{ color: 'var(--text-muted)' }}>Health Score</div>
            </div>
            <span className="font-mono text-xl font-bold" style={{ color: healthColor }}>
              {summary.health_score}
            </span>
            <span className="font-mono text-sm" style={{ color: 'var(--text-muted)' }}>/100</span>
          </div>
        </div>

        {/* Metrics grid */}
        <div className="px-6 pb-4">
          <div className="grid grid-cols-4 gap-2 mb-4">
            <MetricCell icon={<Timer size={13} />} label="Duration" value={`${summary.duration_seconds}s`} />
            <MetricCell icon={<ShieldAlert size={13} />} label="Critical" value={summary.critical_count} color="var(--critical)" />
            <MetricCell icon={<AlertTriangle size={13} />} label="Warnings" value={summary.warning_count} color="var(--warning)" />
            <MetricCell icon={<Brain size={13} />} label="AI Tokens" value={summary.total_tokens?.toLocaleString()} />
          </div>

          {/* Divider */}
          <div className="h-px mb-4" style={{ background: 'var(--border-primary)' }} />

          {/* Actions */}
          <button onClick={() => { onExportPdf(); onDismiss(); }}
            className="w-full flex items-center justify-center gap-2 px-4 py-2.5 rounded-md text-[11px] font-bold uppercase tracking-wider transition-all btn-glow"
            style={{ background: 'var(--accent-cyan)', color: 'var(--bg-primary)' }}>
            <FileDown size={14} />
            Download PDF Report
          </button>
          <button onClick={onDismiss}
            className="w-full mt-2 px-4 py-2 rounded-md text-xs transition-colors text-center hover:bg-white/5"
            style={{ color: 'var(--text-muted)' }}>
            View Full Report
          </button>
        </div>
      </div>
    </div>
  )
}

function MetricCell({ icon, label, value, color }) {
  return (
    <div className="rounded-md px-2.5 py-2 text-center"
      style={{ background: 'var(--bg-primary)', border: '1px solid var(--border-primary)' }}>
      <div className="flex items-center justify-center gap-1 mb-1" style={{ color: 'var(--text-muted)' }}>
        {icon}
      </div>
      <div className="font-mono text-sm font-bold" style={{ color: color || 'var(--text-primary)' }}>
        {value}
      </div>
      <div className="text-[8px] uppercase tracking-wider mt-0.5" style={{ color: 'var(--text-muted)' }}>{label}</div>
    </div>
  )
}
