import { Zap, Play, FileDown, Eye, Scan } from 'lucide-react'

export default function Header({ onRunRecon, onExportPdf, isRunning, canExport, demoMode }) {
  return (
    <header className="h-[52px] flex items-center justify-between px-5"
      style={{ background: 'var(--bg-primary)', borderBottom: '1px solid var(--border-primary)' }}>

      {/* Left: Logo + title */}
      <div className="flex items-center gap-3">
        <div className="w-7 h-7 rounded-md flex items-center justify-center"
          style={{ background: 'rgba(99,102,241,0.1)', border: '1px solid rgba(99,102,241,0.25)' }}>
          <Zap size={14} style={{ color: 'var(--accent-cyan)' }} />
        </div>
        <div className="flex items-baseline gap-1.5">
          <span className="text-[15px] font-bold text-white tracking-tight">LegacyBridge</span>
          <span className="text-[15px] font-bold tracking-tight" style={{ color: 'var(--accent-cyan)' }}>AI</span>
        </div>
        <div className="w-px h-5 mx-1" style={{ background: 'var(--border-primary)' }} />
        <span className="text-[12px] font-semibold tracking-wide" style={{ color: 'var(--text-secondary)' }}>
          Data Migration Intelligence
        </span>
        <span className="px-1.5 py-0.5 rounded text-[9px] font-mono font-bold"
          style={{ background: 'rgba(99,102,241,0.1)', color: 'var(--accent-cyan)', border: '1px solid rgba(99,102,241,0.2)' }}>
          v1.0
        </span>
      </div>

      {/* Right: Controls */}
      <div className="flex items-center gap-2.5">
        {/* Live indicator */}
        {isRunning && (
          <span className="flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-semibold"
            style={{ background: 'var(--critical-bg)', border: '1px solid rgba(255,68,68,0.3)' }}>
            <span className="w-1.5 h-1.5 rounded-full pulse-dot" style={{ background: 'var(--critical)' }} />
            <span style={{ color: 'var(--critical)' }}>LIVE</span>
          </span>
        )}

        {/* Demo badge */}
        {demoMode && !isRunning && (
          <span className="flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium"
            style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)', color: 'var(--text-muted)' }}>
            <Eye size={11} />
            Demo
          </span>
        )}

        <div className="w-px h-5" style={{ background: 'var(--border-primary)' }} />

        {/* Run button */}
        <button
          onClick={onRunRecon}
          disabled={isRunning}
          className="flex items-center gap-2 px-5 py-[7px] rounded text-[11px] font-bold uppercase tracking-wider transition-all btn-glow"
          style={isRunning ? {
            background: 'var(--bg-tertiary)',
            color: 'var(--text-muted)',
            border: '1px solid var(--border-primary)',
            cursor: 'not-allowed',
          } : {
            background: 'var(--accent-cyan)',
            color: 'var(--bg-primary)',
            border: '1px solid var(--accent-cyan)',
          }}
        >
          {isRunning ? (
            <>
              <Scan size={13} className="animate-spin" style={{ animationDuration: '2s' }} />
              Analyzing
            </>
          ) : (
            <>
              <Play size={12} fill="currentColor" />
              Run Analysis
            </>
          )}
        </button>

        {/* Export button */}
        <button
          onClick={onExportPdf}
          disabled={!canExport}
          className="flex items-center gap-2 px-4 py-[7px] rounded text-[11px] font-bold uppercase tracking-wider transition-all"
          style={canExport ? {
            background: 'transparent',
            color: 'var(--accent-cyan)',
            border: '1px solid var(--accent-cyan)',
          } : {
            background: 'transparent',
            color: 'var(--text-muted)',
            border: '1px solid var(--border-primary)',
            cursor: 'not-allowed',
          }}
        >
          <FileDown size={12} />
          Export PDF
        </button>
      </div>
    </header>
  )
}
