import { Database, ShieldCheck, ShieldAlert, AlertTriangle } from 'lucide-react'

const TABLES = ['vendor', 'inventory', 'inventory_transaction', 'supplier_contract', 'purchase_order']

function StatusIcon({ status }) {
  if (status === 'CRITICAL' || status === 'MISMATCH') return <ShieldAlert size={10} style={{ color: 'var(--critical)' }} />
  if (status === 'WARNING') return <AlertTriangle size={10} style={{ color: 'var(--warning)' }} />
  if (status === 'HEALTHY' || status === 'MATCH') return <ShieldCheck size={10} style={{ color: 'var(--healthy)' }} />
  return null
}

function getPillStyle(status) {
  if (status === 'CRITICAL' || status === 'MISMATCH') return {
    background: 'var(--critical-bg)', border: '1px solid var(--critical)', dotColor: 'var(--critical)', pulse: true
  }
  if (status === 'WARNING') return {
    background: 'var(--warning-bg)', border: '1px solid var(--warning)', dotColor: 'var(--warning)', pulse: false
  }
  if (status === 'HEALTHY' || status === 'MATCH') return {
    background: 'var(--healthy-bg)', border: '1px solid var(--healthy)', dotColor: 'var(--healthy)', pulse: false
  }
  if (status === 'SCANNING') return {
    background: 'var(--bg-tertiary)', border: '1px solid var(--accent-cyan)', dotColor: 'var(--accent-cyan)', pulse: true,
    leftBorder: '2px solid var(--accent-cyan)'
  }
  return {
    background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)', dotColor: 'var(--text-muted)', pulse: false
  }
}

export default function StatusBar({ tableStatuses }) {
  return (
    <div className="h-10 flex items-center gap-2 px-5"
      style={{ background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-primary)' }}>
      {TABLES.map(table => {
        const status = tableStatuses[table]
        const style = getPillStyle(status)
        return (
          <div key={table}
            className="flex items-center gap-2 px-2.5 py-1 rounded transition-all duration-300"
            style={{
              background: style.background,
              border: style.border,
              borderLeft: style.leftBorder || undefined,
            }}>
            <Database size={10} style={{ color: 'var(--text-muted)' }} />
            <span className="font-mono text-[11px]" style={{ color: 'var(--text-secondary)' }}>
              {table}
            </span>
            <span className={`w-1.5 h-1.5 rounded-full ${style.pulse ? 'pulse-dot' : ''}`}
              style={{ background: style.dotColor }} />
            <StatusIcon status={status} />
          </div>
        )
      })}
    </div>
  )
}
