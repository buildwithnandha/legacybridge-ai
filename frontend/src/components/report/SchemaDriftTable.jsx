import { GitBranch, Info } from 'lucide-react'

export default function SchemaDriftTable({ report }) {
  const tables = report?.recon?.tables
  if (!tables) return null

  const rows = []
  const notes = []
  for (const [tableName, info] of Object.entries(tables)) {
    const schema = info.schema_diff
    if (!schema) continue
    for (const col of schema.missing_columns || []) {
      rows.push({ table: tableName, issue: 'MISSING', column: col.column, source: col.type, target: '\u2014', severity: 'CRITICAL' })
    }
    for (const m of schema.type_mismatches || []) {
      rows.push({ table: tableName, issue: 'TYPE_MISMATCH', column: m.column, source: m.source_type, target: m.target_type, severity: 'WARNING' })
    }
    for (const col of schema.extra_columns || []) {
      rows.push({ table: tableName, issue: 'EXTRA', column: col.column, source: '\u2014', target: col.type, severity: 'WARNING' })
    }
    // Collect data-level notes for tables with clean schemas but data issues
    if (schema.note) {
      notes.push({ table: tableName, note: schema.note })
    }
  }

  if (rows.length === 0 && notes.length === 0) return null

  return (
    <div>
      <div className="flex items-center gap-2 mb-2">
        <GitBranch size={13} style={{ color: 'var(--accent-cyan)' }} />
        <span className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: 'var(--text-secondary)' }}>
          Schema &amp; Data Drift Analysis
        </span>
      </div>
      {rows.length > 0 && (
        <DataTable
          headers={['Table', 'Issue', 'Column', 'Source', 'Target', 'Severity']}
          rows={rows.map(r => [
            { value: r.table, mono: true, color: 'var(--text-code)' },
            { value: r.issue, mono: true },
            { value: r.column, mono: true, color: 'var(--text-primary)' },
            { value: r.source, mono: true },
            { value: r.target, mono: true },
            { value: r.severity, badge: true, severity: r.severity },
          ])}
        />
      )}
      {/* Data-level notes for tables with clean schemas */}
      {notes.map(n => (
        <div key={n.table} className="flex items-start gap-2 mt-2 px-3 py-2 rounded-md"
          style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)' }}>
          <Info size={12} className="mt-0.5 flex-shrink-0" style={{ color: 'var(--text-muted)' }} />
          <div>
            <span className="font-mono text-[11px]" style={{ color: 'var(--text-code)' }}>{n.table}</span>
            <span className="text-[11px] ml-2" style={{ color: 'var(--text-muted)' }}>{n.note}</span>
          </div>
        </div>
      ))}
    </div>
  )
}

function DataTable({ headers, rows }) {
  return (
    <div className="rounded-md overflow-hidden" style={{ border: '1px solid var(--border-primary)' }}>
      <table className="w-full text-xs">
        <thead>
          <tr style={{ background: 'var(--bg-tertiary)' }}>
            {headers.map(h => (
              <th key={h} className="px-3 py-2 text-left font-medium text-[10px] uppercase tracking-wider"
                style={{ color: 'var(--text-muted)', borderBottom: '1px solid var(--border-primary)' }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? 'var(--bg-secondary)' : 'transparent' }}>
              {row.map((cell, j) => (
                <td key={j} className={`px-3 py-1.5 ${cell.mono ? 'font-mono' : ''}`}
                  style={{ color: cell.color || 'var(--text-secondary)', borderBottom: '1px solid var(--border-primary)' }}>
                  {cell.badge ? (
                    <span className="text-[10px] font-semibold uppercase px-1.5 py-0.5 rounded"
                      style={{
                        color: cell.severity === 'CRITICAL' ? 'var(--critical)' : 'var(--warning)',
                        background: cell.severity === 'CRITICAL' ? 'var(--critical-bg)' : 'var(--warning-bg)',
                      }}>{cell.value}</span>
                  ) : cell.value}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
